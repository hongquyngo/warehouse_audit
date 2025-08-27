# for_auditor.py - Enhanced Physical Count with Media Attachments
# Records both existing products found in warehouse and new items not in ERP
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
import time
from typing import Dict, List, Optional, Tuple
import json
from sqlalchemy import text
import mimetypes
import os

# Import existing utilities
from utils.auth import AuthManager
from utils.config import config
from utils.db import get_db_engine
from utils.s3_utils import S3Manager

# Import services
from audit_service import AuditService, AuditException
from audit_queries import AuditQueries

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Warehouse Physical Count",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize services
auth = AuthManager()
audit_service = AuditService()
s3_manager = S3Manager()

# ============== CONSTANTS ==============
ALLOWED_IMAGE_TYPES = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']
ALLOWED_DOC_TYPES = ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'txt']
MAX_FILE_SIZE_MB = 10

# ============== SESSION STATE INITIALIZATION ==============
def init_session_state():
    """Initialize all session state variables"""
    if 'new_items_list' not in st.session_state:
        st.session_state.new_items_list = []
    
    if 'item_counter' not in st.session_state:
        st.session_state.item_counter = 0
    
    if 'last_save_time' not in st.session_state:
        st.session_state.last_save_time = None
    
    if 'show_preview' not in st.session_state:
        st.session_state.show_preview = True
    
    if 'default_location' not in st.session_state:
        st.session_state.default_location = {'zone': '', 'rack': '', 'bin': ''}
    
    if 'form_data' not in st.session_state:
        st.session_state.form_data = {}
    
    if 'product_selector' not in st.session_state:
        st.session_state.product_selector = "-- Not in ERP / New Product --"
    
    if 'show_team_counts' not in st.session_state:
        st.session_state.show_team_counts = False
    
    # Media attachment states
    if 'pending_attachments' not in st.session_state:
        st.session_state.pending_attachments = []
    
    if 'item_attachments' not in st.session_state:
        st.session_state.item_attachments = {}
    
    if 'show_media_gallery' not in st.session_state:
        st.session_state.show_media_gallery = False

# ============== MEDIA HANDLING FUNCTIONS ==============

def validate_file(uploaded_file) -> Tuple[bool, str]:
    """Validate uploaded file"""
    if uploaded_file is None:
        return False, "No file selected"
    
    # Check file size
    file_size_mb = uploaded_file.size / (1024 * 1024)
    if file_size_mb > MAX_FILE_SIZE_MB:
        return False, f"File size exceeds {MAX_FILE_SIZE_MB}MB limit"
    
    # Check file type
    file_extension = uploaded_file.name.split('.')[-1].lower()
    allowed_extensions = ALLOWED_IMAGE_TYPES + ALLOWED_DOC_TYPES
    
    if file_extension not in allowed_extensions:
        return False, f"File type .{file_extension} not allowed"
    
    return True, "Valid"

def get_file_category(filename: str) -> str:
    """Determine file category based on extension"""
    extension = filename.split('.')[-1].lower()
    if extension in ALLOWED_IMAGE_TYPES:
        return 'images'
    else:
        return 'docs'

def get_file_type(filename: str) -> str:
    """Determine file type for database"""
    extension = filename.split('.')[-1].lower()
    if extension in ALLOWED_IMAGE_TYPES:
        return 'image'
    elif extension in ALLOWED_DOC_TYPES:
        return 'document'
    else:
        return 'other'

def upload_count_attachments(count_id: int, attachments: List[Dict], transaction_code: str) -> List[Dict]:
    """Upload attachments for a physical count detail"""
    uploaded = []
    
    for attachment in attachments:
        try:
            file = attachment['file']
            description = attachment.get('description', '')
            
            # Get file details
            file_content = file.read()
            file_name = file.name
            file_size = len(file_content)
            mime_type = mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
            file_category = get_file_category(file_name)
            file_type = get_file_type(file_name)
            
            # Upload to S3
            success, s3_key = s3_manager.upload_audit_attachment(
                file_content=file_content,
                filename=file_name,
                entity_type='count_detail',
                entity_code=transaction_code,
                entity_id=count_id,
                file_category=file_category,
                content_type=mime_type
            )
            
            if success:
                # Save to database
                attachment_data = {
                    'entity_type': 'count_detail',
                    'entity_id': count_id,
                    'file_name': file_name,
                    'file_type': file_type,
                    'mime_type': mime_type,
                    'file_size': file_size,
                    's3_key': s3_key,
                    's3_bucket': s3_manager.bucket_name,
                    'description': description,
                    'uploaded_by_user_id': st.session_state.user_id
                }
                
                attachment_id = audit_service.save_media_attachment(attachment_data)
                attachment_data['id'] = attachment_id
                uploaded.append(attachment_data)
                
                logger.info(f"Uploaded attachment {file_name} for physical count {count_id}")
            else:
                st.error(f"Failed to upload {file_name}: {s3_key}")
                
        except Exception as e:
            logger.error(f"Error uploading attachment: {e}")
            st.error(f"Error uploading {attachment['file'].name}: {str(e)}")
    
    return uploaded

def display_attachment_preview(attachments: List[Dict]):
    """Display preview of pending attachments"""
    if not attachments:
        return
    
    st.markdown(f"**📎 {len(attachments)} attachments**")
    
    for att in attachments:
        file = att['file']
        file_type = get_file_type(file.name)
        
        col1, col2 = st.columns([1, 4])
        with col1:
            if file_type == 'image':
                st.write("🖼️")
            else:
                st.write("📄")
        
        with col2:
            st.caption(f"{file.name} ({file.size / 1024:.1f}KB)")
            if att.get('description'):
                st.caption(f"Note: {att['description']}")

# ============== CACHE FUNCTIONS ==============

@st.cache_data(ttl=3600)
def get_all_products():
    """Get all active products from database"""
    try:
        query = """
        SELECT 
            p.id,
            p.name as product_name,
            p.pt_code,
            COALESCE(p.legacy_pt_code, '') as legacy_code,
            COALESCE(p.package_size, '') as package_size,
            p.brand_id,
            COALESCE(b.brand_name, '') as brand_name
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE p.delete_flag = 0
        ORDER BY p.pt_code, p.name
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            products = [dict(row._mapping) for row in result.fetchall()]
            logger.info(f"Loaded {len(products)} products from database")
            return products
    except Exception as e:
        logger.error(f"Error getting products: {e}")
        st.error(f"Failed to load products: {str(e)}")
        return []

@st.cache_data(ttl=300)
def get_team_physical_count_summary(session_id: int):
    """Get team-wide physical count summary"""
    try:
        query = """
        SELECT 
            COUNT(DISTINCT acd.created_by_user_id) as total_users,
            COUNT(DISTINCT acd.transaction_id) as total_transactions,
            COUNT(*) as total_items,
            SUM(acd.actual_quantity) as total_quantity,
            COUNT(DISTINCT acd.product_id) as products_in_erp,
            COUNT(DISTINCT CASE WHEN acd.product_id IS NULL THEN acd.actual_notes END) as products_not_in_erp,
            COUNT(DISTINCT CASE WHEN acd.product_id IS NOT NULL THEN acd.product_id END) as unique_erp_products,
            MIN(acd.counted_date) as first_counted,
            MAX(acd.counted_date) as last_counted
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        WHERE at.session_id = :session_id
        AND acd.is_new_item = 1
        AND acd.delete_flag = 0
        AND at.delete_flag = 0
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {"session_id": session_id})
            row = result.fetchone()
            
            if row:
                return {
                    'total_users': row.total_users or 0,
                    'total_transactions': row.total_transactions or 0,
                    'total_items': row.total_items or 0,
                    'total_quantity': float(row.total_quantity) if row.total_quantity else 0,
                    'products_in_erp': row.products_in_erp or 0,
                    'products_not_in_erp': row.products_not_in_erp or 0,
                    'unique_erp_products': row.unique_erp_products or 0,
                    'first_counted': row.first_counted,
                    'last_counted': row.last_counted
                }
            return {}
    except Exception as e:
        logger.error(f"Error getting team summary: {e}")
        return {}

@st.cache_data(ttl=300)
def get_team_physical_count_for_product(session_id: int, product_id: int):
    """Get team physical count summary for a specific product"""
    try:
        query = """
        SELECT 
            COUNT(DISTINCT acd.created_by_user_id) as total_users,
            COUNT(DISTINCT acd.transaction_id) as total_transactions,
            COUNT(*) as total_records,
            SUM(acd.actual_quantity) as total_quantity,
            MIN(acd.counted_date) as first_counted,
            MAX(acd.counted_date) as last_counted,
            GROUP_CONCAT(DISTINCT u.username) as users_list,
            GROUP_CONCAT(DISTINCT at.transaction_code) as transaction_codes
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        JOIN users u ON acd.created_by_user_id = u.id
        WHERE at.session_id = :session_id
        AND acd.product_id = :product_id
        AND acd.is_new_item = 1
        AND acd.delete_flag = 0
        AND at.delete_flag = 0
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {
                "session_id": session_id, 
                "product_id": product_id
            })
            row = result.fetchone()
            
            if row and row.total_records:
                return {
                    'total_users': row.total_users or 0,
                    'total_transactions': row.total_transactions or 0,
                    'total_records': row.total_records or 0,
                    'total_quantity': float(row.total_quantity) if row.total_quantity else 0,
                    'first_counted': row.first_counted,
                    'last_counted': row.last_counted,
                    'users_list': row.users_list.split(',') if row.users_list else [],
                    'transaction_codes': row.transaction_codes.split(',') if row.transaction_codes else []
                }
            return None
    except Exception as e:
        logger.error(f"Error getting team product count: {e}")
        return None

@st.cache_data(ttl=300)
def get_team_physical_counts_detail(session_id: int):
    """Get detailed team physical counts grouped by transaction"""
    try:
        query = """
        SELECT 
            acd.transaction_id,
            at.transaction_code,
            at.transaction_name,
            at.status as transaction_status,
            u.username as counted_by,
            CONCAT(e.first_name, ' ', e.last_name) as counter_name,
            acd.product_id,
            COALESCE(p.name, SUBSTRING_INDEX(acd.actual_notes, ' - ', -1)) as product_name,
            COALESCE(p.pt_code, 'N/A') as pt_code,
            acd.batch_no,
            acd.actual_quantity,
            acd.zone_name,
            acd.rack_name,
            acd.bin_name,
            acd.actual_notes,
            acd.counted_date,
            acd.id as count_detail_id,
            CASE 
                WHEN acd.product_id IS NOT NULL THEN 'IN_ERP'
                ELSE 'NOT_IN_ERP'
            END as item_type
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        JOIN users u ON acd.created_by_user_id = u.id
        LEFT JOIN employees e ON u.employee_id = e.id
        LEFT JOIN products p ON acd.product_id = p.id
        WHERE at.session_id = :session_id
        AND acd.is_new_item = 1
        AND acd.delete_flag = 0
        AND at.delete_flag = 0
        ORDER BY at.transaction_code, acd.counted_date DESC
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {"session_id": session_id})
            return [dict(row._mapping) for row in result.fetchall()]
    except Exception as e:
        logger.error(f"Error getting team detail counts: {e}")
        return []

@st.cache_data(ttl=300)
def get_team_top_products(session_id: int, limit: int = 10):
    """Get top products by team quantity"""
    try:
        query = """
        SELECT 
            CASE 
                WHEN acd.product_id IS NOT NULL THEN p.name
                ELSE SUBSTRING_INDEX(acd.actual_notes, ' - ', -1)
            END as product_name,
            CASE 
                WHEN acd.product_id IS NOT NULL THEN p.pt_code
                ELSE 'NOT_IN_ERP'
            END as pt_code,
            COUNT(*) as count_records,
            SUM(acd.actual_quantity) as total_quantity,
            COUNT(DISTINCT acd.created_by_user_id) as unique_users
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        LEFT JOIN products p ON acd.product_id = p.id
        WHERE at.session_id = :session_id
        AND acd.is_new_item = 1
        AND acd.delete_flag = 0
        AND at.delete_flag = 0
        GROUP BY acd.product_id, product_name, pt_code
        ORDER BY total_quantity DESC
        LIMIT :limit
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {"session_id": session_id, "limit": limit})
            return [dict(row._mapping) for row in result.fetchall()]
    except Exception as e:
        logger.error(f"Error getting top products: {e}")
        return []

# ============== TEAM COUNT DISPLAY FUNCTIONS ==============

def display_team_physical_counts(session_id: int, current_tx_id: int):
    """Display all team physical counts with attachment indicators"""
    try:
        # Get detailed counts
        all_counts = get_team_physical_counts_detail(session_id)
        
        if all_counts:
            # Group by transaction
            transactions = {}
            for count in all_counts:
                tx_id = count['transaction_id']
                if tx_id not in transactions:
                    transactions[tx_id] = {
                        'transaction_code': count['transaction_code'],
                        'transaction_name': count['transaction_name'],
                        'transaction_status': count['transaction_status'],
                        'counts': []
                    }
                transactions[tx_id]['counts'].append(count)
            
            # Display each transaction
            for tx_id, tx_data in transactions.items():
                # Calculate transaction totals
                tx_total_qty = sum(c['actual_quantity'] for c in tx_data['counts'])
                tx_total_items = len(tx_data['counts'])
                tx_users = len(set(c['counted_by'] for c in tx_data['counts']))
                tx_in_erp = sum(1 for c in tx_data['counts'] if c['item_type'] == 'IN_ERP')
                tx_not_in_erp = tx_total_items - tx_in_erp
                
                is_current = (tx_id == current_tx_id)
                status_emoji = "✅" if tx_data['transaction_status'] == 'completed' else "📝"
                current_indicator = " 👈 (Current)" if is_current else ""
                
                st.markdown(f"### {status_emoji} {tx_data['transaction_code']} - {tx_data['transaction_name']}{current_indicator}")
                
                # Transaction metrics
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("👥 Users", tx_users)
                with col2:
                    st.metric("📊 Items", tx_total_items)
                with col3:
                    st.metric("📢 Total Qty", f"{tx_total_qty:.0f}")
                with col4:
                    st.metric("📦 In ERP", tx_in_erp)
                with col5:
                    st.metric("❓ Not in ERP", tx_not_in_erp)
                
                # Show count details in expandable section
                with st.expander(f"View {len(tx_data['counts'])} items", expanded=is_current):
                    for count in tx_data['counts']:
                        # Check for attachments
                        attachments = audit_service.get_entity_attachments('count_detail', count['count_detail_id'])
                        has_attachments = len(attachments) > 0
                        
                        col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 1, 1, 2, 1])
                        
                        with col1:
                            st.write(f"**{count['counter_name'] or count['counted_by']}**")
                            st.caption(f"@{count['counted_by']}")
                        
                        with col2:
                            if count['item_type'] == 'IN_ERP':
                                st.write(f"📦 {count['pt_code']} - {count['product_name']}")
                            else:
                                st.write(f"❓ {count['product_name']}")
                            st.caption(f"Batch: {count['batch_no'] or 'N/A'}")
                        
                        with col3:
                            st.write(f"Qty: {count['actual_quantity']:.0f}")
                        
                        with col4:
                            location = f"{count['zone_name']}-{count['rack_name']}-{count['bin_name']}"
                            st.write(f"📍 {location}")
                        
                        with col5:
                            st.caption(pd.to_datetime(count['counted_date']).strftime('%Y-%m-%d %H:%M'))
                        
                        with col6:
                            if has_attachments:
                                st.write(f"📎 {len(attachments)}")
                
                st.markdown("---")
        else:
            st.info("No physical counts recorded by team yet")
            
    except Exception as e:
        st.error(f"Error loading team counts: {str(e)}")

# ============== ITEM MANAGEMENT FUNCTIONS ==============

def add_new_item(item_data: Dict):
    """Add item to list with validation"""
    # Validate required fields
    if not item_data.get('product_name'):
        raise ValueError("Product name is required")
    
    if item_data.get('actual_quantity', 0) <= 0:
        raise ValueError("Quantity must be greater than 0")
    
    # Handle date conversion
    if item_data.get('expired_date') and hasattr(item_data['expired_date'], 'isoformat'):
        item_data['expired_date'] = item_data['expired_date'].isoformat()
    
    # Add metadata
    item_data['temp_id'] = f"new_{st.session_state.item_counter}_{int(time.time() * 1000)}"
    item_data['added_time'] = datetime.now()
    
    # Add to list
    st.session_state.new_items_list.append(item_data)
    st.session_state.item_counter += 1
    
    # Store attachments if any
    if st.session_state.pending_attachments:
        st.session_state.item_attachments[item_data['temp_id']] = st.session_state.pending_attachments.copy()
        st.session_state.pending_attachments = []
    
    return item_data['temp_id']

def remove_item(temp_id: str):
    """Remove item from list"""
    st.session_state.new_items_list = [
        item for item in st.session_state.new_items_list 
        if item.get('temp_id') != temp_id
    ]
    # Remove attachments
    if temp_id in st.session_state.item_attachments:
        del st.session_state.item_attachments[temp_id]

def clear_all_items():
    """Clear all items from list"""
    st.session_state.new_items_list = []
    st.session_state.item_counter = 0
    st.session_state.item_attachments = {}
    st.session_state.pending_attachments = []
    # Clear team count cache
    get_team_physical_count_summary.clear()
    get_team_physical_counts_detail.clear()
    get_team_top_products.clear()
    get_team_physical_count_for_product.clear()

def get_items_summary() -> Dict:
    """Get summary statistics for current user's pending physical items"""
    if not st.session_state.new_items_list:
        return {
            'total_items': 0,
            'total_quantity': 0,
            'unique_products': 0,
            'total_batches': 0,
            'items_in_erp': 0,
            'items_not_in_erp': 0,
            'total_attachments': 0
        }
    
    total_quantity = sum(item.get('actual_quantity', 0) for item in st.session_state.new_items_list)
    unique_products = len(set(item.get('product_name', '').upper() for item in st.session_state.new_items_list))
    total_batches = len(set((item.get('product_name', ''), item.get('batch_no', '')) 
                           for item in st.session_state.new_items_list))
    
    # Count by whether product exists in ERP
    items_in_erp = sum(1 for item in st.session_state.new_items_list 
                       if item.get('product_id') is not None)
    items_not_in_erp = len(st.session_state.new_items_list) - items_in_erp
    
    # Count attachments
    total_attachments = sum(len(attachments) for attachments in st.session_state.item_attachments.values())
    
    return {
        'total_items': len(st.session_state.new_items_list),
        'total_quantity': total_quantity,
        'unique_products': unique_products,
        'total_batches': total_batches,
        'items_in_erp': items_in_erp,
        'items_not_in_erp': items_not_in_erp,
        'total_attachments': total_attachments
    }

def save_items_to_db(transaction_id: int) -> Tuple[int, List[str]]:
    """Save all physical items and attachments to database"""
    if not st.session_state.new_items_list:
        return 0, ["No items to save"]
    
    # Get transaction code for S3 organization
    tx_info = audit_service.get_transaction_info(transaction_id)
    transaction_code = tx_info.get('transaction_code', f'TXN_{transaction_id}')
    
    # Prepare count data list
    count_list = []
    for item in st.session_state.new_items_list:
        # Handle expired_date conversion
        expired_date = item.get('expired_date')
        if expired_date:
            try:
                if isinstance(expired_date, str):
                    expired_date = datetime.fromisoformat(expired_date).date()
            except:
                logger.warning(f"Failed to parse expiry date: {expired_date}")
                expired_date = None
        
        # Create appropriate notes based on whether product exists in ERP
        if item.get('product_id'):
            # Physical item that exists in ERP product master
            actual_notes = f"PHYSICAL COUNT - IN ERP: {item.get('reference_pt_code', '')} - {item.get('product_name', '')} - {item.get('notes', '')}"
        else:
            # Physical item NOT in ERP product master
            actual_notes = f"PHYSICAL COUNT - NOT IN ERP: {item.get('product_name', '')} - {item.get('brand', '')} - {item.get('notes', '')}"
        
        count_data = {
            'transaction_id': transaction_id,
            'product_id': item.get('product_id'),  # Will be actual ID or None
            'batch_no': item.get('batch_no', ''),
            'expired_date': expired_date,
            'zone_name': item.get('zone_name', ''),
            'rack_name': item.get('rack_name', ''),
            'bin_name': item.get('bin_name', ''),
            'location_notes': item.get('location_notes', ''),
            'system_quantity': 0,  # Always 0 for physical count items not in ERP inventory
            'system_value_usd': 0,  # Always 0 for physical count items not in ERP inventory
            'actual_quantity': item.get('actual_quantity', 0),
            'actual_notes': actual_notes,
            'is_new_item': True,  # ALWAYS TRUE - all are physical items not in ERP inventory
            'created_by_user_id': st.session_state.user_id
        }
        count_list.append(count_data)
    
    # Save all counts at once and get IDs
    saved_ids, errors = audit_service.save_batch_counts(count_list)
    
    # Upload attachments for successful saves
    for item, count_id in zip(st.session_state.new_items_list, saved_ids):
        if count_id:  # Successfully saved
            temp_id = item.get('temp_id')
            if temp_id and temp_id in st.session_state.item_attachments:
                attachments = st.session_state.item_attachments[temp_id]
                # count_id is the entity_id for entity_type='count_detail'
                upload_count_attachments(count_id, attachments, transaction_code)
    
    # Count successes
    successful_saves = len([id for id in saved_ids if id is not None])
    
    if successful_saves > 0:
        st.session_state.last_save_time = datetime.now()
        clear_all_items()
        # Clear caches to refresh team data
        get_team_physical_count_summary.clear()
        get_team_physical_counts_detail.clear()
        get_team_top_products.clear()
        get_team_physical_count_for_product.clear()
    
    return successful_saves, errors

# ============== UI COMPONENTS ==============

def show_header():
    """Display header with user info"""
    col1, col2, col3 = st.columns([6, 2, 2])
    
    with col1:
        st.title("📦 Warehouse Physical Count - Items NOT in ERP Inventory")
        st.caption("Record physical items found in warehouse that are not in ERP inventory data")
    
    with col2:
        st.metric("User", auth.get_user_display_name())
    
    with col3:
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()

def show_summary_bar():
    """Display summary statistics bar with team data and attachments"""
    # Get current user summary
    user_summary = get_items_summary()
    
    # Get team summary if we have a session
    team_summary = None
    if 'selected_session_id' in st.session_state:
        team_summary = get_team_physical_count_summary(st.session_state.selected_session_id)
    
    col1, col2, col3 = st.columns([4, 4, 4])
    
    with col1:
        st.markdown("### 📋 Your Pending Items")
        if user_summary['total_items'] > 0:
            subcol1, subcol2, subcol3, subcol4 = st.columns(4)
            with subcol1:
                st.metric("Items", user_summary['total_items'])
            with subcol2:
                st.metric("Quantity", f"{user_summary['total_quantity']:.0f}")
            with subcol3:
                st.metric("Products", user_summary['unique_products'])
            with subcol4:
                st.metric("📎", user_summary['total_attachments'])
        else:
            st.info("No pending items")
    
    with col2:
        st.markdown("### 👥 Team Total (All Saved)")
        if team_summary and team_summary.get('total_items', 0) > 0:
            subcol1, subcol2, subcol3 = st.columns(3)
            with subcol1:
                st.metric("Items", team_summary['total_items'])
            with subcol2:
                st.metric("Quantity", f"{team_summary['total_quantity']:.0f}")
            with subcol3:
                st.metric("Users", team_summary['total_users'])
        else:
            st.info("No team counts yet")
    
    with col3:
        st.markdown("### 🎯 Actions")
        if user_summary['total_items'] > 0:
            col_save, col_clear = st.columns(2)
            with col_save:
                if st.button("💾 Save All", use_container_width=True, type="primary"):
                    st.session_state.trigger_save = True
            with col_clear:
                if st.button("🗑️ Clear", use_container_width=True):
                    st.session_state.show_clear_confirm = True
        
        # Team view button
        if team_summary and team_summary.get('total_items', 0) > 0:
            if st.button(
                f"👥 View All Team Counts ({team_summary['total_users']} users, {team_summary['total_items']} items)",
                use_container_width=True,
                key="toggle_team_view"
            ):
                st.session_state.show_team_counts = not st.session_state.show_team_counts
    
    st.markdown("---")

def show_transaction_selector():
    """Show transaction selector"""
    st.markdown("### 📋 Select Transaction")
    
    # Get user's draft transactions
    if 'selected_session_id' not in st.session_state:
        sessions = audit_service.get_sessions_by_status('in_progress')
        if sessions:
            st.session_state.selected_session_id = sessions[0]['id']
    
    if 'selected_session_id' in st.session_state:
        transactions = audit_service.get_user_transactions(
            st.session_state.selected_session_id,
            st.session_state.user_id,
            status='draft'
        )
        
        if transactions:
            tx_options = {
                f"{tx['transaction_name']} ({tx['transaction_code']})": tx
                for tx in transactions
            }
            
            selected_key = st.selectbox(
                "Select your draft transaction:",
                options=list(tx_options.keys()),
                help="Select the transaction to add new items to"
            )
            
            selected_tx = tx_options[selected_key]
            st.session_state.selected_tx_id = selected_tx['id']
            
            # Show transaction info
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"🏢 Warehouse: {selected_tx.get('warehouse_name', 'N/A')}")
            with col2:
                st.info(f"🏷️ Zones: {selected_tx.get('assigned_zones', 'All')}")
            with col3:
                st.info(f"📦 Items Counted: {selected_tx.get('total_items_counted', 0)}")
            
            return selected_tx['id']
        else:
            st.warning("⚠️ No draft transactions available. Please create one first.")
            return None
    else:
        st.warning("⚠️ No active session found. Please select a session.")
        return None

def show_entry_form():
    """Show simplified entry form with media upload"""
    st.markdown("### ✏️ Add Physical Item")
    
    # Initialize form key if not exists
    if 'form_key' not in st.session_state:
        st.session_state.form_key = 0
    
    # Load all products once
    all_products = get_all_products()
    
    # PRODUCT SELECTOR OUTSIDE FORM
    st.markdown("**Product (if exists in ERP)**")
    
    # Create options for selectbox with ALL products
    product_options = {"-- Not in ERP / New Product --": None}
    
    # Add all products to options
    for p in all_products:
        display_name = f"{p['pt_code']} - {p['product_name']}"
        if p.get('brand_name'):
            display_name += f" | {p['brand_name']}"
        if p.get('package_size'):
            display_name += f" ({p['package_size']})"
        # Add Product ID at the end
        display_name += f" |ID: {p['id']}"
        product_options[display_name] = p
    
    # Store selected product in session state
    if 'selected_product_key' not in st.session_state:
        st.session_state.selected_product_key = "-- Not in ERP / New Product --"
    
    # Product selector OUTSIDE form
    selected_product_key = st.selectbox(
        "Select Product",
        options=list(product_options.keys()),
        key=f"product_selector_widget_{st.session_state.form_key}",
        help="Type to search in the dropdown. Select 'Not in ERP' if product doesn't exist",
        index=0 if st.session_state.selected_product_key == "-- Not in ERP / New Product --" 
               else list(product_options.keys()).index(st.session_state.selected_product_key)
    )
    
    # Update session state
    st.session_state.selected_product_key = selected_product_key
    selected_product = product_options.get(selected_product_key)
    
    # Show selected product info and team count check
    if selected_product:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.success(f"✅ ERP Product Selected: {selected_product['pt_code']} - {selected_product['product_name']} (ID: {selected_product['id']})")
        
        # Check if team has already counted this product
        if 'selected_session_id' in st.session_state:
            team_product_count = get_team_physical_count_for_product(
                st.session_state.selected_session_id,
                selected_product['id']
            )
            
            if team_product_count:
                with col2:
                    st.warning(f"⚠️ Already counted by team: {team_product_count['total_quantity']:.0f} units")
                
                # Show detailed team count info
                with st.expander(f"👥 View Team Counts ({team_product_count['total_users']} users, {team_product_count['total_records']} records)", expanded=False):
                    col_info1, col_info2, col_info3 = st.columns(3)
                    with col_info1:
                        st.metric("Total Quantity", f"{team_product_count['total_quantity']:.0f}")
                    with col_info2:
                        st.metric("Total Records", team_product_count['total_records'])
                    with col_info3:
                        st.metric("Users", team_product_count['total_users'])
                    
                    st.markdown("**Counted by:**")
                    for user in team_product_count['users_list']:
                        st.caption(f"• {user}")
                    
                    st.markdown("**In transactions:**")
                    for tx_code in team_product_count['transaction_codes']:
                        st.caption(f"• {tx_code}")
                    
                    if team_product_count['last_counted']:
                        st.caption(f"Last counted: {pd.to_datetime(team_product_count['last_counted']).strftime('%Y-%m-%d %H:%M')}")
            else:
                with col2:
                    st.info("ℹ️ Not yet counted by team")
    else:
        st.info("ℹ️ Product not in ERP - Enter details manually below")
    
    # Main form with dynamic key for reset
    with st.form(f"new_item_form_{st.session_state.form_key}", clear_on_submit=True):
        # Form fields
        col1, col2 = st.columns(2)
        
        with col1:
            # Product ID - only show if product selected
            if selected_product:
                product_id_display = st.text_input(
                    "Product ID", 
                    value=str(selected_product.get('id', '')),
                    disabled=True,
                    help="Auto-filled from ERP"
                )
            
            # Product name - auto-filled or manual entry
            if selected_product:
                product_name = st.text_input(
                    "Product Name*", 
                    value=selected_product.get('product_name', ''),
                    disabled=True
                )
                actual_product_name = selected_product.get('product_name', '')
            else:
                product_name = st.text_input(
                    "Product Name*", 
                    placeholder="Enter product name"
                )
                actual_product_name = product_name
            
            # Brand - auto-filled or manual entry
            if selected_product:
                brand_name = st.text_input(
                    "Brand", 
                    value=selected_product.get('brand_name', ''),
                    disabled=True
                )
                actual_brand = selected_product.get('brand_name', '')
            else:
                brand_name = st.text_input(
                    "Brand", 
                    placeholder="Enter brand name"
                )
                actual_brand = brand_name
            
            batch_no = st.text_input("Batch Number", placeholder="Enter batch number")
            
            # Package size
            if selected_product:
                package_size = st.text_input(
                    "Package Size", 
                    value=selected_product.get('package_size', ''),
                    disabled=True
                )
                actual_package_size = selected_product.get('package_size', '')
            else:
                package_size = st.text_input(
                    "Package Size", 
                    placeholder="e.g., 100 tablets, 500ml"
                )
                actual_package_size = package_size
        
        with col2:
            quantity = st.number_input(
                "Quantity*", 
                min_value=0.0, 
                value=0.0,  # Default value
                step=1.0, 
                format="%.2f",
                help="Physical quantity found in warehouse"
            )
            
            expired_date = st.date_input(
                "Expiry Date", 
                value=None,
                help="Leave empty if no expiry date"
            )
            
            # Location inputs with default values
            st.markdown("**Location**")
            col_z, col_r, col_b = st.columns(3)
            with col_z:
                zone = st.text_input("Zone", value=st.session_state.default_location.get('zone', ''))
            with col_r:
                rack = st.text_input("Rack", value=st.session_state.default_location.get('rack', ''))
            with col_b:
                bin_name = st.text_input("Bin", value=st.session_state.default_location.get('bin', ''))
        
        notes = st.text_area(
            "Additional Notes", 
            placeholder="Any observations, damage, special conditions, etc."
        )
        
        # Media upload section
        st.markdown("### 📎 Attachments (Optional)")
        
        col1, col2 = st.columns(2)
        with col1:
            uploaded_files = st.file_uploader(
                "Upload Images/Documents",
                type=ALLOWED_IMAGE_TYPES + ALLOWED_DOC_TYPES,
                accept_multiple_files=True,
                key=f"file_uploader_{st.session_state.form_key}",
                help=f"Max {MAX_FILE_SIZE_MB}MB per file"
            )
        
        with col2:
            attachment_notes = st.text_area(
                "Attachment Notes",
                key=f"attachment_notes_{st.session_state.form_key}",
                placeholder="Describe the attachments (optional)",
                height=100
            )
        
        # Submit buttons
        col_submit, col_reset = st.columns([3, 1])
        
        with col_submit:
            submitted = st.form_submit_button(
                "➕ Add Physical Item",
                use_container_width=True,
                type="primary",
                disabled=len(st.session_state.new_items_list) >= 20
            )
        
        with col_reset:
            reset = st.form_submit_button("🔄 Reset Form", use_container_width=True)
        
        # Handle form submission
        if submitted:
            # Initialize actual_product_name if not defined (edge case)
            if 'actual_product_name' not in locals():
                actual_product_name = ""
            
            if not actual_product_name:
                st.error("❌ Product name is required!")
            elif quantity <= 0:
                st.error("❌ Quantity must be greater than 0!")
            elif not zone:
                st.error("❌ Zone is required for location!")
            else:
                try:
                    # Process uploaded files
                    if uploaded_files:
                        st.session_state.pending_attachments = []
                        for file in uploaded_files:
                            valid, msg = validate_file(file)
                            if valid:
                                st.session_state.pending_attachments.append({
                                    'file': file,
                                    'description': attachment_notes
                                })
                            else:
                                st.warning(f"⚠️ {file.name}: {msg}")
                    
                    # Prepare item data
                    if selected_product:
                        # Product exists in ERP master data
                        product_id = selected_product['id']
                        notes_prefix = f"IN ERP: {selected_product['pt_code']} - "
                    else:
                        # Product not in ERP master data
                        product_id = None
                        notes_prefix = f"NOT IN ERP: "
                    
                    item_data = {
                        'product_name': actual_product_name,
                        'brand': actual_brand if 'actual_brand' in locals() else '',
                        'batch_no': batch_no,
                        'package_size': actual_package_size if 'actual_package_size' in locals() else '',
                        'actual_quantity': quantity,
                        'expired_date': expired_date,
                        'zone_name': zone,
                        'rack_name': rack,
                        'bin_name': bin_name,
                        'notes': notes_prefix + notes if notes else notes_prefix + "Physical count",
                        'product_id': product_id,
                        'is_new_item': True,  # Always TRUE - all are physical items not in ERP inventory
                        'reference_pt_code': selected_product['pt_code'] if selected_product else None,
                        'created_by_user_id': st.session_state.user_id
                    }
                    
                    add_new_item(item_data)
                    
                    # Success message
                    attachment_count = len(st.session_state.pending_attachments) if hasattr(st.session_state, 'pending_attachments') else 0
                    if product_id:
                        st.success(f"✅ Added: {selected_product['pt_code']} - {actual_product_name} (ID: {product_id}, Qty: {quantity}, 📎 {attachment_count})")
                    else:
                        st.success(f"✅ Added: {actual_product_name} - NOT IN ERP (Qty: {quantity}, 📎 {attachment_count})")
                    
                    # Update default location for next entry
                    st.session_state.default_location = {'zone': zone, 'rack': rack, 'bin': bin_name}
                    
                    # Reset product selector to default
                    st.session_state.selected_product_key = "-- Not in ERP / New Product --"
                    
                    # Clear cache to refresh team counts
                    get_team_physical_count_for_product.clear()
                    
                    # Increment form key to force form reset
                    st.session_state.form_key += 1
                    
                    # Short delay then rerun to clear form
                    time.sleep(0.5)
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        
        if reset:
            # Reset product selector and form
            st.session_state.selected_product_key = "-- Not in ERP / New Product --"
            st.session_state.pending_attachments = []
            st.session_state.form_key += 1
            st.rerun()

def show_items_preview():
    """Display preview of pending items with attachments"""
    if not st.session_state.new_items_list:
        return
    
    st.markdown("### 📋 Pending Items")
    
    # Display as a table with attachment indicator
    items_data = []
    for item in st.session_state.new_items_list:
        temp_id = item.get('temp_id')
        attachment_count = len(st.session_state.item_attachments.get(temp_id, []))
        
        items_data.append({
            'Type': '📦 ERP' if item.get('product_id') else '❓ New',
            'ID': item.get('product_id', '-'),
            'Product': item.get('product_name', ''),
            'PT Code': item.get('reference_pt_code', '-'),
            'Brand': item.get('brand', '-'),
            'Batch': item.get('batch_no', '-'),
            'Quantity': f"{item.get('actual_quantity', 0):.0f}",
            'Location': f"{item.get('zone_name', '')}-{item.get('rack_name', '')}-{item.get('bin_name', '')}",
            '📎': attachment_count if attachment_count > 0 else '-',
            'temp_id': temp_id
        })
    
    # Create DataFrame
    df = pd.DataFrame(items_data)
    
    # Display with action column
    for idx, row in df.iterrows():
        col1, col2, col3, col4, col5, col6, col7, col8, col9, col10 = st.columns([1, 1, 3, 2, 2, 2, 2, 2, 1, 1])
        
        with col1:
            st.write(row['Type'])
        with col2:
            st.write(row['ID'])
        with col3:
            st.write(row['Product'])
        with col4:
            st.write(row['PT Code'])
        with col5:
            st.write(row['Brand'])
        with col6:
            st.write(row['Batch'])
        with col7:
            st.write(row['Quantity'])
        with col8:
            st.write(row['Location'])
        with col9:
            st.write(str(row['📎']))
        with col10:
            if st.button("🗑️", key=f"del_{row['temp_id']}", help="Remove"):
                remove_item(row['temp_id'])
                st.rerun()
        
        # Show attachments if expanded
        if row['📎'] != '-':
            with st.expander(f"View {row['📎']} attachments", expanded=False):
                attachments = st.session_state.item_attachments.get(row['temp_id'], [])
                display_attachment_preview(attachments)
        
        if idx < len(df) - 1:
            st.divider()

def show_media_gallery():
    """Display media gallery for physical count items"""
    st.subheader("📸 Media Gallery - Physical Count")
    
    if 'selected_session_id' not in st.session_state:
        st.warning("⚠️ Please select a session first")
        return
    
    # Get session info
    session_info = audit_service.get_session_info(st.session_state.selected_session_id)
    if not session_info:
        st.error("Session not found")
        return
    
    st.info(f"**Session:** {session_info['session_name']} ({session_info['session_code']})")
    
    # Filter options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Get user's transactions
        user_transactions = audit_service.get_user_transactions(
            st.session_state.selected_session_id,
            st.session_state.user_id
        )
        
        tx_filter = st.selectbox(
            "Filter by Transaction",
            ["All"] + [f"{tx['transaction_name']} ({tx['transaction_code']})" for tx in user_transactions]
        )
    
    with col2:
        media_type_filter = st.selectbox(
            "Filter by Type",
            ["All", "Images", "Documents"]
        )
    
    with col3:
        if st.button("🔄 Refresh Gallery", use_container_width=True):
            st.rerun()
    
    # Get attachments for physical count items
    try:
        all_attachments = []
        
        for tx in user_transactions:
            if tx_filter != "All" and tx['transaction_code'] not in tx_filter:
                continue
            
            # Get physical counts for this transaction
            query = """
            SELECT acd.*, p.name as product_name, p.pt_code
            FROM audit_count_details acd
            LEFT JOIN products p ON acd.product_id = p.id
            WHERE acd.transaction_id = :transaction_id
            AND acd.is_new_item = 1
            AND acd.delete_flag = 0
            ORDER BY acd.counted_date DESC
            """
            
            engine = get_db_engine()
            with engine.connect() as conn:
                result = conn.execute(text(query), {"transaction_id": tx['id']})
                counts = [dict(row._mapping) for row in result.fetchall()]
            
            for count in counts:
                # Get attachments for each count
                attachments = audit_service.get_entity_attachments('count_detail', count['id'])
                
                for att in attachments:
                    # Apply type filter
                    if media_type_filter == "Images" and att.get('file_type') != 'image':
                        continue
                    elif media_type_filter == "Documents" and att.get('file_type') != 'document':
                        continue
                    
                    # Add metadata
                    att['transaction_name'] = tx['transaction_name']
                    att['product_name'] = count.get('product_name') or 'Not in ERP'
                    att['pt_code'] = count.get('pt_code', 'N/A')
                    att['batch_no'] = count.get('batch_no', 'N/A')
                    att['counted_date'] = count.get('counted_date')
                    att['location'] = f"{count.get('zone_name', '')}-{count.get('rack_name', '')}-{count.get('bin_name', '')}"
                    
                    # Generate presigned URL
                    att['s3_url'] = s3_manager.get_presigned_url(att['s3_key'], expiration=3600)
                    
                    all_attachments.append(att)
        
        if all_attachments:
            st.markdown(f"### Found {len(all_attachments)} attachments")
            
            # Display in grid
            cols = st.columns(3)
            for idx, att in enumerate(all_attachments):
                with cols[idx % 3]:
                    with st.container():
                        if att['pt_code'] != 'N/A':
                            st.markdown(f"**📦 {att['pt_code']} - {att['product_name']}**")
                        else:
                            st.markdown(f"**❓ {att['product_name']}**")
                        
                        st.caption(f"Batch: {att['batch_no']} | Loc: {att['location']}")
                        st.caption(f"Transaction: {att['transaction_name']}")
                        
                        if att['file_type'] == 'image' and att['s3_url']:
                            st.image(att['s3_url'], use_column_width=True)
                        else:
                            st.write(f"📄 {att['file_name']}")
                        
                        st.caption(f"Uploaded by: {att.get('uploaded_by_name', att.get('uploaded_by_username', 'Unknown'))}")
                        st.caption(f"Date: {pd.to_datetime(att['uploaded_date']).strftime('%m/%d %H:%M')}")
                        
                        if att.get('description'):
                            st.caption(f"Note: {att['description']}")
                        
                        if att['s3_url']:
                            st.markdown(f"[📥 Download]({att['s3_url']})")
                        
                        st.markdown("---")
        else:
            st.info("No media attachments found for physical count items")
            
    except Exception as e:
        st.error(f"Error loading media gallery: {str(e)}")
        logger.error(f"Media gallery error: {e}")

def export_items_to_csv():
    """Export items to CSV"""
    if not st.session_state.new_items_list:
        return
    
    # Create DataFrame
    df_data = []
    for item in st.session_state.new_items_list:
        expiry_date = item.get('expired_date', '')
        if expiry_date:
            try:
                if isinstance(expiry_date, str) and expiry_date != '':
                    expiry_date = datetime.fromisoformat(expiry_date).strftime('%Y-%m-%d')
            except:
                pass
        
        temp_id = item.get('temp_id')
        attachment_count = len(st.session_state.item_attachments.get(temp_id, []))
        
        df_data.append({
            'ERP Status': 'In ERP Master' if item.get('product_id') else 'Not in ERP',
            'Product ID': item.get('product_id', ''),
            'Product Name': item.get('product_name', ''),
            'PT Code': item.get('reference_pt_code', ''),
            'Brand': item.get('brand', ''),
            'Batch Number': item.get('batch_no', ''),
            'Package Size': item.get('package_size', ''),
            'Quantity': item.get('actual_quantity', 0),
            'Expiry Date': expiry_date,
            'Zone': item.get('zone_name', ''),
            'Rack': item.get('rack_name', ''),
            'Bin': item.get('bin_name', ''),
            'Notes': item.get('notes', ''),
            'Attachments': attachment_count,
            'Added Time': item.get('added_time', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('added_time') else ''
        })
    
    df = pd.DataFrame(df_data)
    csv = df.to_csv(index=False)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"warehouse_physical_items_{timestamp}.csv",
        mime="text/csv"
    )

def handle_save_action(transaction_id: int):
    """Handle save action with progress"""
    if 'trigger_save' in st.session_state and st.session_state.trigger_save:
        st.session_state.trigger_save = False
        
        with st.spinner(f"Saving {len(st.session_state.new_items_list)} items and uploading attachments..."):
            progress_bar = st.progress(0)
            
            # Simulate progress
            for i in range(50):
                progress_bar.progress(i / 100)
                time.sleep(0.01)
            
            # Save to database
            saved_count, errors = save_items_to_db(transaction_id)
            
            progress_bar.progress(100)
            time.sleep(0.5)
            
            if errors and saved_count == 0:
                st.error(f"❌ Failed to save items")
                for error in errors[:3]:
                    st.caption(f"• {error}")
            elif errors and saved_count > 0:
                st.warning(f"⚠️ Saved {saved_count} items with {len(errors)} errors")
                for error in errors[:3]:  # Show first 3 errors
                    st.caption(f"• {error}")
            else:
                st.success(f"✅ Successfully saved {saved_count} items with attachments!")
                st.balloons()
                time.sleep(1)
                st.rerun()

def handle_clear_confirmation():
    """Handle clear all confirmation"""
    if 'show_clear_confirm' in st.session_state and st.session_state.show_clear_confirm:
        st.session_state.show_clear_confirm = False
        
        with st.container():
            st.warning("⚠️ Are you sure you want to clear all pending items?")
            col1, col2, col3 = st.columns([1, 1, 3])
            
            with col1:
                if st.button("✅ Yes, Clear All", type="primary"):
                    clear_all_items()
                    st.success("✅ All items cleared!")
                    time.sleep(0.5)
                    st.rerun()
            
            with col2:
                if st.button("❌ Cancel"):
                    st.rerun()

def show_statistics():
    """Show team statistics and analytics"""
    st.markdown("### 📊 Team Statistics")
    
    if 'selected_session_id' not in st.session_state:
        st.info("Select a session to view team statistics")
        return
    
    # Get team summary
    team_summary = get_team_physical_count_summary(st.session_state.selected_session_id)
    
    if not team_summary or team_summary.get('total_items', 0) == 0:
        st.info("No team data available yet")
        return
    
    # Main metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Items", team_summary['total_items'])
    
    with col2:
        st.metric("Total Quantity", f"{team_summary['total_quantity']:.0f}")
    
    with col3:
        st.metric("Unique Users", team_summary['total_users'])
    
    with col4:
        st.metric("Transactions", team_summary['total_transactions'])
    
    # Breakdown by type
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📦 Items in ERP Master", team_summary['products_in_erp'], 
                  help="Physical items that exist in ERP product master")
    
    with col2:
        st.metric("❓ Items NOT in ERP", team_summary['products_not_in_erp'],
                  help="Physical items not found in ERP product master")
    
    with col3:
        st.metric("🎯 Unique ERP Products", team_summary['unique_erp_products'])
    
    # Time range
    if team_summary.get('first_counted') and team_summary.get('last_counted'):
        st.caption(f"📅 Count period: {pd.to_datetime(team_summary['first_counted']).strftime('%Y-%m-%d %H:%M')} - {pd.to_datetime(team_summary['last_counted']).strftime('%Y-%m-%d %H:%M')}")
    
    # Top products chart
    st.markdown("#### 📈 Top Products by Quantity (Team)")
    
    top_products = get_team_top_products(st.session_state.selected_session_id)
    
    if top_products:
        # Create DataFrame for visualization
        df_top = pd.DataFrame(top_products)
        df_top['Product'] = df_top.apply(lambda x: f"{x['pt_code']} - {x['product_name'][:30]}..." 
                                         if len(x['product_name']) > 30 
                                         else f"{x['pt_code']} - {x['product_name']}", axis=1)
        
        # Bar chart
        st.bar_chart(df_top.set_index('Product')['total_quantity'])
        
        # Details table
        st.markdown("##### Product Details")
        for _, product in df_top.iterrows():
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            with col1:
                st.write(product['Product'])
            with col2:
                st.metric("Quantity", f"{product['total_quantity']:.0f}")
            with col3:
                st.metric("Records", product['count_records'])
            with col4:
                st.metric("Users", product['unique_users'])

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry for warehouse physical count"""
    init_session_state()
    
    # Check authentication
    if not auth.check_session():
        show_login_page()
    else:
        show_main_app()

def show_login_page():
    """Display login page"""
    st.title("🔑 Login - Warehouse Physical Count")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter username")
            password = st.text_input("Password", type="password", placeholder="Enter password")
            submit = st.form_submit_button("Login", use_container_width=True)
            
            if submit and username and password:
                success, result = auth.authenticate(username, password)
                if success:
                    auth.login(result)
                    st.success("✅ Login successful!")
                    st.rerun()
                else:
                    st.error(f"❌ {result.get('error', 'Login failed')}")

def show_main_app():
    """Main application interface"""
    # Header
    show_header()
    
    # Summary bar with team data
    show_summary_bar()
    
    # Transaction selector
    transaction_id = show_transaction_selector()
    
    if transaction_id:
        # Show team counts if toggled
        if st.session_state.show_team_counts:
            st.markdown("---")
            st.markdown("## 👥 All Team Physical Counts")
            display_team_physical_counts(st.session_state.selected_session_id, transaction_id)
            st.markdown("---")
        
        # Main content in tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📝 Add Items", "📋 Review & Save", "📊 Team Statistics", "📸 Media Gallery"])
        
        with tab1:
            # Entry form
            show_entry_form()
            
            # Show compact preview in sidebar
            with st.sidebar:
                st.markdown("### 📦 Quick Preview")
                summary = get_items_summary()
                st.metric("Pending Items", summary['total_items'])
                if summary['total_attachments'] > 0:
                    st.metric("📎 Attachments", summary['total_attachments'])
                
                if st.session_state.new_items_list:
                    for item in st.session_state.new_items_list[-5:]:  # Show last 5
                        status = "📦" if item.get('product_id') else "❓"
                        product_info = f"{item['product_name'][:20]}..."
                        if item.get('product_id'):
                            product_info += f" (ID: {item['product_id']})"
                        
                        # Check attachments
                        temp_id = item.get('temp_id')
                        att_count = len(st.session_state.item_attachments.get(temp_id, []))
                        if att_count > 0:
                            product_info += f" 📎{att_count}"
                        
                        st.caption(f"{status} {product_info} - Qty: {item['actual_quantity']:.0f}")
                
                # Clear cache button
                st.markdown("---")
                if st.button("🔄 Clear Cache", help="Clear cached products and reload"):
                    st.cache_data.clear()
                    st.success("Cache cleared!")
                    st.rerun()
        
        with tab2:
            # Items preview
            show_items_preview()
            
            # Export and Save section
            if st.session_state.new_items_list:
                st.markdown("---")
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    summary = get_items_summary()
                    st.info(f"💡 Ready to save {summary['total_items']} items ({summary['total_attachments']} attachments) to transaction")
                
                with col2:
                    export_items_to_csv()
                
                with col3:
                    if st.button("💾 Save to Database", use_container_width=True, type="primary"):
                        st.session_state.trigger_save = True
        
        with tab3:
            show_statistics()
        
        with tab4:
            show_media_gallery()
        
        # Handle save action
        handle_save_action(transaction_id)
        
        # Handle clear confirmation
        handle_clear_confirmation()
    
    # Footer
    st.markdown("---")
    if st.session_state.last_save_time:
        st.caption(f"Last save: {st.session_state.last_save_time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
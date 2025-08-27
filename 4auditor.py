# 4auditor.py - Enhanced Warehouse Audit System with Media Attachments
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import logging
from typing import Dict, List, Optional, Tuple
from functools import lru_cache
from sqlalchemy import text
import mimetypes
import os

# Import existing utilities
from utils.auth import AuthManager
from utils.config import config
from utils.db import get_db_engine
from utils.s3_utils import S3Manager

# Import our services
from audit_service import AuditService, AuditException, SessionNotFoundException, InvalidTransactionStateException, CountValidationException
from audit_queries import AuditQueries

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Warehouse Audit System",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize services
auth = AuthManager()
audit_service = AuditService()
queries = AuditQueries()
s3_manager = S3Manager()

# ============== CONSTANTS ==============
ALLOWED_IMAGE_TYPES = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']
ALLOWED_DOC_TYPES = ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'txt']
MAX_FILE_SIZE_MB = 10

# ============== SIMPLIFIED SESSION STATE ==============

def init_session_state():
    """Initialize session state variables"""
    defaults = {
        # Core states
        'temp_counts': [],
        'selected_product': None,
        'selected_batch': None,
        'form_batch_no': '',
        'form_location': '',
        'form_expiry': None,
        
        # UI feedback
        'last_action': None,
        'last_action_time': None,
        
        # Cache keys
        'products_map': {},
        'batches_map': {},
        
        # Display control
        'show_teamwork_view': False,
        'show_attachments': {},
        
        # Loading states
        'products_loaded': False,
        'current_warehouse_id': None,
        'product_options': ["-- Select Product --"],
        
        # Media attachments
        'pending_attachments': [],  # Temporary storage for files before saving
        'count_attachments': {},  # Map count index to attachments
    }
    
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

# ============== CACHE FUNCTIONS ==============

@st.cache_data(ttl=3600)
def cached_get_warehouses():
    """Cached wrapper for get_warehouses"""
    return audit_service.get_warehouses()

@st.cache_data(ttl=1800)
def get_warehouse_products(warehouse_id: int):
    """Cached get warehouse products"""
    return audit_service.get_warehouse_products(warehouse_id)

@st.cache_data(ttl=900)
def get_product_batches(warehouse_id: int, product_id: int):
    """Cached get product batch details"""
    return audit_service.get_product_batch_details(warehouse_id, product_id)

@st.cache_data(ttl=600)
def get_count_summary(transaction_id: int):
    """Cached get transaction count summary"""
    return audit_service.get_transaction_count_summary(transaction_id)

@st.cache_data(ttl=300)
def get_sessions_by_status(status: str):
    """Cached get sessions by status"""
    return audit_service.get_sessions_by_status(status)

@st.cache_data(ttl=300)
def get_session_product_summary(session_id: int, product_id: int):
    """Get total counts for a product across all transactions in session"""
    return audit_service.get_product_total_summary(session_id, product_id)

@st.cache_data(ttl=300)
def get_all_products_team_summary(session_id: int):
    """Get team count summary for all products in session"""
    try:
        # Get summary for all products at once
        engine = get_db_engine()
        query = """
        SELECT 
            acd.product_id,
            COUNT(DISTINCT acd.transaction_id) as total_transactions,
            COUNT(DISTINCT acd.created_by_user_id) as total_users,
            COUNT(DISTINCT acd.batch_no) as total_batches,
            COUNT(*) as total_count_records,
            SUM(acd.actual_quantity) as grand_total_counted
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        WHERE at.session_id = :session_id
        AND acd.delete_flag = 0
        AND at.delete_flag = 0
        GROUP BY acd.product_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"session_id": session_id})
            rows = result.fetchall()
            
            # Convert to dictionary keyed by product_id
            summary_dict = {}
            for row in rows:
                summary_dict[row.product_id] = {
                    'total_transactions': row.total_transactions,
                    'total_users': row.total_users,
                    'total_batches': row.total_batches,
                    'total_count_records': row.total_count_records,
                    'grand_total_counted': float(row.grand_total_counted) if row.grand_total_counted else 0
                }
            
            return summary_dict
    except Exception as e:
        logger.error(f"Error getting all products team summary: {e}")
        return {}

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

def display_attachment_preview(attachment: Dict):
    """Display attachment preview based on type"""
    file_type = attachment.get('file_type', 'other')
    
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col1:
        if file_type == 'image':
            st.write("🖼️ Image")
        else:
            st.write("📄 Document")
    
    with col2:
        st.write(f"**{attachment.get('file_name', 'Unknown')}**")
        st.caption(f"Size: {attachment.get('file_size_mb', 0):.2f}MB")
        if attachment.get('description'):
            st.caption(f"Note: {attachment['description']}")
    
    with col3:
        if file_type == 'image' and attachment.get('s3_url'):
            if st.button("👁️ View", key=f"view_{attachment['id']}"):
                st.image(attachment['s3_url'], caption=attachment['file_name'])
        elif attachment.get('s3_url'):
            st.markdown(f"[📥 Download]({attachment['s3_url']})")

def upload_count_attachments(count_id: int, attachments: List[Dict], transaction_code: str) -> List[Dict]:
    """Upload attachments for a count detail"""
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
                
                logger.info(f"Uploaded attachment {file_name} for count {count_id}")
            else:
                st.error(f"Failed to upload {file_name}: {s3_key}")
                
        except Exception as e:
            logger.error(f"Error uploading attachment: {e}")
            st.error(f"Error uploading {attachment['file'].name}: {str(e)}")
    
    return uploaded

# ============== OPTIMIZED CALLBACKS ==============

def on_product_change():
    """Callback when product is selected"""
    selected = st.session_state.product_select
    if selected and selected != "-- Select Product --":
        # Prevent unnecessary updates
        product_data = st.session_state.products_map.get(selected)
        if product_data and (not st.session_state.selected_product or 
                           st.session_state.selected_product.get('product_id') != product_data.get('product_id')):
            st.session_state.selected_product = product_data
            st.session_state.selected_batch = None
            st.session_state.form_batch_no = ''
            st.session_state.form_location = ''
            st.session_state.form_expiry = None

def on_batch_change():
    """Callback when batch is selected"""
    selected = st.session_state.batch_select
    if selected and selected != "-- Manual Entry --":
        batch_no = selected.split(" (")[0].replace("🔴", "").replace("🟡", "").replace("🟢", "").strip()
        batch_data = st.session_state.batches_map.get(batch_no)
        if batch_data:
            st.session_state.selected_batch = batch_data
            st.session_state.form_batch_no = batch_no
            st.session_state.form_location = batch_data.get('location', '')
            if batch_data.get('expired_date'):
                try:
                    st.session_state.form_expiry = pd.to_datetime(batch_data['expired_date']).date()
                except:
                    st.session_state.form_expiry = None
    else:
        st.session_state.selected_batch = None
        st.session_state.form_batch_no = ''
        st.session_state.form_location = ''
        st.session_state.form_expiry = None

def add_count_callback():
    """Add count to temporary list with attachments"""
    qty = st.session_state.get('qty_input', 0)
    batch_no = st.session_state.get('batch_input', '')
    location = st.session_state.get('loc_input', '')
    notes = st.session_state.get('notes_input', '')
    expiry = st.session_state.get('expiry_input', None)
    
    if qty > 0 and st.session_state.selected_product:
        # Parse location
        zone, rack, bin = '', '', ''
        if location and '-' in location:
            parts = location.split('-', 2)
            zone = parts[0].strip() if len(parts) > 0 else ''
            rack = parts[1].strip() if len(parts) > 1 else ''
            bin = parts[2].strip() if len(parts) > 2 else ''
        else:
            zone = location.strip() if location else ''
        
        # Create count data
        count = {
            'transaction_id': st.session_state.tx_id,
            'product_id': st.session_state.selected_product['product_id'],
            'product_name': st.session_state.selected_product['product_name'],
            'pt_code': st.session_state.selected_product.get('pt_code', ''),
            'batch_no': batch_no,
            'expired_date': expiry,
            'zone_name': zone,
            'rack_name': rack,
            'bin_name': bin,
            'location_notes': '',
            'system_quantity': st.session_state.selected_batch['quantity'] if st.session_state.selected_batch else 0,
            'system_value_usd': st.session_state.selected_batch.get('value_usd', 0) if st.session_state.selected_batch else 0,
            'actual_quantity': qty,
            'actual_notes': notes,
            'created_by_user_id': st.session_state.user_id,
            'time': datetime.now().strftime('%H:%M:%S')
        }
        
        count_index = len(st.session_state.temp_counts)
        st.session_state.temp_counts.append(count)
        
        # Store pending attachments for this count
        if st.session_state.pending_attachments:
            st.session_state.count_attachments[count_index] = st.session_state.pending_attachments.copy()
            st.session_state.pending_attachments = []
        
        st.session_state.last_action = f"✅ Added count #{count_index + 1}"
        st.session_state.last_action_time = datetime.now()
        
        # Clear form inputs
        st.session_state.qty_input = 0
        st.session_state.notes_input = ''

def save_counts_callback():
    """Save all counts to database with attachments"""
    if st.session_state.temp_counts:
        try:
            st.session_state.last_action = "💾 Saving counts and uploading media..."
            
            # Get transaction code for S3 organization
            tx_info = audit_service.get_transaction_info(st.session_state.tx_id)
            transaction_code = tx_info.get('transaction_code', f'TXN_{st.session_state.tx_id}')
            
            # Save counts and get IDs
            saved_ids, errors = audit_service.save_batch_counts(st.session_state.temp_counts)
            
            # Upload attachments for each successfully saved count
            for idx, (count_data, count_id) in enumerate(zip(st.session_state.temp_counts, saved_ids)):
                if count_id and idx in st.session_state.count_attachments:
                    attachments = st.session_state.count_attachments[idx]
                    # count_id is the entity_id for entity_type='count_detail'
                    upload_count_attachments(count_id, attachments, transaction_code)
            
            # Count successful saves
            successful_saves = len([id for id in saved_ids if id is not None])
            
            if errors and successful_saves == 0:
                st.session_state.last_action = f"❌ Failed to save items: {errors[0]}"
            elif errors:
                st.session_state.last_action = f"⚠️ Saved {successful_saves} counts with {len(errors)} errors"
                for error in errors[:3]:  # Show first 3 errors
                    st.caption(f"• {error}")
            else:
                st.session_state.last_action = f"✅ Successfully saved {successful_saves} counts!"
                st.session_state.temp_counts = []
                st.session_state.count_attachments = {}
                # Clear relevant caches
                get_count_summary.clear()
                get_session_product_summary.clear()
                get_all_products_team_summary.clear()
                # Force reload of products to update status
                st.session_state.products_loaded = False
            
            st.session_state.last_action_time = datetime.now()
            
        except Exception as e:
            st.session_state.last_action = f"❌ Error: {str(e)}"
            st.session_state.last_action_time = datetime.now()
            logger.error(f"Save error: {e}")

# ============== DISPLAY FUNCTIONS ==============

def display_teamwork_counts(session_id: int, product_id: int, current_tx_id: int):
    """Display all counts for a product across all transactions with attachments"""
    try:
        # Get all counts
        all_counts = audit_service.get_product_counts_all_transactions(session_id, product_id)
        
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
                tx_total_qty = sum(c['total_counted'] for c in tx_data['counts'])
                tx_total_records = sum(c['count_records'] for c in tx_data['counts'])
                tx_users = len(set(c['counted_by'] for c in tx_data['counts']))
                
                is_current = (tx_id == current_tx_id)
                status_emoji = "✅" if tx_data['transaction_status'] == 'completed' else "📝"
                current_indicator = " 👈 (Current)" if is_current else ""
                
                st.markdown(f"### {status_emoji} {tx_data['transaction_code']} - {tx_data['transaction_name']}{current_indicator}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("👥 Users", tx_users)
                with col2:
                    st.metric("📊 Records", tx_total_records)
                with col3:
                    st.metric("📢 Total", f"{tx_total_qty:.0f}")
                
                # Show count details
                for count in tx_data['counts']:
                    with st.container():
                        col1, col2, col3, col4 = st.columns([2, 1.5, 1, 2.5])
                        
                        with col1:
                            st.write(f"**{count['counter_name'] or count['counted_by']}**")
                            st.caption(f"@{count['counted_by']}")
                        
                        with col2:
                            st.write(f"Batch: {count['batch_no'] or 'N/A'}")
                            st.caption(f"{count['count_records']} records")
                        
                        with col3:
                            st.write(f"Qty: {count['total_counted']:.0f}")
                        
                        with col4:
                            locations = count['locations'].split(',') if count['locations'] else []
                            st.write(f"📍 {len(locations)} locations")
                            st.caption(f"Last: {pd.to_datetime(count['last_counted']).strftime('%H:%M')}")
                
                st.markdown("---")
                
        else:
            st.info("No counts recorded for this product yet")
            
    except Exception as e:
        st.error(f"Error loading teamwork view: {str(e)}")

def render_temp_counts():
    """Display temporary counts with attachments"""
    if st.session_state.temp_counts:
        st.markdown(f"### 📋 Pending Counts ({len(st.session_state.temp_counts)})")
        
        # Group by product
        grouped = {}
        for count in st.session_state.temp_counts:
            key = count['product_id']
            if key not in grouped:
                grouped[key] = {
                    'product_name': count['product_name'],
                    'counts': []
                }
            grouped[key]['counts'].append(count)
        
        # Display grouped
        for product_id, group in grouped.items():
            total_qty = sum(c['actual_quantity'] for c in group['counts'])
            st.markdown(f"**{group['product_name']}** - {len(group['counts'])} records, Total: {total_qty:.0f}")
            
            for i, count in enumerate(group['counts']):
                idx = st.session_state.temp_counts.index(count)
                
                with st.expander(f"Count #{idx + 1}: {count['actual_quantity']:.0f} @ {count['zone_name']}{'-' + count['rack_name'] if count['rack_name'] else ''}{'-' + count['bin_name'] if count['bin_name'] else ''}"):
                    col1, col2, col3 = st.columns([2, 2, 1])
                    
                    with col1:
                        st.write(f"**Batch:** {count.get('batch_no', 'N/A')}")
                        st.write(f"**Time:** {count['time']}")
                    
                    with col2:
                        if count.get('actual_notes'):
                            st.write(f"**Notes:** {count['actual_notes']}")
                        
                        # Show attachments if any
                        if idx in st.session_state.count_attachments:
                            attachments = st.session_state.count_attachments[idx]
                            st.write(f"**📎 Attachments:** {len(attachments)}")
                            for att in attachments:
                                st.caption(f"• {att['file'].name} ({att['file'].size / 1024:.1f}KB)")
                    
                    with col3:
                        if st.button("❌ Remove", key=f"del_{idx}"):
                            st.session_state.temp_counts.pop(idx)
                            if idx in st.session_state.count_attachments:
                                del st.session_state.count_attachments[idx]
                            st.session_state.last_action = "🗑️ Removed count"
                            st.session_state.last_action_time = datetime.now()
                            st.rerun()

# ============== MAIN COUNTING INTERFACE ==============

@st.fragment(run_every=None)
def counting_form_fragment():
    """Isolated counting form with media upload"""
    
    if not st.session_state.selected_product:
        st.info("👆 Please select a product above")
        return
    
    # Product info display
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"**{st.session_state.selected_product['product_name']}**")
        st.caption(f"PT Code: {st.session_state.selected_product.get('pt_code', 'N/A')}")
    with col2:
        st.metric("System Total", f"{st.session_state.selected_product.get('total_quantity', 0):.0f}")
    
    st.markdown("---")
    
    # Form inputs with media upload
    with st.form("counting_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        
        with col1:
            batch_no = st.text_input(
                "Batch Number",
                key="batch_input_form",
                value=st.session_state.form_batch_no,
                placeholder="Enter batch or select from dropdown"
            )
            
            expiry = st.date_input(
                "Expiry Date",
                key="expiry_input_form",
                value=st.session_state.form_expiry,
                min_value=date(2020, 1, 1),
                max_value=date(2030, 12, 31)
            )
            
            qty = st.number_input(
                "Actual Quantity*",
                min_value=0.0,
                step=1.0,
                key="qty_input_form",
                format="%.2f"
            )
        
        with col2:
            location = st.text_input(
                "Location",
                key="loc_input_form",
                value=st.session_state.form_location,
                placeholder="e.g., A1-R01-B01"
            )
            
            notes = st.text_area(
                "Notes",
                key="notes_input_form",
                height=100,
                placeholder="Any observations"
            )
        
        # Media upload section
        st.markdown("### 📎 Attachments (Optional)")
        
        col1, col2 = st.columns(2)
        with col1:
            uploaded_files = st.file_uploader(
                "Upload Images/Documents",
                type=ALLOWED_IMAGE_TYPES + ALLOWED_DOC_TYPES,
                accept_multiple_files=True,
                key="file_uploader",
                help=f"Max {MAX_FILE_SIZE_MB}MB per file"
            )
        
        with col2:
            attachment_notes = st.text_area(
                "Attachment Notes",
                key="attachment_notes",
                placeholder="Describe the attachments (optional)",
                height=100
            )
        
        # Form submission
        submit_col1, submit_col2 = st.columns(2)
        
        with submit_col1:
            add_submitted = st.form_submit_button(
                f"➕ Add Count ({len(st.session_state.temp_counts)}/20)",
                type="primary",
                use_container_width=True,
                disabled=len(st.session_state.temp_counts) >= 20
            )
        
        with submit_col2:
            save_submitted = st.form_submit_button(
                f"💾 Save All ({len(st.session_state.temp_counts)})",
                use_container_width=True,
                disabled=len(st.session_state.temp_counts) == 0
            )
        
        # Handle form submission
        if add_submitted:
            if qty > 0 and st.session_state.selected_product:
                # Update session state values
                st.session_state.qty_input = qty
                st.session_state.batch_input = batch_no
                st.session_state.loc_input = location
                st.session_state.notes_input = notes
                st.session_state.expiry_input = expiry
                
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
                
                # Add count
                add_count_callback()
                st.rerun()
            else:
                st.warning("⚠️ Please enter a quantity greater than 0")
        
        elif save_submitted:
            save_counts_callback()
            st.rerun()
    
    # Clear all button outside form
    if st.button("🗑️ Clear All", use_container_width=True):
        st.session_state.temp_counts = []
        st.session_state.count_attachments = {}
        st.session_state.pending_attachments = []
        st.session_state.last_action = "🗑️ Cleared all pending counts"
        st.session_state.last_action_time = datetime.now()
        st.rerun()

def counting_page():
    """Main counting page with media support"""
    st.subheader("🚀 Fast Counting Mode")
    
    init_session_state()
    
    # Check prerequisites
    if 'selected_session_id' not in st.session_state:
        st.warning("⚠️ Please select a session in Transactions tab first")
        return
    
    # Get draft transactions
    try:
        transactions = audit_service.get_user_transactions(
            st.session_state.selected_session_id,
            st.session_state.user_id,
            status='draft'
        )
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")
        return
    
    if not transactions:
        st.warning("⚠️ No draft transactions available for counting")
        st.info("Please create a new transaction in the Transactions tab")
        return
    
    # Transaction selector
    tx_options = {f"{t['transaction_name']} ({t['transaction_code']})": t for t in transactions}
    selected_tx_key = st.selectbox(
        "Select Transaction",
        list(tx_options.keys()),
        help="Select the transaction you want to count for"
    )
    
    selected_tx = tx_options[selected_tx_key]
    st.session_state.tx_id = selected_tx['id']
    warehouse_id = selected_tx['warehouse_id']
    
    # Show action status
    if st.session_state.last_action and st.session_state.last_action_time:
        time_diff = (datetime.now() - st.session_state.last_action_time).seconds
        if time_diff < 3:
            if "✅" in st.session_state.last_action:
                st.success(st.session_state.last_action)
            elif "⚠️" in st.session_state.last_action:
                st.warning(st.session_state.last_action)
            elif "❌" in st.session_state.last_action:
                st.error(st.session_state.last_action)
            else:
                st.info(st.session_state.last_action)
    
    # Display temporary counts
    render_temp_counts()
    
    st.markdown("### 📦 Product Selection")
    
    # Initialize loading state
    if 'products_loaded' not in st.session_state:
        st.session_state.products_loaded = False
        st.session_state.current_warehouse_id = None
    
    # Check if we need to reload products
    if (not st.session_state.products_loaded or 
        st.session_state.current_warehouse_id != warehouse_id):
        
        with st.spinner("Loading products..."):
            try:
                # Get products
                products = get_warehouse_products(warehouse_id)
                
                # Get team count summaries for ALL products at once (efficient)
                team_summaries = get_all_products_team_summary(st.session_state.selected_session_id)
                
                # Build product options
                product_options = ["-- Select Product --"]
                products_map = {}
                
                for p in products:
                    product_id = p['product_id']
                    system_qty = p.get('total_quantity', 0)
                    
                    # Get team count info from pre-loaded summaries
                    team_summary = team_summaries.get(product_id, {})
                    team_counted_qty = team_summary.get('grand_total_counted', 0)
                    team_count_records = team_summary.get('total_count_records', 0)
                    
                    # Check temp counts
                    temp_qty = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                                   if tc.get('product_id') == product_id)
                    
                    # Determine status based on TEAM counted quantity
                    if temp_qty > 0:
                        status = "📝"  # Has pending counts
                    elif team_counted_qty >= system_qty * 0.95 and system_qty > 0:
                        status = "✅"  # Fully counted (95%+)
                    elif team_counted_qty > 0:
                        status = "🟡"  # Partially counted
                    else:
                        status = "⭕"  # Not counted
                    
                    # Format display
                    product_name = p.get('product_name', 'Unknown')
                    package_size = p.get('package_size', 'Unknown')
                    brand = p.get('brand', 'Unknown')

                    # Cut strings to 40 chars
                    product_display = product_name[:40] + ("..." if len(product_name) > 40 else "")
                    package_display = package_size[:40] + ("..." if len(package_size) > 40 else "")

                    display = f"{status} {p.get('pt_code', 'N/A')} - {product_display} || {package_display} ({brand})"

                    if team_counted_qty > 0:
                        display += f" [{team_count_records} records, {team_counted_qty:.0f}/{system_qty:.0f}]"
                    else:
                        display += f" [System: {system_qty:.0f}]"
                    
                    product_options.append(display)
                    products_map[display] = p
                
                # Store in session state
                st.session_state.product_options = product_options
                st.session_state.products_map = products_map
                st.session_state.products_loaded = True
                st.session_state.current_warehouse_id = warehouse_id
                
            except Exception as e:
                st.error(f"Error loading products: {str(e)}")
                st.session_state.products_loaded = False
                return
    
    # Product selector (use stored options)
    col1, col2 = st.columns([5, 1])
    with col1:
        # Get current selection
        current_selection = None
        if st.session_state.selected_product:
            # Find current product in options
            for opt in st.session_state.get('product_options', []):
                if opt in st.session_state.products_map:
                    prod_data = st.session_state.products_map[opt]
                    if prod_data.get('product_id') == st.session_state.selected_product.get('product_id'):
                        current_selection = opt
                        break
        
        selected = st.selectbox(
            "Select Product",
            st.session_state.get('product_options', ["-- Select Product --"]),
            index=st.session_state.get('product_options', ["-- Select Product --"]).index(current_selection) if current_selection else 0,
            key="product_select",
            on_change=on_product_change,
            help="⭕ Not counted | 📝 Has pending counts"
        )
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            # Clear caches and reload flags
            get_warehouse_products.clear()
            get_count_summary.clear()
            get_session_product_summary.clear()
            get_all_products_team_summary.clear()
            st.session_state.products_loaded = False
            st.rerun()
    
    # Load team count data separately (after product selection)
    if st.session_state.selected_product and 'product_id' in st.session_state.selected_product:
        try:
            summary = get_session_product_summary(
                st.session_state.selected_session_id, 
                st.session_state.selected_product['product_id']
            )
            
            if summary.get('total_count_records', 0) > 0:
                # Update status display with team counts
                team_counted_qty = summary.get('grand_total_counted', 0)
                system_qty = st.session_state.selected_product.get('total_quantity', 0)
                
                # Show completion status
                if team_counted_qty >= system_qty * 0.95:
                    st.success(f"✅ Product fully counted by team: {team_counted_qty:.0f}/{system_qty:.0f}")
                elif team_counted_qty > 0:
                    st.warning(f"🟡 Partially counted by team: {team_counted_qty:.0f}/{system_qty:.0f}")
                
                # Team counts button
                if st.button(
                    f"👥 View All Team Counts ({summary['total_users']} users, "
                    f"{summary['total_transactions']} transactions)",
                    key="toggle_teamwork"
                ):
                    st.session_state.show_teamwork_view = not st.session_state.show_teamwork_view
                
                # Show teamwork view if toggled
                if st.session_state.show_teamwork_view:
                    with st.container():
                        st.markdown("---")
                        display_teamwork_counts(
                            st.session_state.selected_session_id,
                            st.session_state.selected_product['product_id'],
                            selected_tx['id']
                        )
                        st.markdown("---")
        except Exception as e:
            logger.error(f"Error loading team counts: {e}")
    
    # Batch selector
    if st.session_state.selected_product:
        batches = get_product_batches(warehouse_id, st.session_state.selected_product['product_id'])
        
        if batches:
            st.markdown("### 📦 Batch Selection (Optional)")
            
            batch_options = ["-- Manual Entry --"]
            batches_map = {}
            
            today = date.today()
            
            for batch in batches:
                # Check expiry status
                status = ""
                if batch.get('expired_date'):
                    try:
                        exp_date = pd.to_datetime(batch['expired_date']).date()
                        if exp_date < today:
                            status = "🔴 "  # Expired
                        elif exp_date < today + timedelta(days=90):
                            status = "🟡 "  # Expiring soon
                        else:
                            status = "🟢 "  # Normal
                    except:
                        pass
                
                # Format option
                qty_str = f"{batch['quantity']:.0f}"
                loc_str = batch.get('location', 'N/A')
                option = f"{status}{batch['batch_no']} (Qty: {qty_str}, Loc: {loc_str})"
                
                batch_options.append(option)
                batches_map[batch['batch_no']] = batch
            
            st.session_state.batches_map = batches_map
            
            st.selectbox(
                "Select Batch or Manual Entry",
                batch_options,
                key="batch_select",
                on_change=on_batch_change,
                help="🔴 Expired | 🟡 Expiring Soon (<90 days) | 🟢 Normal"
            )
            
            st.info("💡 **Multiple Counts Allowed**: You can count the same batch multiple times from different locations")
        
        st.markdown("### ✏️ Count Entry")
    
    # Counting form with media
    counting_form_fragment()

# ============== ROLE PERMISSIONS ==============

# Role permissions
AUDIT_ROLES = {
    # Executive Level - Full access
    'admin': ['manage_sessions', 'view_all', 'create_transactions', 'export_data', 'user_management'],
    'GM': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'MD': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    
    # Management Level - Session management + full view
    'supply_chain': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'sales_manager': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    
    # Operational Level - Can participate in audits
    'sales': ['create_transactions', 'view_own', 'view_assigned_sessions'],
    
    # View Level - Read only
    'viewer': ['view_own', 'view_assigned_sessions'],
    
    # External/Restricted - Limited or no access
    'customer': [],  # No audit access
    'vendor': []     # No audit access
}

def check_permission(action: str) -> bool:
    """Check if current user has permission for action"""
    user_role = st.session_state.get('user_role', 'viewer')
    return action in AUDIT_ROLES.get(user_role, [])

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry point"""
    try:
        if not auth.check_session():
            show_login_page()
        else:
            show_main_app()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        logger.error(f"Main app error: {e}")

def show_login_page():
    """Display simple login page"""
    st.title("📦 Warehouse Audit System")
    st.markdown("### Please login to access the system")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            
            submit = st.form_submit_button("🔐 Login", use_container_width=True)
            
            if submit:
                if username and password:
                    try:
                        success, result = auth.authenticate(username, password)
                        
                        if success:
                            auth.login(result)
                            st.success("✅ Login successful!")
                            st.rerun()
                        else:
                            st.error("❌ Invalid username or password")
                    except Exception as e:
                        logger.error(f"Login error: {e}")
                        st.error(f"❌ Login error: {str(e)}")
                else:
                    st.warning("⚠️ Please enter both username and password")

def show_main_app():
    """Display main application interface"""
    init_session_state()
    
    # Sidebar with user info
    with st.sidebar:
        st.markdown("### 👤 User Info")
        display_name = st.session_state.get('employee_name') or st.session_state.get('username', 'User')
        st.write(f"**Name:** {display_name}")
        st.write(f"**Role:** {st.session_state.get('user_role', 'N/A')}")
        
        # Login time
        login_time = st.session_state.get('login_time')
        if login_time:
            st.write(f"**Login:** {login_time.strftime('%H:%M')}")
        
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    # Main content based on role
    user_role = st.session_state.get('user_role', 'viewer')
    
    if not AUDIT_ROLES.get(user_role, []):
        show_no_access_interface()
    elif check_permission('create_transactions'):
        show_audit_interface()
    else:
        show_viewer_interface()

def show_no_access_interface():
    """Interface for users without audit permissions"""
    st.title("🚫 Access Restricted")
    st.warning("⚠️ You don't have permission to access the Audit System")
    st.info("Please contact your administrator for access")

def show_audit_interface():
    """Main audit interface"""
    st.title("📦 Warehouse Audit System")
    
    tab1, tab2, tab3 = st.tabs(["📄 Transactions", "🚀 Fast Counting", "📸 Media Gallery"])
    
    with tab1:
        show_transactions_page()
    
    with tab2:
        counting_page()
    
    with tab3:
        show_media_gallery()

def show_viewer_interface():
    """Read-only viewer interface"""
    st.title("👀 Audit Viewer")
    st.info("You have read-only access to audit data")

def show_transactions_page():
    """Transactions management page"""
    st.subheader("📄 My Audit Transactions")
    
    try:
        # Get active sessions
        sessions = get_sessions_by_status('in_progress')
        
        if not sessions:
            st.warning("⚠️ No active audit sessions available")
            st.info("Please wait for an administrator to start an audit session")
            return
        
        # Session selector
        session_options = {
            f"{s['session_name']} ({s['session_code']})": s['id'] 
            for s in sessions
        }
        
        selected_session_key = st.selectbox(
            "Select Active Session",
            list(session_options.keys()),
            help="Select the audit session you want to work on"
        )
        
        selected_session_id = session_options[selected_session_key]
        st.session_state.selected_session_id = selected_session_id
        
        # Create new transaction form
        with st.expander("➕ Create New Transaction", expanded=False):
            with st.form("create_transaction"):
                col1, col2 = st.columns(2)
                
                with col1:
                    tx_name = st.text_input(
                        "Transaction Name*",
                        placeholder="e.g., Zone A1-A3 counting"
                    )
                    zones = st.text_input(
                        "Assigned Zones",
                        placeholder="e.g., A1,A2,A3"
                    )
                
                with col2:
                    categories = st.text_input(
                        "Categories",
                        placeholder="e.g., Antibiotics, Cold items"
                    )
                    notes = st.text_area(
                        "Notes",
                        placeholder="Additional notes"
                    )
                
                if st.form_submit_button("Create Transaction", use_container_width=True):
                    if tx_name:
                        try:
                            tx_code = audit_service.create_transaction({
                                'session_id': selected_session_id,
                                'transaction_name': tx_name,
                                'assigned_zones': zones,
                                'assigned_categories': categories,
                                'notes': notes,
                                'created_by_user_id': st.session_state.user_id
                            })
                            st.success(f"✅ Transaction created! Code: {tx_code}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                    else:
                        st.warning("⚠️ Please enter transaction name")
        
        # Display user transactions
        st.markdown("### My Transactions")
        
        transactions = audit_service.get_user_transactions(
            selected_session_id,
            st.session_state.user_id
        )
        
        if transactions:
            for tx in transactions:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        st.write(f"**{tx['transaction_name']}**")
                        st.caption(f"Code: {tx['transaction_code']}")
                    
                    with col2:
                        status_color = {
                            'draft': '🟡',
                            'completed': '✅'
                        }
                        st.write(f"{status_color.get(tx['status'], '⭕')} {tx['status'].title()}")
                        if tx.get('assigned_zones'):
                            st.caption(f"Zones: {tx['assigned_zones']}")
                    
                    with col3:
                        st.write(f"Items: {tx.get('total_items_counted', 0)}")
                        st.caption(f"Created: {pd.to_datetime(tx['created_date']).strftime('%m/%d %H:%M')}")
                    
                    with col4:
                        if tx['status'] == 'draft':
                            if tx.get('total_items_counted', 0) > 0:
                                if st.button("✅ Submit", key=f"submit_{tx['id']}"):
                                    try:
                                        audit_service.submit_transaction(tx['id'], st.session_state.user_id)
                                        st.success("✅ Transaction submitted!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ Error: {str(e)}")
                            else:
                                st.caption("No counts yet")
                        else:
                            st.caption("Completed ✅")
                    
                    st.markdown("---")
        else:
            st.info("No transactions created yet")
            
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")
        logger.error(f"Transactions page error: {e}")

def show_media_gallery():
    """Display media gallery for current session"""
    st.subheader("📸 Media Gallery")
    
    if 'selected_session_id' not in st.session_state:
        st.warning("⚠️ Please select a session in Transactions tab first")
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
    
    # Get attachments based on filters
    try:
        # For now, show attachments from all user's transactions
        all_attachments = []
        
        for tx in user_transactions:
            if tx_filter != "All" and tx['transaction_code'] not in tx_filter:
                continue
            
            # Get counts for this transaction
            counts = audit_service.get_recent_counts(tx['id'], limit=100)
            
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
                    att['product_name'] = count.get('product_name', 'Unknown')
                    att['batch_no'] = count.get('batch_no', 'N/A')
                    att['counted_date'] = count.get('counted_date')
                    
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
                        st.markdown(f"**{att['product_name']}**")
                        st.caption(f"Batch: {att['batch_no']} | {att['transaction_name']}")
                        
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
            st.info("No media attachments found for the selected filters")
            
    except Exception as e:
        st.error(f"Error loading media gallery: {str(e)}")
        logger.error(f"Media gallery error: {e}")

if __name__ == "__main__":
    main()
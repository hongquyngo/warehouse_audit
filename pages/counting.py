# unified_counting.py - High Performance Unified Counting System
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import logging
from typing import Dict, List, Optional, Tuple, Literal
from functools import lru_cache
import time
from sqlalchemy import text

# Import existing utilities
from utils.auth import AuthManager
from utils.db import get_db_engine
from audit_service import AuditService
from audit_queries import AuditQueries

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Unified Counting System",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize services
auth = AuthManager()
audit_service = AuditService()

# Constants
MAX_PENDING_COUNTS = 50
CACHE_TTL_PRODUCTS = 3600
CACHE_TTL_TEAM = 300

# Role permissions (same as main.py)
AUDIT_ROLES = {
    'admin': ['manage_sessions', 'view_all', 'create_transactions', 'export_data', 'user_management'],
    'GM': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'MD': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'supply_chain': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'sales_manager': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'sales': ['create_transactions', 'view_own', 'view_assigned_sessions'],
    'viewer': ['view_own', 'view_assigned_sessions'],
    'customer': [],
    'vendor': []
}

def check_permission(action: str) -> bool:
    """Check if current user has permission for action"""
    user_role = st.session_state.get('user_role', 'viewer')
    return action in AUDIT_ROLES.get(user_role, [])

# ============== SESSION STATE INITIALIZATION ==============

def init_session_state():
    """Initialize all session state variables with optimized defaults"""
    defaults = {
        # Core states
        'count_mode': 'inventory',  # 'inventory' | 'physical'
        'pending_counts': [],
        'edit_mode': {'active': False, 'index': None},
        
        # UI states
        'selected_product': None,
        'selected_batch': None,
        'show_team_view': False,
        'last_action': None,
        'last_action_time': None,
        
        # Form states
        'form_key': 0,
        'default_location': {'zone': '', 'rack': '', 'bin': ''},
        
        # Cache states
        'products_loaded': False,
        'current_warehouse_id': None,
        'products_cache': {},
        'team_cache_time': None,
        
        # Performance states
        'batch_save_in_progress': False,
        'pending_save_count': 0,
        
        # View mode states
        'view_only_mode': False,
        'selected_view_session': None,
        'selected_view_transaction': None,
        
        # Track mode changes
        'previous_count_mode': None,
        'previous_warehouse_id': None
    }
    
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default
    
    # Clear product cache if mode or warehouse changed
    if (st.session_state.get('previous_count_mode') != st.session_state.get('count_mode') or
        st.session_state.get('previous_warehouse_id') != st.session_state.get('selected_warehouse_id')):
        # Clear product cache
        for key in list(st.session_state.keys()):
            if key.startswith('products_'):
                del st.session_state[key]
        # Update trackers
        st.session_state.previous_count_mode = st.session_state.get('count_mode')
        st.session_state.previous_warehouse_id = st.session_state.get('selected_warehouse_id')

# ============== PERFORMANCE OPTIMIZED CACHE ==============

@st.cache_data(ttl=CACHE_TTL_PRODUCTS)
def get_products_for_mode(warehouse_id: int, mode: Literal['inventory', 'physical']) -> List[Dict]:
    """Get products based on counting mode"""
    if mode == 'inventory':
        # Only products with inventory in warehouse
        return audit_service.get_warehouse_products(warehouse_id)
    else:
        # All products from master
        query = """
        SELECT DISTINCT
            p.id as product_id,
            p.name as product_name,
            p.pt_code,
            COALESCE(p.legacy_pt_code, '') as legacy_code,
            b.brand_name as brand,
            p.package_size,
            -- Check if has inventory
            CASE WHEN idv.product_id IS NOT NULL THEN 1 ELSE 0 END as has_inventory
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        LEFT JOIN (
            SELECT DISTINCT product_id 
            FROM inventory_detailed_view 
            WHERE warehouse_id = :warehouse_id 
            AND remaining_quantity > 0
        ) idv ON p.id = idv.product_id
        WHERE p.delete_flag = 0
        ORDER BY p.pt_code
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {"warehouse_id": warehouse_id})
            return [dict(row._mapping) for row in result.fetchall()]

@st.cache_data(ttl=CACHE_TTL_TEAM)
def get_team_counts_summary(session_id: int, count_mode: str) -> Dict:
    """Get team counting summary with mode filter"""
    try:
        is_new_filter = "1" if count_mode == "physical" else "0"
        
        query = """
        SELECT 
            COUNT(DISTINCT acd.created_by_user_id) as total_users,
            COUNT(DISTINCT acd.transaction_id) as total_transactions,
            COUNT(DISTINCT acd.product_id) as unique_products,
            COUNT(*) as total_records,
            SUM(acd.actual_quantity) as total_quantity
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        WHERE at.session_id = :session_id
        AND acd.is_new_item = :is_new
        AND acd.delete_flag = 0
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {
                "session_id": session_id,
                "is_new": is_new_filter
            })
            row = result.fetchone()
            
            if row:
                return {
                    'total_users': row.total_users or 0,
                    'total_transactions': row.total_transactions or 0,
                    'unique_products': row.unique_products or 0,
                    'total_records': row.total_records or 0,
                    'total_quantity': float(row.total_quantity) if row.total_quantity else 0
                }
    except Exception as e:
        logger.error(f"Error getting team summary: {e}")
    
    return {'total_users': 0, 'total_transactions': 0, 'unique_products': 0, 
            'total_records': 0, 'total_quantity': 0}

@st.cache_data(ttl=300)
def check_product_counted(session_id: int, product_id: int, count_mode: str) -> Dict:
    """Check if product already counted by team"""
    try:
        is_new_filter = "1" if count_mode == "physical" else "0"
        
        query = """
        SELECT 
            COUNT(DISTINCT acd.created_by_user_id) as users_count,
            SUM(acd.actual_quantity) as total_quantity,
            COUNT(*) as count_records
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        WHERE at.session_id = :session_id
        AND acd.product_id = :product_id
        AND acd.is_new_item = :is_new
        AND acd.delete_flag = 0
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {
                "session_id": session_id,
                "product_id": product_id,
                "is_new": is_new_filter
            })
            row = result.fetchone()
            
            if row and row.count_records > 0:
                return {
                    'counted': True,
                    'users_count': row.users_count,
                    'total_quantity': float(row.total_quantity),
                    'count_records': row.count_records
                }
    except Exception as e:
        logger.error(f"Error checking product count: {e}")
    
    return {'counted': False, 'users_count': 0, 'total_quantity': 0, 'count_records': 0}

@st.cache_data(ttl=300)
def check_product_counted_batch(session_id: int, product_ids: List[int], count_mode: str) -> Dict[int, Dict]:
    """Check multiple products at once for better performance"""
    try:
        is_new_filter = "1" if count_mode == "physical" else "0"
        
        # Convert list to tuple for SQL IN clause
        product_ids_str = ','.join(str(id) for id in product_ids)
        
        query = f"""
        SELECT 
            acd.product_id,
            COUNT(DISTINCT acd.created_by_user_id) as users_count,
            SUM(acd.actual_quantity) as total_quantity,
            COUNT(*) as count_records
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        WHERE at.session_id = :session_id
        AND acd.product_id IN ({product_ids_str})
        AND acd.is_new_item = :is_new
        AND acd.delete_flag = 0
        GROUP BY acd.product_id
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {
                "session_id": session_id,
                "is_new": is_new_filter
            })
            
            # Build result dict
            counts = {}
            for row in result:
                counts[row.product_id] = {
                    'counted': True,
                    'users_count': row.users_count,
                    'total_quantity': float(row.total_quantity),
                    'count_records': row.count_records
                }
            
            # Add default for products not found
            for pid in product_ids:
                if pid not in counts:
                    counts[pid] = {'counted': False, 'users_count': 0, 'total_quantity': 0, 'count_records': 0}
            
            return counts
    except Exception as e:
        logger.error(f"Error checking batch product counts: {e}")
        return {pid: {'counted': False, 'users_count': 0, 'total_quantity': 0, 'count_records': 0} for pid in product_ids}

@st.cache_data(ttl=300)
def get_all_session_counts(session_id: int) -> pd.DataFrame:
    """Get all counts for a session - for view_all permission users"""
    try:
        query = """
        SELECT 
            acd.*,
            at.transaction_code,
            at.transaction_name,
            at.status as transaction_status,
            u.username,
            CONCAT(e.first_name, ' ', e.last_name) as counter_name,
            p.pt_code,
            p.name as product_name,
            b.brand_name
        FROM audit_count_details acd
        JOIN audit_transactions at ON acd.transaction_id = at.id
        JOIN users u ON acd.created_by_user_id = u.id
        LEFT JOIN employees e ON u.employee_id = e.id
        LEFT JOIN products p ON acd.product_id = p.id
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE at.session_id = :session_id
        AND acd.delete_flag = 0
        AND at.delete_flag = 0
        ORDER BY acd.counted_date DESC
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {"session_id": session_id})
            df = pd.DataFrame([dict(row._mapping) for row in result.fetchall()])
            return df
    except Exception as e:
        logger.error(f"Error getting session counts: {e}")
        return pd.DataFrame()

# ============== OPTIMIZED CALLBACKS ==============

def add_count_callback():
    """Add count without page rerun"""
    # Get form values
    product = st.session_state.get('selected_product')
    batch_no = st.session_state.get('batch_input', '')
    quantity = st.session_state.get('qty_input', 0)
    location = st.session_state.get('location_input', '')
    notes = st.session_state.get('notes_input', '')
    expiry = st.session_state.get('expiry_input')
    
    # Validate
    if not product:
        st.session_state.last_action = "❌ Please select a product"
        st.session_state.last_action_time = datetime.now()
        return
    
    if quantity <= 0:
        st.session_state.last_action = "❌ Quantity must be greater than 0"
        st.session_state.last_action_time = datetime.now()
        return
    
    # Parse location
    zone, rack, bin_name = parse_location(location)
    if not zone:
        st.session_state.last_action = "❌ Zone is required"
        st.session_state.last_action_time = datetime.now()
        return
    
    # Create count record
    count_data = {
        'temp_id': f"tmp_{int(time.time() * 1000)}_{len(st.session_state.pending_counts)}",
        'transaction_id': st.session_state.get('selected_tx_id'),
        'product_id': product.get('product_id'),
        'product_name': product.get('product_name'),
        'pt_code': product.get('pt_code', ''),
        'brand': product.get('brand', ''),
        'batch_no': batch_no,
        'expired_date': expiry,
        'zone_name': zone,
        'rack_name': rack,
        'bin_name': bin_name,
        'actual_quantity': quantity,
        'actual_notes': notes,
        'system_quantity': st.session_state.get('selected_batch', {}).get('quantity', 0),
        'system_value_usd': st.session_state.get('selected_batch', {}).get('value_usd', 0),
        'count_mode': st.session_state.count_mode,
        'is_new_item': st.session_state.count_mode == 'physical',
        'created_by_user_id': st.session_state.user_id,
        'added_time': datetime.now()
    }
    
    # Add to pending
    st.session_state.pending_counts.append(count_data)
    
    # Update default location
    st.session_state.default_location = {'zone': zone, 'rack': rack, 'bin': bin_name}
    
    # Success feedback
    st.session_state.last_action = f"✅ Added #{len(st.session_state.pending_counts)}"
    st.session_state.last_action_time = datetime.now()
    
    # Reset form
    st.session_state.form_key += 1
    st.session_state.selected_product = None
    st.session_state.selected_batch = None

def update_count_callback(index: int):
    """Update specific count"""
    if 0 <= index < len(st.session_state.pending_counts):
        # Get updated values
        item = st.session_state.pending_counts[index]
        
        # Update from edit form fields
        item['batch_no'] = st.session_state.get(f'edit_batch_{index}', item['batch_no'])
        item['actual_quantity'] = st.session_state.get(f'edit_qty_{index}', item['actual_quantity'])
        item['zone_name'] = st.session_state.get(f'edit_zone_{index}', item['zone_name'])
        item['rack_name'] = st.session_state.get(f'edit_rack_{index}', item['rack_name'])
        item['bin_name'] = st.session_state.get(f'edit_bin_{index}', item['bin_name'])
        item['actual_notes'] = st.session_state.get(f'edit_notes_{index}', item['actual_notes'])
        item['expired_date'] = st.session_state.get(f'edit_expiry_{index}', item['expired_date'])
        item['modified_time'] = datetime.now()
        
        # Close edit mode
        st.session_state.edit_mode = {'active': False, 'index': None}
        st.session_state.last_action = "✅ Count updated"
        st.session_state.last_action_time = datetime.now()

def save_all_counts_callback():
    """Save all pending counts with progress tracking"""
    if not st.session_state.pending_counts:
        st.session_state.last_action = "⚠️ No counts to save"
        st.session_state.last_action_time = datetime.now()
        return
    
    st.session_state.batch_save_in_progress = True
    st.session_state.pending_save_count = len(st.session_state.pending_counts)

# ============== HELPER FUNCTIONS ==============

def parse_location(location: str) -> Tuple[str, str, str]:
    """Parse location string into zone, rack, bin"""
    if not location:
        return '', '', ''
    
    parts = location.strip().split('-', 2)
    zone = parts[0].strip() if len(parts) > 0 else ''
    rack = parts[1].strip() if len(parts) > 1 else ''
    bin_name = parts[2].strip() if len(parts) > 2 else ''
    
    return zone, rack, bin_name

def format_product_display(product: Dict, show_counts: bool = True) -> str:
    """Format product for display with status"""
    display = f"{product.get('pt_code', 'N/A')} - {product.get('product_name', '')[:40]}"
    
    if product.get('brand'):
        display += f" | {product['brand']}"
    
    if show_counts and 'team_count' in product:
        tc = product['team_count']
        if tc['counted']:
            display += f" [👥 {tc['users_count']} users, {tc['total_quantity']:.0f} qty]"
    
    return display

def get_pending_summary() -> Dict:
    """Get summary of pending counts"""
    if not st.session_state.pending_counts:
        return {'total_items': 0, 'total_quantity': 0, 'unique_products': 0}
    
    return {
        'total_items': len(st.session_state.pending_counts),
        'total_quantity': sum(c['actual_quantity'] for c in st.session_state.pending_counts),
        'unique_products': len(set(c['product_id'] for c in st.session_state.pending_counts if c['product_id']))
    }

# ============== UI COMPONENTS ==============

def render_header():
    """Render page header with mode selector"""
    col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])
    
    with col1:
        st.title("📦 Unified Counting System")
    
    with col2:
        # Mode selector - only for users with create_transactions permission and not in view mode
        if check_permission('create_transactions') and not st.session_state.get('view_only_mode', False):
            mode = st.selectbox(
                "Count Mode",
                options=['inventory', 'physical'],
                format_func=lambda x: '📊 Inventory Count' if x == 'inventory' else '🏭 Physical Count',
                key='count_mode',
                help="Inventory: Count items in system | Physical: Count all items in warehouse"
            )
    
    with col3:
        st.metric("User", auth.get_user_display_name())
    
    with col4:
        if st.button("🏠 Home", use_container_width=True):
            st.switch_page("main.py")
    
    with col5:
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()

def render_summary_bar():
    """Render summary statistics bar"""
    # Get summaries
    pending_summary = get_pending_summary()
    team_summary = {}
    
    session_id = st.session_state.get('selected_session_id') or st.session_state.get('selected_view_session')
    
    if session_id:
        team_summary = get_team_counts_summary(session_id, st.session_state.count_mode)
    
    col1, col2, col3 = st.columns([4, 4, 4])
    
    with col1:
        st.markdown("### 📋 Your Pending")
        if pending_summary['total_items'] > 0:
            subcol1, subcol2, subcol3 = st.columns(3)
            with subcol1:
                st.metric("Items", pending_summary['total_items'])
            with subcol2:
                st.metric("Quantity", f"{pending_summary['total_quantity']:.0f}")
            with subcol3:
                st.metric("Products", pending_summary['unique_products'])
        else:
            st.info("No pending counts")
    
    with col2:
        st.markdown("### 👥 Team Total")
        if team_summary.get('total_records', 0) > 0:
            subcol1, subcol2, subcol3 = st.columns(3)
            with subcol1:
                st.metric("Records", team_summary['total_records'])
            with subcol2:
                st.metric("Quantity", f"{team_summary['total_quantity']:.0f}")
            with subcol3:
                st.metric("Users", team_summary['total_users'])
        else:
            st.info("No team counts yet")
    
    with col3:
        st.markdown("### 🎯 Actions")
        if check_permission('create_transactions') and pending_summary['total_items'] > 0 and not st.session_state.get('view_only_mode', False):
            if st.button(
                f"💾 Save All ({pending_summary['total_items']})",
                use_container_width=True,
                type="primary",
                on_click=save_all_counts_callback
            ):
                pass
        
        if team_summary.get('total_records', 0) > 0 and not st.session_state.get('view_only_mode', False):
            if st.button("👥 View Team Counts", use_container_width=True):
                st.session_state.show_team_view = not st.session_state.show_team_view

@st.fragment(run_every=None)
def product_selector_fragment():
    """Isolated product selector to prevent reruns"""
    warehouse_id = st.session_state.get('selected_warehouse_id')
    if not warehouse_id:
        # Try to get from selected transaction if in view mode
        if st.session_state.get('selected_view_transaction'):
            tx_info = audit_service.get_transaction_info(st.session_state.selected_view_transaction)
            if tx_info:
                session_info = audit_service.get_session_info(tx_info['session_id'])
                warehouse_id = session_info.get('warehouse_id')
        
        if not warehouse_id:
            st.warning("⚠️ Please select a transaction first")
            return
    
    # Load products based on mode - use session state to prevent repeated loading
    session_id = st.session_state.get('selected_session_id') or st.session_state.get('selected_view_session')
    cache_key = f"products_{warehouse_id}_{st.session_state.count_mode}_{session_id}"
    if cache_key not in st.session_state:
        with st.spinner("Loading products..."):
            products = get_products_for_mode(warehouse_id, st.session_state.count_mode)
            st.session_state[cache_key] = products
    else:
        products = st.session_state[cache_key]
    
    # Add team count status only if not already added
    if session_id and products and len(products) > 0 and not products[0].get('team_count_checked', False):
        with st.spinner("Checking team counts..."):
            # Get all product IDs
            product_ids = [p['product_id'] for p in products if p.get('product_id')]
            
            # Batch check counts
            if product_ids:
                counts_dict = check_product_counted_batch(session_id, product_ids, st.session_state.count_mode)
                
                # Apply counts to products
                for product in products:
                    if product.get('product_id'):
                        product['team_count'] = counts_dict.get(product['product_id'], 
                            {'counted': False, 'users_count': 0, 'total_quantity': 0, 'count_records': 0})
                    else:
                        product['team_count'] = {'counted': False, 'users_count': 0, 'total_quantity': 0, 'count_records': 0}
                    product['team_count_checked'] = True
    
    # Product search and filter
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            "Search Product",
            placeholder="Type PT code or product name...",
            key="product_search"
        )
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            # Clear product cache in session state
            for key in list(st.session_state.keys()):
                if key.startswith('products_'):
                    del st.session_state[key]
            # Clear function caches
            st.cache_data.clear()
            st.rerun()
    
    # Filter products
    if search_term:
        search_lower = search_term.lower()
        filtered_products = [
            p for p in products
            if search_lower in p.get('pt_code', '').lower() or
               search_lower in p.get('product_name', '').lower()
        ]
    else:
        filtered_products = products
    
    # Product selection
    if st.session_state.count_mode == 'physical':
        product_options = ["-- Not in ERP / New Product --"] + [
            format_product_display(p) for p in filtered_products[:100]
        ]
        product_map = {format_product_display(p): p for p in filtered_products[:100]}
        product_map["-- Not in ERP / New Product --"] = None
    else:
        product_options = ["-- Select Product --"] + [
            format_product_display(p) for p in filtered_products[:100]
        ]
        product_map = {format_product_display(p): p for p in filtered_products[:100]}
        product_map["-- Select Product --"] = None
    
    selected_display = st.selectbox(
        "Select Product",
        options=product_options,
        key="product_select_widget"
    )
    
    st.session_state.selected_product = product_map.get(selected_display)
    
    # Show product info if selected
    if st.session_state.selected_product:
        product = st.session_state.selected_product
        tc = product.get('team_count', {})
        
        if tc.get('counted'):
            st.warning(f"⚠️ Already counted by {tc['users_count']} users: {tc['total_quantity']:.0f} units")
        
        # Load batches for inventory mode
        if st.session_state.count_mode == 'inventory':
            batches = audit_service.get_product_batch_details(
                warehouse_id,
                product['product_id']
            )
            
            if batches:
                batch_options = ["-- Manual Entry --"] + [
                    f"{b['batch_no']} (Qty: {b['quantity']:.0f}, Loc: {b.get('location', 'N/A')})"
                    for b in batches
                ]
                
                selected_batch = st.selectbox(
                    "Select Batch",
                    options=batch_options,
                    key="batch_select_widget"
                )
                
                if selected_batch != "-- Manual Entry --":
                    batch_no = selected_batch.split(" (")[0]
                    st.session_state.selected_batch = next(
                        (b for b in batches if b['batch_no'] == batch_no), None
                    )

@st.fragment(run_every=None)
def counting_form_fragment():
    """Isolated counting form"""
    if not st.session_state.get('selected_product') and st.session_state.count_mode == 'inventory':
        st.info("👆 Please select a product above")
        return
    
    st.markdown("### ✏️ Count Entry")
    
    # Initialize form fields from selected product/batch
    product = st.session_state.get('selected_product', {})
    batch = st.session_state.get('selected_batch', {})
    
    with st.form(f"count_form_{st.session_state.form_key}", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            # Product name (manual for physical mode without selection)
            if not product and st.session_state.count_mode == 'physical':
                st.markdown("**Product Name** <span style='color:red;'>*</span>", unsafe_allow_html=True)
                product_name = st.text_input(
                    "Product Name",
                    key="manual_product_name",
                    label_visibility="collapsed"
                )
                brand = st.text_input("Brand", key="manual_brand")
            else:
                product_name = product.get('product_name', '')
                brand = product.get('brand', '')
                st.text_input("Product", value=f"{product.get('pt_code', '')} - {product_name}", disabled=True)
            
            # Batch number
            batch_no = st.text_input(
                "Batch Number",
                value=batch.get('batch_no', '') if batch else '',
                key="batch_input"
            )
            
            # Quantity
            st.markdown("**Quantity** <span style='color:red;'>*</span>", unsafe_allow_html=True)
            quantity = st.number_input(
                "Quantity",
                min_value=0.0,
                value=0.0,
                step=1.0,
                key="qty_input",
                label_visibility="collapsed"
            )
        
        with col2:
            # Location
            default_loc = st.session_state.default_location
            location_str = batch.get('location', '') if batch else f"{default_loc['zone']}-{default_loc['rack']}-{default_loc['bin']}".strip('-')
            
            st.markdown("**Location (Zone-Rack-Bin)** <span style='color:red;'>*</span>", unsafe_allow_html=True)
            location = st.text_input(
                "Location",
                value=location_str,
                key="location_input",
                placeholder="e.g., A1-R01-B01",
                label_visibility="collapsed"
            )
            
            # Expiry date
            expiry = st.date_input(
                "Expiry Date",
                value=pd.to_datetime(batch.get('expired_date')).date() if batch and batch.get('expired_date') else None,
                key="expiry_input",
                min_value=date(2020, 1, 1),
                max_value=date(2030, 12, 31)
            )
            
            # Notes
            notes = st.text_area(
                "Notes",
                key="notes_input",
                height=69
            )
        
        # Submit buttons
        col_add, col_clear = st.columns([3, 1])
        
        with col_add:
            submitted = st.form_submit_button(
                f"➕ Add Count ({len(st.session_state.pending_counts)}/{MAX_PENDING_COUNTS})",
                use_container_width=True,
                type="primary",
                disabled=len(st.session_state.pending_counts) >= MAX_PENDING_COUNTS
            )
        
        with col_clear:
            if st.form_submit_button("🔄 Clear Form", use_container_width=True):
                st.session_state.form_key += 1
                st.rerun()
        
        if submitted:
            # Manual product handling for physical mode
            if not product and st.session_state.count_mode == 'physical':
                if product_name:
                    st.session_state.selected_product = {
                        'product_id': None,
                        'product_name': product_name,
                        'brand': brand,
                        'pt_code': 'N/A'
                    }
            add_count_callback()
            st.rerun()

@st.fragment(run_every=None)
def pending_counts_fragment():
    """Display and manage pending counts"""
    if not st.session_state.pending_counts:
        return
    
    st.markdown(f"### 📋 Pending Counts ({len(st.session_state.pending_counts)})")
    
    # Group by product
    grouped = {}
    for i, count in enumerate(st.session_state.pending_counts):
        key = count.get('product_id') or count.get('product_name', 'Unknown')
        if key not in grouped:
            grouped[key] = []
        grouped[key].append((i, count))
    
    # Display grouped counts
    for product_key, items in grouped.items():
        with st.expander(f"📦 {items[0][1]['product_name']} ({len(items)} counts)", expanded=True):
            for idx, count in items:
                # Check if in edit mode
                if st.session_state.edit_mode['active'] and st.session_state.edit_mode['index'] == idx:
                    render_edit_form(idx, count)
                else:
                    render_count_item(idx, count)

def render_count_item(idx: int, count: Dict):
    """Render single count item"""
    col1, col2, col3, col4, col5 = st.columns([2.5, 1.5, 1, 1.5, 1.5])
    
    with col1:
        location = f"{count['zone_name']}-{count['rack_name']}-{count['bin_name']}"
        st.write(f"📍 **{location}**")
        if count.get('batch_no'):
            st.caption(f"Batch: {count['batch_no']}")
    
    with col2:
        st.metric("Quantity", f"{count['actual_quantity']:.0f}")
        if count.get('system_quantity', 0) > 0:
            variance = count['actual_quantity'] - count['system_quantity']
            st.caption(f"Var: {variance:+.0f}")
    
    with col3:
        if count.get('expired_date'):
            exp_date = pd.to_datetime(count['expired_date']).date()
            days_to_exp = (exp_date - date.today()).days
            
            if days_to_exp < 0:
                st.caption("🔴 Expired")
            elif days_to_exp < 90:
                st.caption(f"🟡 {days_to_exp}d")
            else:
                st.caption("🟢 OK")
    
    with col4:
        st.caption(f"⏰ {count['added_time'].strftime('%H:%M')}")
        if count.get('actual_notes'):
            st.caption(f"📝 {count['actual_notes'][:20]}...")
    
    with col5:
        col_edit, col_del = st.columns(2)
        with col_edit:
            if st.button("✏️", key=f"edit_btn_{idx}", help="Edit"):
                st.session_state.edit_mode = {'active': True, 'index': idx}
                st.rerun()
        
        with col_del:
            if st.button("🗑️", key=f"del_btn_{idx}", help="Delete"):
                st.session_state.pending_counts.pop(idx)
                st.session_state.last_action = "🗑️ Count removed"
                st.session_state.last_action_time = datetime.now()
                st.rerun()

def render_edit_form(idx: int, count: Dict):
    """Render edit form for count item"""
    st.markdown("#### ✏️ Edit Count")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.text_input("Product", value=count['product_name'], disabled=True)
        
        batch_no = st.text_input(
            "Batch Number",
            value=count.get('batch_no', ''),
            key=f"edit_batch_{idx}"
        )
        
        st.markdown("**Quantity** <span style='color:red;'>*</span>", unsafe_allow_html=True)
        quantity = st.number_input(
            "Quantity",
            value=float(count['actual_quantity']),
            min_value=0.01,
            step=1.0,
            key=f"edit_qty_{idx}",
            label_visibility="collapsed"
        )
    
    with col2:
        col_z, col_r, col_b = st.columns(3)
        with col_z:
            st.markdown("**Zone** <span style='color:red;'>*</span>", unsafe_allow_html=True)
            zone = st.text_input(
                "Zone",
                value=count['zone_name'],
                key=f"edit_zone_{idx}",
                label_visibility="collapsed"
            )
        with col_r:
            rack = st.text_input(
                "Rack",
                value=count['rack_name'],
                key=f"edit_rack_{idx}"
            )
        with col_b:
            bin_name = st.text_input(
                "Bin",
                value=count['bin_name'],
                key=f"edit_bin_{idx}"
            )
        
        expiry = st.date_input(
            "Expiry Date",
            value=pd.to_datetime(count['expired_date']).date() if count.get('expired_date') else None,
            key=f"edit_expiry_{idx}"
        )
        
        notes = st.text_area(
            "Notes",
            value=count.get('actual_notes', ''),
            key=f"edit_notes_{idx}",
            height=50
        )
    
    # Action buttons
    col_save, col_cancel = st.columns(2)
    
    with col_save:
        if st.button("💾 Save Changes", type="primary", key=f"save_edit_{idx}"):
            # Validate
            if not zone or quantity <= 0:
                st.error("Zone and quantity are required!")
            else:
                update_count_callback(idx)
                st.rerun()
    
    with col_cancel:
        if st.button("❌ Cancel", key=f"cancel_edit_{idx}"):
            st.session_state.edit_mode = {'active': False, 'index': None}
            st.rerun()

def handle_batch_save():
    """Handle batch save with progress"""
    if st.session_state.batch_save_in_progress and st.session_state.pending_counts:
        progress_container = st.container()
        
        with progress_container:
            with st.spinner(f"Saving {st.session_state.pending_save_count} counts..."):
                progress_bar = st.progress(0)
                
                # Prepare data
                count_list = []
                for count in st.session_state.pending_counts:
                    # Format for database
                    if st.session_state.count_mode == 'physical':
                        if count.get('product_id'):
                            notes = f"PHYSICAL COUNT - IN ERP: {count.get('actual_notes', '')}"
                        else:
                            notes = f"PHYSICAL COUNT - NOT IN ERP: {count.get('product_name', '')} - {count.get('actual_notes', '')}"
                    else:
                        notes = count.get('actual_notes', '')
                    
                    count_data = {
                        'transaction_id': count['transaction_id'],
                        'product_id': count.get('product_id'),
                        'batch_no': count.get('batch_no', ''),
                        'expired_date': count.get('expired_date'),
                        'zone_name': count['zone_name'],
                        'rack_name': count['rack_name'],
                        'bin_name': count['bin_name'],
                        'system_quantity': count.get('system_quantity', 0),
                        'system_value_usd': count.get('system_value_usd', 0),
                        'actual_quantity': count['actual_quantity'],
                        'actual_notes': notes,
                        'is_new_item': count.get('is_new_item', False),
                        'created_by_user_id': count['created_by_user_id']
                    }
                    count_list.append(count_data)
                
                # Update progress
                progress_bar.progress(30)
                
                # Save to database
                saved, errors = audit_service.save_batch_counts(count_list)
                
                progress_bar.progress(100)
                
                # Show results
                if errors and saved == 0:
                    st.error(f"❌ Failed to save counts")
                    for error in errors[:3]:
                        st.caption(f"• {error}")
                elif errors and saved > 0:
                    st.warning(f"⚠️ Saved {saved} counts with {len(errors)} errors")
                else:
                    st.success(f"✅ Successfully saved {saved} counts!")
                    st.balloons()
                    
                    # Clear pending counts
                    st.session_state.pending_counts = []
                    st.session_state.form_key += 1
                    
                    # Clear caches
                    get_team_counts_summary.clear()
                    check_product_counted.clear()
                    check_product_counted_batch.clear()
                    get_all_session_counts.clear()
                    
                    # Clear product cache in session state to force reload
                    for key in list(st.session_state.keys()):
                        if key.startswith('products_'):
                            del st.session_state[key]
                
                # Reset save state
                st.session_state.batch_save_in_progress = False
                st.session_state.pending_save_count = 0
                
                time.sleep(1.5)
                st.rerun()

def show_view_only_interface():
    """Interface for managers with view_all permission"""
    # Info banner
    st.info("👀 **View Only Mode** - You can view all count data without creating a transaction")
    
    col_title, col_exit = st.columns([4, 1])
    with col_title:
        st.markdown("### 📊 View Count Data")
    with col_exit:
        if st.button("❌ Exit View Mode", use_container_width=True):
            st.session_state.view_only_mode = False
            st.rerun()
    
    # Session selector
    sessions = audit_service.get_sessions_by_status('in_progress')
    
    if not sessions:
        st.warning("⚠️ No active sessions available")
        return
    
    session_options = {f"{s['session_name']} ({s['session_code']})": s['id'] for s in sessions}
    selected_session_name = st.selectbox("Select Session to View", session_options.keys())
    st.session_state.selected_view_session = session_options[selected_session_name]
    
    # Get all counts for session
    counts_df = get_all_session_counts(st.session_state.selected_view_session)
    
    if not counts_df.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Records", len(counts_df))
        with col2:
            st.metric("Total Quantity", f"{counts_df['actual_quantity'].sum():.0f}")
        with col3:
            st.metric("Unique Products", counts_df['product_id'].nunique())
        with col4:
            st.metric("Active Users", counts_df['created_by_user_id'].nunique())
        
        st.markdown("---")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            users = ['All'] + sorted(counts_df['username'].unique().tolist())
            selected_user = st.selectbox("Filter by User", users)
        
        with col2:
            transactions = ['All'] + sorted(counts_df['transaction_code'].unique().tolist())
            selected_tx = st.selectbox("Filter by Transaction", transactions)
        
        with col3:
            count_types = ['All', 'Inventory Counts', 'Physical Counts']
            selected_type = st.selectbox("Count Type", count_types)
        
        # Apply filters
        filtered_df = counts_df.copy()
        
        if selected_user != 'All':
            filtered_df = filtered_df[filtered_df['username'] == selected_user]
        
        if selected_tx != 'All':
            filtered_df = filtered_df[filtered_df['transaction_code'] == selected_tx]
        
        if selected_type == 'Inventory Counts':
            filtered_df = filtered_df[filtered_df['is_new_item'] == 0]
        elif selected_type == 'Physical Counts':
            filtered_df = filtered_df[filtered_df['is_new_item'] == 1]
        
        # Display data
        st.markdown(f"### 📊 Count Details ({len(filtered_df)} records)")
        
        # Format for display
        display_df = filtered_df[[
            'counted_date', 'transaction_code', 'username', 'pt_code', 
            'product_name', 'batch_no', 'actual_quantity', 'zone_name',
            'rack_name', 'bin_name', 'actual_notes'
        ]].copy()
        
        display_df.columns = [
            'Date/Time', 'Transaction', 'User', 'PT Code', 
            'Product', 'Batch', 'Quantity', 'Zone',
            'Rack', 'Bin', 'Notes'
        ]
        
        # Show as dataframe
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Date/Time": st.column_config.DatetimeColumn(
                    "Date/Time",
                    format="DD/MM/YYYY HH:mm"
                ),
                "Quantity": st.column_config.NumberColumn(
                    "Quantity",
                    format="%.0f"
                )
            }
        )
        
        # Export option
        csv = display_df.to_csv(index=False)
        st.download_button(
            "📥 Download CSV",
            csv,
            f"count_data_{st.session_state.selected_view_session}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv"
        )
    else:
        st.info("No count data available for this session")

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry"""
    init_session_state()
    
    if not auth.check_session():
        show_login()
    else:
        show_app()

def show_login():
    """Show login page"""
    st.title("🔐 Login - Unified Counting System")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            
            if st.form_submit_button("Login", use_container_width=True):
                if username and password:
                    success, result = auth.authenticate(username, password)
                    if success:
                        auth.login(result)
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials")

def show_app():
    """Show main application"""
    # Header with mode selector
    render_header()
    
    # Check if user has permissions
    if not check_permission('create_transactions') and not check_permission('view_all'):
        st.warning("⚠️ You don't have permission to access this page")
        if st.button("Go to Home"):
            st.switch_page("main.py")
        return
    
    # View-only mode for managers without transaction
    if check_permission('view_all') and not check_permission('create_transactions'):
        st.session_state.view_only_mode = True
        show_view_only_interface()
        return
    
    # For users with create_transactions permission
    if check_permission('create_transactions'):
        # Check if navigated from audit_management with selected transaction
        if 'selected_tx_id' in st.session_state and st.session_state.selected_tx_id:
            # Get transaction info to set session
            try:
                tx_info = audit_service.get_transaction_info(st.session_state.selected_tx_id)
                if tx_info:
                    st.session_state.selected_session_id = tx_info['session_id']
                    st.session_state.selected_warehouse_id = tx_info['warehouse_id']
            except Exception as e:
                logger.error(f"Error loading transaction info: {e}")
        
        # Check for session
        if 'selected_session_id' not in st.session_state:
            # Get active sessions
            sessions = audit_service.get_sessions_by_status('in_progress')
            if sessions:
                st.session_state.selected_session_id = sessions[0]['id']
        
        if 'selected_session_id' in st.session_state:
            # Get user transactions
            transactions = audit_service.get_user_transactions(
                st.session_state.selected_session_id,
                st.session_state.user_id,
                status='draft'
            )
            
            # For managers with view_all, also show option to view without transaction
            if check_permission('view_all') and not transactions:
                col1, col2 = st.columns(2)
                with col1:
                    st.warning("⚠️ No draft transactions available")
                    st.info("Please create a transaction first")
                with col2:
                    if st.button("👀 View All Session Data", use_container_width=True):
                        st.session_state.view_only_mode = True
                        st.rerun()
                
                # Check if already in view mode
                if st.session_state.get('view_only_mode', False):
                    show_view_only_interface()
                return
            
            if transactions:
                # Transaction selector
                tx_options = {
                    f"{tx['transaction_name']} ({tx['transaction_code']})": tx
                    for tx in transactions
                }
                
                # Find default selection if navigated from audit_management
                default_index = 0
                if 'selected_tx_id' in st.session_state and st.session_state.selected_tx_id:
                    for idx, (key, tx) in enumerate(tx_options.items()):
                        if tx['id'] == st.session_state.selected_tx_id:
                            default_index = idx
                            break
                
                col1, col2 = st.columns([4, 1])
                with col1:
                    selected_tx_key = st.selectbox(
                        "Select Transaction",
                        options=list(tx_options.keys()),
                        index=default_index
                    )
                
                with col2:
                    # View all data button for managers  
                    if st.button("👀 View All", use_container_width=True, help="View all session data"):
                        st.session_state.view_only_mode = True
                        st.rerun()
                
                selected_tx = tx_options[selected_tx_key]
                st.session_state.selected_tx_id = selected_tx['id']
                st.session_state.selected_warehouse_id = selected_tx['warehouse_id']
                
                # Show action feedback
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
                
                # Summary bar
                render_summary_bar()
                
                # Team view if toggled
                if st.session_state.show_team_view:
                    st.markdown("---")
                    st.markdown("### 👥 Team Count Details")
                    # Show team counts
                    counts_df = get_all_session_counts(st.session_state.selected_session_id)
                    if not counts_df.empty:
                        # Filter by current count mode
                        if st.session_state.count_mode == 'inventory':
                            team_df = counts_df[counts_df['is_new_item'] == 0]
                        else:
                            team_df = counts_df[counts_df['is_new_item'] == 1]
                        
                        if not team_df.empty:
                            st.dataframe(
                                team_df[['username', 'pt_code', 'product_name', 'batch_no', 
                                       'actual_quantity', 'zone_name', 'counted_date']],
                                use_container_width=True,
                                hide_index=True
                            )
                        else:
                            st.info("No team counts for current mode")
                    st.markdown("---")
                
                # Main content columns
                col_left, col_right = st.columns([5, 7])
                
                with col_left:
                    # Product selector
                    st.markdown("### 📦 Product Selection")
                    product_selector_fragment()
                
                with col_right:
                    # Counting form
                    counting_form_fragment()
                
                # Pending counts section
                st.markdown("---")
                pending_counts_fragment()
                
                # Handle batch save
                handle_batch_save()
                
            else:
                st.warning("⚠️ No draft transactions available")
                st.info("Please create a transaction first")
                if st.button("Go to Audit Management"):
                    st.switch_page("pages/audit_management.py")
        else:
            st.warning("⚠️ No active session found")

# Clear button in sidebar
with st.sidebar:
    st.markdown("### 🛠️ Tools")
    
    if check_permission('create_transactions'):
        if st.button("🗑️ Clear All Pending", use_container_width=True):
            if st.checkbox("Confirm clear all"):
                st.session_state.pending_counts = []
                st.success("Cleared!")
                st.rerun()
    
    if st.button("🔄 Clear Cache", use_container_width=True):
        # Clear all caches
        st.cache_data.clear()
        # Clear product cache in session state
        for key in list(st.session_state.keys()):
            if key.startswith('products_'):
                del st.session_state[key]
        st.success("Cache cleared!")
    
    # Toggle view mode for managers
    if check_permission('view_all'):
        view_mode = st.checkbox(
            "View Only Mode", 
            value=st.session_state.get('view_only_mode', False),
            help="Switch to view-only mode to see all session data"
        )
        if view_mode != st.session_state.get('view_only_mode', False):
            st.session_state.view_only_mode = view_mode
            st.rerun()

if __name__ == "__main__":
    main()
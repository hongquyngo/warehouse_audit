# main.py - Warehouse Audit System with Optimized Counting Tab
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import logging
import time
from typing import Dict, List, Optional, Tuple
import json
from functools import partial

# Import existing utilities
from utils.auth import AuthManager
from utils.config import config
from utils.db import get_db_engine

# Import our services
from audit_service import AuditService, AuditException, SessionNotFoundException, InvalidTransactionStateException, CountValidationException
from audit_queries import AuditQueries

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Warehouse Audit System",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize services
auth = AuthManager()
audit_service = AuditService()
queries = AuditQueries()

# ============== SESSION STATE INITIALIZATION ==============

def init_session_state():
    """Initialize all session state variables for optimized performance"""
    defaults = {
        # Counting specific
        'temp_counts': [],
        'count_counter': 0,
        'selected_product_data': None,
        'selected_batch_data': None,
        'form_values': {},
        'show_count_history': {},
        'last_action': None,
        'last_action_time': None,
        
        # Form field values
        'batch_no_input': '',
        'actual_qty_input': 0.0,
        'location_input': '',
        'notes_input': '',
        'expiry_date_input': None,
        
        # Cache management
        'cached_products': None,
        'cached_batches': {},
        'cached_transactions': None,
        'cached_count_summaries': {},
        'cache_timestamp': {},
        
        # UI state
        'counting_mode': 'fast',  # 'fast' or 'detailed'
        'show_batch_details': True,
        'auto_save_enabled': False,
        'auto_save_threshold': 10,
        
        # Performance tracking
        'action_count': 0,
        'last_refresh_time': datetime.now()
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

# Call init at startup
init_session_state()

# ============== CACHE FUNCTIONS WITH TTL ==============

@st.cache_data(ttl=3600)  # 1 hour cache
def cached_get_warehouses():
    """Cached wrapper for get_warehouses"""
    return audit_service.get_warehouses()

@st.cache_data(ttl=1800)  # 30 minutes cache
def cached_get_warehouse_brands(warehouse_id: int):
    """Cached wrapper for get_warehouse_brands"""
    return audit_service.get_warehouse_brands(warehouse_id)

@st.cache_data(ttl=900)  # 15 minutes cache
def cached_get_warehouse_products(warehouse_id: int):
    """Cached wrapper for get_warehouse_products"""
    return audit_service.get_warehouse_products(warehouse_id)

@st.cache_data(ttl=600)  # 10 minutes cache
def cached_search_products_with_filters(warehouse_id: int, search_term: str = "", brand_filter: str = ""):
    """Cached wrapper for search_products_with_filters"""
    return audit_service.search_products_with_filters(warehouse_id, search_term, brand_filter)

# ============== OPTIMIZED TEMP COUNT MANAGEMENT ==============

def add_temp_count_optimized(count_data: Dict):
    """Add count to temporary storage without rerun"""
    st.session_state.temp_counts.append(count_data)
    st.session_state.count_counter += 1
    st.session_state.last_action = f"✅ Added count #{len(st.session_state.temp_counts)}"
    st.session_state.last_action_time = datetime.now()
    
    # Auto-save check
    if st.session_state.auto_save_enabled and len(st.session_state.temp_counts) >= st.session_state.auto_save_threshold:
        return True  # Signal to save
    return False

def remove_temp_count(index: int):
    """Remove count from temporary storage"""
    if 0 <= index < len(st.session_state.temp_counts):
        removed = st.session_state.temp_counts.pop(index)
        st.session_state.last_action = f"🗑️ Removed count for {removed['product_name']}"
        st.session_state.last_action_time = datetime.now()

def clear_temp_counts():
    """Clear temporary counts after saving"""
    count = len(st.session_state.temp_counts)
    st.session_state.temp_counts = []
    st.session_state.count_counter = 0
    st.session_state.last_action = f"🗑️ Cleared {count} counts"
    st.session_state.last_action_time = datetime.now()

def get_temp_counts_summary() -> Dict:
    """Get summary of temporary counts"""
    if not st.session_state.temp_counts:
        return {'total': 0, 'products': 0, 'quantity': 0}
    
    product_ids = set()
    total_quantity = 0
    
    for count in st.session_state.temp_counts:
        if count.get('product_id'):
            product_ids.add(count['product_id'])
        total_quantity += count.get('actual_quantity', 0)
    
    return {
        'total': len(st.session_state.temp_counts),
        'products': len(product_ids),
        'quantity': total_quantity
    }

# ============== CALLBACKS FOR NO-RERUN OPERATIONS ==============

def on_product_selected():
    """Callback when product is selected"""
    selected_key = st.session_state.get('product_selector_fast', '')
    
    if selected_key and selected_key != "-- Select Product --":
        # Parse product data from the key
        product_data = st.session_state.get('product_lookup', {}).get(selected_key)
        if product_data:
            st.session_state.selected_product_data = product_data
            
            # Clear batch selection when product changes
            st.session_state.selected_batch_data = None
            
            # Trigger batch loading
            warehouse_id = st.session_state.get('current_warehouse_id')
            if warehouse_id:
                load_batches_for_product(product_data['product_id'], warehouse_id)
    else:
        st.session_state.selected_product_data = None
        st.session_state.selected_batch_data = None

def on_batch_selected():
    """Callback when batch is selected"""
    selected_batch = st.session_state.get('batch_selector_fast', '')
    
    if selected_batch and selected_batch != "-- Manual Entry --":
        batch_no = selected_batch.split(" (")[0]
        batches = st.session_state.cached_batches.get(st.session_state.selected_product_data['product_id'], [])
        
        for batch in batches:
            if batch['batch_no'] == batch_no:
                st.session_state.selected_batch_data = batch
                # Auto-populate batch number
                st.session_state.batch_no_input = batch['batch_no']
                # Auto-populate location if available
                if batch.get('location'):
                    st.session_state.location_input = batch['location']
                # Auto-populate expiry date if available
                if batch.get('expired_date'):
                    try:
                        st.session_state.expiry_date_input = pd.to_datetime(batch['expired_date']).date()
                    except:
                        pass
                break
    else:
        # Manual entry mode
        st.session_state.selected_batch_data = None
        # Clear auto-populated fields
        if 'batch_no_input' in st.session_state:
            st.session_state.batch_no_input = ""
        if 'location_input' in st.session_state:
            st.session_state.location_input = ""

def load_batches_for_product(product_id: int, warehouse_id: int):
    """Load batch details for selected product"""
    cache_key = f"{product_id}_{warehouse_id}"
    
    # Check cache age
    cache_time = st.session_state.cache_timestamp.get(f"batches_{cache_key}")
    if cache_time and (datetime.now() - cache_time).seconds < 300:  # 5 min cache
        return  # Use existing cache
    
    # Load fresh data
    try:
        batches = audit_service.get_product_batch_details(warehouse_id, product_id)
        st.session_state.cached_batches[product_id] = batches
        st.session_state.cache_timestamp[f"batches_{cache_key}"] = datetime.now()
    except Exception as e:
        logger.error(f"Error loading batches: {e}")
        st.session_state.cached_batches[product_id] = []

def add_count_callback():
    """Callback for adding count without rerun"""
    # Get form values
    actual_qty = st.session_state.get('actual_qty_input', 0)
    location = st.session_state.get('location_input', '')
    notes = st.session_state.get('notes_input', '')
    batch_no = st.session_state.get('batch_no_input', '')
    expiry_date = st.session_state.get('expiry_date_input', None)
    
    if actual_qty > 0 and st.session_state.selected_product_data:
        # Validate batch number
        if not batch_no:
            st.session_state.last_action = "⚠️ Please enter batch number"
            st.session_state.last_action_time = datetime.now()
            return
        
        # Parse location
        zone, rack, bin_loc = parse_location(location)
        
        count_data = {
            'transaction_id': st.session_state.selected_tx_id,
            'product_id': st.session_state.selected_product_data['product_id'],
            'product_name': st.session_state.selected_product_data['product_name'],
            'pt_code': st.session_state.selected_product_data.get('pt_code', 'N/A'),
            'batch_no': batch_no,
            'expired_date': expiry_date,
            'zone_name': zone,
            'rack_name': rack,
            'bin_name': bin_loc,
            'location_notes': '',
            'system_quantity': st.session_state.selected_batch_data['quantity'] if st.session_state.selected_batch_data else 0,
            'system_value_usd': st.session_state.selected_batch_data.get('value_usd', 0) if st.session_state.selected_batch_data else 0,
            'actual_quantity': actual_qty,
            'actual_notes': notes,
            'created_by_user_id': st.session_state.user_id
        }
        
        should_save = add_temp_count_optimized(count_data)
        
        # Clear form inputs
        st.session_state.actual_qty_input = 0.0
        st.session_state.notes_input = ""
        
        # Clear batch selection if manual entry
        if not st.session_state.selected_batch_data:
            st.session_state.batch_no_input = ""
        
        # Auto-save if threshold reached
        if should_save:
            save_all_counts_callback()
    else:
        if actual_qty <= 0:
            st.session_state.last_action = "⚠️ Please enter quantity > 0"
        else:
            st.session_state.last_action = "⚠️ Please select a product first"
        st.session_state.last_action_time = datetime.now()

def save_all_counts_callback():
    """Save all temp counts without rerun"""
    if not st.session_state.temp_counts:
        st.session_state.last_action = "⚠️ No counts to save"
        return
    
    try:
        # Validate all counts have batch numbers
        invalid_counts = [i for i, c in enumerate(st.session_state.temp_counts) if not c.get('batch_no')]
        if invalid_counts:
            st.session_state.last_action = f"⚠️ Missing batch numbers in {len(invalid_counts)} counts"
            st.session_state.last_action_time = datetime.now()
            return
        
        # Save batch
        saved, errors = audit_service.save_batch_counts(st.session_state.temp_counts)
        
        if errors:
            st.session_state.last_action = f"⚠️ Saved {saved} counts with {len(errors)} errors"
            # Keep failed counts
            st.session_state.temp_counts = [c for i, c in enumerate(st.session_state.temp_counts) if f"Row {i+1}" in str(errors)]
        else:
            st.session_state.last_action = f"✅ Successfully saved {saved} counts!"
            clear_temp_counts()
            
            # Clear cache to refresh counts
            cache_key = f"count_summary_{st.session_state.selected_tx_id}"
            if cache_key in st.session_state.cached_count_summaries:
                del st.session_state.cached_count_summaries[cache_key]
        
        st.session_state.last_action_time = datetime.now()
        
    except Exception as e:
        st.session_state.last_action = f"❌ Error: {str(e)}"
        st.session_state.last_action_time = datetime.now()

def parse_location(location: str) -> Tuple[str, str, str]:
    """Parse location string into zone, rack, bin"""
    if not location:
        return "", "", ""
    
    if '-' in location:
        parts = location.split('-')
        zone = parts[0].strip() if len(parts) > 0 else ""
        rack = parts[1].strip() if len(parts) > 1 else ""
        bin_loc = parts[2].strip() if len(parts) > 2 else ""
        return zone, rack, bin_loc
    else:
        return location.strip(), "", ""

# ============== ROLE PERMISSIONS ==============

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
    if 'user_role' not in st.session_state:
        return False
    
    user_role = st.session_state.user_role
    return action in AUDIT_ROLES.get(user_role, [])

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry point"""
    try:
        # Check authentication
        if not auth.check_session():
            show_login_page()
        else:
            show_main_app()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        logger.error(f"Main app error: {e}")

def show_login_page():
    """Display login page"""
    st.title("🏭 Warehouse Audit System")
    st.markdown("### Please login to access the system")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            submit = st.form_submit_button("🔐 Login", use_container_width=True)
            
            if submit:
                if username and password:
                    success, result = auth.authenticate(username, password)
                    
                    if success:
                        auth.login(result)
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error(f"❌ {result.get('error', 'Login failed')}")
                else:
                    st.warning("⚠️ Please enter both username and password")

def show_main_app():
    """Display main application interface"""
    # Sidebar with user info
    with st.sidebar:
        st.markdown("### 👤 User Info")
        st.write(f"**Name:** {auth.get_user_display_name()}")
        st.write(f"**Role:** {st.session_state.user_role}")
        st.write(f"**Login:** {st.session_state.login_time.strftime('%H:%M')}")
        
        # Performance stats
        if st.session_state.action_count > 0:
            st.markdown("---")
            st.markdown("**📊 Session Stats:**")
            st.caption(f"Actions: {st.session_state.action_count}")
            uptime = datetime.now() - st.session_state.last_refresh_time
            st.caption(f"Uptime: {str(uptime).split('.')[0]}")
        
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    # Main content based on role
    user_role = st.session_state.user_role
    
    if not AUDIT_ROLES.get(user_role, []):
        show_no_access_interface()
    elif check_permission('manage_sessions'):
        show_admin_interface()
    elif check_permission('create_transactions'):
        show_user_interface()
    else:
        show_viewer_interface()

def show_no_access_interface():
    """Interface for users without audit permissions"""
    st.title("🚫 Access Restricted")
    st.warning("⚠️ You don't have permission to access the Audit System")

def show_admin_interface():
    """Admin interface for session management"""
    st.title("🔧 Admin Dashboard")
    
    user_role = st.session_state.user_role
    is_super_admin = user_role in ['admin']
    
    if is_super_admin:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Session Management", "📊 Dashboard", "📈 Reports", "📦 Warehouse Audit", "👥 User Management"])
        
        with tab5:
            st.info("🚧 User management features coming soon...")
    else:
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Session Management", "📊 Dashboard", "📈 Reports", "📦 Warehouse Audit"])
    
    with tab1:
        st.info("Session management page - implementation continues...")
    
    with tab2:
        st.info("Dashboard page - implementation continues...")
    
    with tab3:
        st.info("Reports page - implementation continues...")
    
    with tab4:
        show_warehouse_audit_content_optimized()

def show_user_interface():
    """User interface for transactions and counting"""
    st.title("📦 Warehouse Audit")
    show_warehouse_audit_content_optimized()

def show_viewer_interface():
    """Viewer interface - read only"""
    st.title("👀 Audit Viewer")
    st.info("Viewer interface - implementation continues...")

# ============== OPTIMIZED WAREHOUSE AUDIT CONTENT ==============

def show_warehouse_audit_content_optimized():
    """Optimized warehouse audit content with minimal reruns"""
    subtab1, subtab2 = st.tabs(["📝 My Transactions", "🔢 Counting"])
    
    with subtab1:
        my_transactions_page_optimized()
    
    with subtab2:
        counting_page_optimized()

def my_transactions_page_optimized():
    """Optimized transactions management page"""
    st.subheader("📝 My Audit Transactions")
    
    # Get active sessions
    try:
        active_sessions = audit_service.get_sessions_by_status('in_progress')
        
        if not active_sessions:
            st.warning("⚠️ No active audit sessions available")
            return
        
        session_options = {
            f"{s['session_name']} ({s['session_code']})": s['id'] 
            for s in active_sessions
        }
        
        selected_session_key = st.selectbox("Select Active Session", session_options.keys())
        selected_session_id = session_options[selected_session_key]
        
        if selected_session_id:
            st.session_state.selected_session_id = selected_session_id
            
            # Create new transaction
            with st.expander("➕ Create New Transaction"):
                create_transaction_form_optimized(selected_session_id)
            
            # My transactions
            st.subheader("📦 My Transactions")
            show_my_transactions_optimized(selected_session_id)
            
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")

def create_transaction_form_optimized(session_id: int):
    """Optimized transaction creation form"""
    with st.form("create_transaction", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            transaction_name = st.text_input("Transaction Name*", 
                placeholder="e.g., Zone A1-A3 counting")
            assigned_zones = st.text_input("Assigned Zones", 
                placeholder="e.g., A1,A2,A3")
        
        with col2:
            assigned_categories = st.text_input("Assigned Categories", 
                placeholder="e.g., Cold items")
            notes = st.text_area("Notes", placeholder="Additional notes")
        
        submit = st.form_submit_button("📝 Create Transaction", use_container_width=True)
        
        if submit and transaction_name:
            try:
                transaction_code = audit_service.create_transaction({
                    'session_id': session_id,
                    'transaction_name': transaction_name,
                    'assigned_zones': assigned_zones,
                    'assigned_categories': assigned_categories,
                    'notes': notes,
                    'created_by_user_id': st.session_state.user_id
                })
                
                st.success(f"✅ Transaction created! Code: {transaction_code}")
                
                # Clear transaction cache
                st.session_state.cached_transactions = None
                time.sleep(1)
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

def show_my_transactions_optimized(session_id: int):
    """Display transactions with minimal reruns"""
    try:
        # Use cached transactions if available
        cache_key = f"transactions_{session_id}_{st.session_state.user_id}"
        
        if st.session_state.cached_transactions is None:
            transactions = audit_service.get_user_transactions(
                session_id, st.session_state.user_id
            )
            st.session_state.cached_transactions = transactions
        else:
            transactions = st.session_state.cached_transactions
        
        if transactions:
            for tx in transactions:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        st.write(f"**{tx['transaction_name']}**")
                        st.caption(f"Code: {tx['transaction_code']}")
                    
                    with col2:
                        st.write(f"Status: {tx['status'].title()}")
                        if tx['assigned_zones']:
                            st.caption(f"Zones: {tx['assigned_zones']}")
                    
                    with col3:
                        st.write(f"Items: {tx['total_items_counted']}")
                    
                    with col4:
                        if tx['status'] == 'draft':
                            if st.button("✅ Submit", key=f"submit_{tx['id']}"):
                                try:
                                    if tx.get('total_items_counted', 0) > 0:
                                        audit_service.submit_transaction(tx['id'], st.session_state.user_id)
                                        st.success("✅ Transaction submitted!")
                                        st.session_state.cached_transactions = None
                                        time.sleep(1)
                                        st.rerun()
                                    else:
                                        st.warning("⚠️ Count items first")
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                    
                    st.markdown("---")
        else:
            st.info("No transactions created yet")
            
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")

# ============== OPTIMIZED COUNTING PAGE ==============

def counting_page_optimized():
    """Highly optimized counting page with minimal reruns"""
    st.subheader("🔢 Fast Inventory Counting")
    
    # Initialize session state
    init_session_state()
    
    # Transaction selection
    if 'selected_session_id' not in st.session_state:
        st.warning("⚠️ Please select a session in My Transactions first")
        return
    
    # Get draft transactions
    try:
        if st.session_state.cached_transactions is None:
            transactions = audit_service.get_user_transactions(
                st.session_state.selected_session_id,
                st.session_state.user_id,
                status='draft'
            )
            st.session_state.cached_transactions = [t for t in transactions if t['status'] == 'draft']
        
        draft_transactions = [t for t in st.session_state.cached_transactions if t['status'] == 'draft']
        
        if not draft_transactions:
            st.warning("⚠️ No draft transactions available for counting")
            # Refresh transactions
            if st.button("🔄 Refresh Transactions"):
                st.session_state.cached_transactions = None
                st.rerun()
            return
        
        # Transaction selector
        tx_options = {
            f"{tx['transaction_name']} ({tx['transaction_code']})": tx
            for tx in draft_transactions
        }
        
        selected_tx_key = st.selectbox(
            "Select Transaction",
            options=list(tx_options.keys()),
            key="tx_selector_counting"
        )
        
        selected_tx = tx_options[selected_tx_key]
        st.session_state.selected_tx_id = selected_tx['id']
        st.session_state.current_warehouse_id = selected_tx['warehouse_id']
        
        # Settings row
        col_settings = st.columns([1, 1, 1, 3])
        
        with col_settings[0]:
            st.session_state.counting_mode = st.radio(
                "Mode",
                ["fast", "detailed"],
                format_func=lambda x: "⚡ Fast" if x == "fast" else "📋 Detailed",
                horizontal=True,
                key="counting_mode_selector"
            )
        
        with col_settings[1]:
            st.session_state.auto_save_enabled = st.checkbox(
                "Auto-save",
                value=st.session_state.auto_save_enabled,
                help="Auto-save when reaching threshold"
            )
        
        with col_settings[2]:
            if st.session_state.auto_save_enabled:
                st.session_state.auto_save_threshold = st.number_input(
                    "Threshold",
                    min_value=5,
                    max_value=20,
                    value=10,
                    step=5,
                    key="auto_save_threshold_input"
                )
        
        # Status display
        show_status_bar()
        
        # Main counting interface
        if st.session_state.counting_mode == "fast":
            fast_counting_interface(selected_tx['id'], selected_tx['warehouse_id'])
        else:
            detailed_counting_interface(selected_tx['id'], selected_tx['warehouse_id'])
            
    except Exception as e:
        st.error(f"Error in counting page: {str(e)}")
        logger.error(f"Counting page error: {e}")

def show_status_bar():
    """Show status bar with last action and temp counts"""
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        # Last action message
        if st.session_state.last_action:
            # Calculate age of last action
            if st.session_state.last_action_time:
                age = (datetime.now() - st.session_state.last_action_time).seconds
                if age < 5:  # Show for 5 seconds
                    if "✅" in st.session_state.last_action:
                        st.success(st.session_state.last_action)
                    elif "⚠️" in st.session_state.last_action:
                        st.warning(st.session_state.last_action)
                    elif "❌" in st.session_state.last_action:
                        st.error(st.session_state.last_action)
                    else:
                        st.info(st.session_state.last_action)
    
    with col2:
        # Temp counts summary
        summary = get_temp_counts_summary()
        if summary['total'] > 0:
            st.metric("Pending", f"{summary['total']}/20")
    
    with col3:
        # Quick actions
        if st.session_state.temp_counts:
            if st.button("💾 Save All", use_container_width=True, key="quick_save_btn"):
                save_all_counts_callback()
                st.rerun()

def fast_counting_interface(transaction_id: int, warehouse_id: int):
    """Fast counting interface with minimal UI elements"""
    
    # Create layout
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        # Product search and selection
        render_product_selector(warehouse_id)
        
        # Counting form
        if st.session_state.selected_product_data:
            render_fast_count_form(transaction_id)
    
    with col_right:
        # Temp counts display
        render_temp_counts_sidebar()
        
        # Progress summary
        render_progress_summary(transaction_id)

def render_product_selector(warehouse_id: int):
    """Render optimized product selector"""
    st.markdown("#### 📦 Product Selection")
    
    # Load products if not cached
    if st.session_state.cached_products is None:
        with st.spinner("Loading products..."):
            products = cached_get_warehouse_products(warehouse_id)
            st.session_state.cached_products = products
    else:
        products = st.session_state.cached_products
    
    if not products:
        st.warning("⚠️ No products available")
        return
    
    # Get count summaries
    if st.session_state.selected_tx_id:
        cache_key = f"count_summary_{st.session_state.selected_tx_id}"
        
        if cache_key not in st.session_state.cached_count_summaries:
            summaries = audit_service.get_transaction_count_summary(st.session_state.selected_tx_id)
            st.session_state.cached_count_summaries[cache_key] = {s['product_id']: s for s in summaries}
        
        count_summaries = st.session_state.cached_count_summaries[cache_key]
    else:
        count_summaries = {}
    
    # Create product options with status
    product_options = ["-- Select Product --"]
    product_lookup = {}
    
    for p in products:
        # Get count status
        count_info = count_summaries.get(p['product_id'], {})
        counted_qty = count_info.get('total_counted', 0)
        system_qty = p.get('total_quantity', 0)
        
        # Check temp counts
        temp_qty = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                      if tc.get('product_id') == p['product_id'])
        
        # Status indicator
        if temp_qty > 0:
            status = "📝"  # Has pending
        elif counted_qty >= system_qty * 0.95:
            status = "✅"  # Fully counted
        elif counted_qty > 0:
            status = "🟡"  # Partially counted
        else:
            status = "⭕"  # Not counted
        
        # Format option
        display = f"{status} {p.get('pt_code', 'N/A')} - {p.get('product_name', 'Unknown')[:50]}"
        if counted_qty > 0 or temp_qty > 0:
            display += f" (Counted: {counted_qty:.0f}"
            if temp_qty > 0:
                display += f", Pending: {temp_qty:.0f}"
            display += ")"
        
        product_options.append(display)
        product_lookup[display] = p
    
    st.session_state.product_lookup = product_lookup
    
    # Product selector with callback
    st.selectbox(
        "Choose Product",
        options=product_options,
        key="product_selector_fast",
        on_change=on_product_selected,
        help="⭕ Not counted | 🟡 Partial | ✅ Complete | 📝 Has pending"
    )

def render_fast_count_form(transaction_id: int):
    """Render fast counting form"""
    st.markdown("#### ✏️ Quick Count")
    
    product = st.session_state.selected_product_data
    
    # Product info
    col1, col2 = st.columns(2)
    with col1:
        st.info(f"**{product['product_name']}**\nPT: {product.get('pt_code', 'N/A')}")
    with col2:
        st.info(f"**System Total:** {product.get('total_quantity', 0):.0f}")
    
    # Batch selector if batches loaded
    if product['product_id'] in st.session_state.cached_batches:
        batches = st.session_state.cached_batches[product['product_id']]
        
        if batches:
            batch_options = ["-- Manual Entry --"] + [
                f"{b['batch_no']} (Qty: {b['quantity']:.0f}, Exp: {b.get('expired_date', 'N/A') if b.get('expired_date') else 'N/A'})"
                for b in batches
            ]
            
            st.selectbox(
                "Quick Select Batch",
                options=batch_options,
                key="batch_selector_fast",
                on_change=on_batch_selected
            )
    
    # Show selected batch info
    if st.session_state.selected_batch_data:
        batch_info = st.session_state.selected_batch_data
        st.success(f"📦 Selected Batch: {batch_info['batch_no']} | System Qty: {batch_info['quantity']:.0f} | Location: {batch_info.get('location', 'N/A')}")
    
    # Count inputs
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Batch number field - auto-populated or manual entry
        default_batch_no = ""
        batch_disabled = False
        
        if st.session_state.selected_batch_data:
            default_batch_no = st.session_state.selected_batch_data['batch_no']
            batch_disabled = True  # Disable when auto-selected
        
        st.text_input(
            "Batch Number*",
            value=default_batch_no,
            placeholder="Enter batch number",
            key="batch_no_input",
            disabled=batch_disabled,
            help="Auto-filled when batch selected above"
        )
        
        # Actual quantity
        st.number_input(
            "Actual Quantity*",
            min_value=0.0,
            step=1.0,
            format="%.0f",
            key="actual_qty_input"
        )
        
        # Location
        default_location = ""
        if st.session_state.selected_batch_data:
            default_location = st.session_state.selected_batch_data.get('location', '')
        
        st.text_input(
            "Location",
            value=default_location,
            placeholder="A1-R01-B01",
            key="location_input"
        )
    
    with col2:
        # Expiry date - auto-populated if available
        if st.session_state.selected_batch_data and st.session_state.selected_batch_data.get('expired_date'):
            try:
                exp_date = pd.to_datetime(st.session_state.selected_batch_data['expired_date']).date()
                st.date_input(
                    "Expiry Date",
                    value=exp_date,
                    key="expiry_date_input",
                    disabled=True
                )
            except:
                st.date_input("Expiry Date", key="expiry_date_input")
        else:
            st.date_input("Expiry Date", key="expiry_date_input")
        
        # Notes
        st.text_area(
            "Notes",
            placeholder="Optional notes",
            height=100,
            key="notes_input"
        )
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.button(
            f"➕ Add ({len(st.session_state.temp_counts)}/20)",
            on_click=add_count_callback,
            disabled=len(st.session_state.temp_counts) >= 20,
            use_container_width=True,
            type="primary"
        )
    
    with col2:
        st.button(
            "💾 Save All",
            on_click=save_all_counts_callback,
            disabled=len(st.session_state.temp_counts) == 0,
            use_container_width=True
        )
    
    with col3:
        if st.button("🔄 Clear", use_container_width=True):
            # Clear all form fields
            st.session_state.actual_qty_input = 0.0
            st.session_state.location_input = ""
            st.session_state.notes_input = ""
            st.session_state.batch_no_input = ""
            st.session_state.expiry_date_input = None
            st.session_state.selected_batch_data = None
            st.session_state.batch_selector_fast = "-- Manual Entry --"

def render_temp_counts_sidebar():
    """Render temp counts in sidebar format"""
    if st.session_state.temp_counts:
        st.markdown(f"#### 📋 Pending ({len(st.session_state.temp_counts)})")
        
        # Create scrollable container
        with st.container():
            for i, count in enumerate(st.session_state.temp_counts):
                # Create expandable header with key info
                header = f"{count['pt_code']} | Batch: {count.get('batch_no', 'N/A')} | Qty: {count['actual_quantity']:.0f}"
                
                with st.expander(header, expanded=False):
                    st.caption(f"Product: {count['product_name']}")
                    st.caption(f"Batch: {count.get('batch_no', 'N/A')}")
                    if count.get('expired_date'):
                        st.caption(f"Expiry: {count['expired_date']}")
                    
                    location = f"{count.get('zone_name', '')}"
                    if count.get('rack_name'):
                        location += f"-{count['rack_name']}"
                    if count.get('bin_name'):
                        location += f"-{count['bin_name']}"
                    if location.strip():
                        st.caption(f"Location: {location}")
                    
                    # Show variance if available
                    if count.get('system_quantity', 0) > 0:
                        variance = count['actual_quantity'] - count['system_quantity']
                        variance_pct = (variance / count['system_quantity']) * 100
                        if variance != 0:
                            if variance > 0:
                                st.caption(f"📈 Variance: +{variance:.0f} ({variance_pct:+.1f}%)")
                            else:
                                st.caption(f"📉 Variance: {variance:.0f} ({variance_pct:+.1f}%)")
                    
                    if count.get('actual_notes'):
                        st.caption(f"Notes: {count['actual_notes']}")
                    
                    if st.button(f"🗑️ Remove", key=f"remove_{i}"):
                        remove_temp_count(i)
                        st.rerun()
    else:
        st.info("No pending counts")

def render_progress_summary(transaction_id: int):
    """Render progress summary"""
    st.markdown("#### 📊 Progress")
    
    try:
        # Get progress from service
        progress = audit_service.get_transaction_progress(transaction_id)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Saved Items", progress.get('items_counted', 0))
        
        with col2:
            st.metric("Total Value", f"${progress.get('total_value', 0):,.0f}")
        
        # Include pending counts
        if st.session_state.temp_counts:
            st.caption(f"➕ {len(st.session_state.temp_counts)} pending saves")
            
            # Show unique batches in pending
            unique_batches = set(c.get('batch_no', '') for c in st.session_state.temp_counts if c.get('batch_no'))
            if unique_batches:
                st.caption(f"📦 Batches pending: {', '.join(sorted(unique_batches))}")
            
    except Exception as e:
        st.error(f"Error loading progress: {str(e)}")

def detailed_counting_interface(transaction_id: int, warehouse_id: int):
    """Detailed counting interface with all features"""
    st.info("📋 Detailed counting mode - includes batch details, variance analysis, etc.")
    
    # Use existing interface for now
    product_search_and_count_enhanced(transaction_id)
    
    st.subheader("📊 Progress")
    show_counting_progress_enhanced(transaction_id)

# ============== LEGACY FUNCTIONS (kept for compatibility) ==============

def product_search_and_count_enhanced(transaction_id: int):
    """Legacy enhanced product search - to be replaced"""
    st.warning("Using legacy interface - switch to Fast mode for better performance")

def show_counting_progress_enhanced(transaction_id: int):
    """Legacy progress display - to be replaced"""
    try:
        progress = audit_service.get_transaction_progress(transaction_id)
        recent_counts = audit_service.get_recent_counts(transaction_id, limit=5)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Items Counted", progress.get('items_counted', 0))
        
        with col2:
            st.metric("Total Value", f"${progress.get('total_value', 0):,.2f}")
            
    except Exception as e:
        st.error(f"Error loading progress: {str(e)}")

# ============== MAIN EXECUTION ==============

if __name__ == "__main__":
    # Track actions
    st.session_state.action_count = st.session_state.get('action_count', 0) + 1
    
    # Run main app
    main()
# main.py - Warehouse Audit System with Enhanced Batch Counting
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import logging
import time
from typing import Dict, List, Optional, Tuple
import json

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

# Initialize session state for temporary counts
if 'temp_counts' not in st.session_state:
    st.session_state.temp_counts = []
if 'count_counter' not in st.session_state:
    st.session_state.count_counter = 0
if 'last_selected_product' not in st.session_state:
    st.session_state.last_selected_product = None
if 'show_count_history' not in st.session_state:
    st.session_state.show_count_history = {}

# ============== TEMP COUNT MANAGEMENT ==============

def add_temp_count(count_data: Dict):
    """Add count to temporary storage"""
    st.session_state.temp_counts.append(count_data)
    st.session_state.count_counter += 1
    
def remove_temp_count(index: int):
    """Remove count from temporary storage"""
    if 0 <= index < len(st.session_state.temp_counts):
        st.session_state.temp_counts.pop(index)
        
def clear_temp_counts():
    """Clear temporary counts after saving"""
    st.session_state.temp_counts = []
    st.session_state.count_counter = 0

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

# ============== CACHE WRAPPER FUNCTIONS ==============

@st.cache_data(ttl=3600)  # Cache for 1 hour
def cached_get_warehouses():
    """Cached wrapper for get_warehouses"""
    return audit_service.get_warehouses()

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def cached_get_warehouse_brands(warehouse_id: int):
    """Cached wrapper for get_warehouse_brands"""
    return audit_service.get_warehouse_brands(warehouse_id)

@st.cache_data(ttl=900)  # Cache for 15 minutes
def cached_get_warehouse_products(warehouse_id: int):
    """Cached wrapper for get_warehouse_products"""
    return audit_service.get_warehouse_products(warehouse_id)

@st.cache_data(ttl=600)  # Cache for 10 minutes
def cached_search_products_with_filters(warehouse_id: int, search_term: str = "", brand_filter: str = ""):
    """Cached wrapper for search_products_with_filters"""
    return audit_service.search_products_with_filters(warehouse_id, search_term, brand_filter)

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
    if 'user_role' not in st.session_state:
        return False
    
    user_role = st.session_state.user_role
    return action in AUDIT_ROLES.get(user_role, [])

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
        
        # Role-based access info
        user_permissions = AUDIT_ROLES.get(st.session_state.user_role, [])
        if user_permissions:
            st.markdown("**Your Permissions:**")
            for perm in user_permissions:
                perm_display = {
                    'manage_sessions': '🔧 Manage Sessions',
                    'view_all': '👁️ View All Data', 
                    'create_transactions': '📝 Create Transactions',
                    'export_data': '📊 Export Data',
                    'user_management': '👥 User Management',
                    'view_own': '👤 View Own Data',
                    'view_assigned_sessions': '📋 View Assigned Sessions'
                }
                st.caption(f"• {perm_display.get(perm, perm)}")
        else:
            st.warning("⚠️ No audit permissions")
        
        st.markdown("---")
        
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    # Main content based on role
    user_role = st.session_state.user_role
    
    # Check if user has any audit permissions
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
    
    st.markdown("""
    ### Contact Administrator
    
    If you need access to the Warehouse Audit System, please contact:
    - **System Administrator** for role permissions
    - **Supply Chain Manager** for audit participation
    
    ### Available Roles for Audit Access:
    - **Admin/GM/MD**: Full system access
    - **Supply Chain/Sales Manager**: Session management
    - **Sales**: Audit participation
    - **Viewer**: Read-only access
    """)
    
    user_role = st.session_state.user_role
    if user_role in ['customer', 'vendor']:
        st.info("💡 External users (Customer/Vendor) don't have audit system access by design")

def show_admin_interface():
    """Admin interface for session management"""
    st.title("🔧 Admin Dashboard")
    
    # Check specific admin permissions
    user_role = st.session_state.user_role
    is_super_admin = user_role in ['admin']
    is_executive = user_role in ['GM', 'MD']
    
    # Admin level tabs - Include Warehouse Audit for all admin roles
    if is_super_admin:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Session Management", "📊 Dashboard", "📈 Reports", "📦 Warehouse Audit", "👥 User Management"])
        
        with tab5:
            user_management_page()
    else:
        # For GM, MD, supply_chain, sales_manager - they can both manage and participate
        tab1, tab2, tab3, tab4 = st.tabs(["📋 Session Management", "📊 Dashboard", "📈 Reports", "📦 Warehouse Audit"])
    
    with tab1:
        session_management_page()
    
    with tab2:
        admin_dashboard_page()
    
    with tab3:
        reports_page()
    
    with tab4:
        # Show user interface within admin interface
        show_warehouse_audit_content()

def show_warehouse_audit_content():
    """Warehouse audit content that can be used in both admin and user interfaces"""
    user_role = st.session_state.user_role
    
    # Different tabs based on role level
    if user_role in ['sales_manager']:
        # Sales managers can see team overview
        subtab1, subtab2, subtab3 = st.tabs(["📝 My Transactions", "🔢 Counting", "👥 Team Overview"])
        
        with subtab3:
            team_overview_page()
    else:
        # Regular users and executives see standard interface
        subtab1, subtab2 = st.tabs(["📝 My Transactions", "🔢 Counting"])
    
    with subtab1:
        my_transactions_page()
    
    with subtab2:
        counting_page()

def show_user_interface():
    """User interface for transactions and counting"""
    st.title("📦 Warehouse Audit")
    
    show_warehouse_audit_content()

def show_viewer_interface():
    """Viewer interface - read only"""
    st.title("👀 Audit Viewer")
    
    user_role = st.session_state.user_role
    
    if check_permission('view_assigned_sessions'):
        st.info("📋 You have view-only access to assigned audit sessions")
        
        tab1, tab2 = st.tabs(["📊 My Sessions", "📈 Reports"])
        
        with tab1:
            view_assigned_sessions_page()
        
        with tab2:
            view_own_reports_page()
    else:
        st.info("👁️ You have view-only access to your own audit data")
        
        tab1, tab2 = st.tabs(["📊 Sessions", "📈 Reports"])
        
        with tab1:
            view_sessions_page()
        
        with tab2:
            reports_page()

# ============== ADMIN PAGES ==============

def session_management_page():
    """Admin session management page"""
    st.subheader("📋 Audit Session Management")
    
    # Quick stats
    try:
        stats = audit_service.get_dashboard_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Sessions", stats.get('active_sessions', 0))
        with col2:
            st.metric("Draft Sessions", stats.get('draft_sessions', 0))
        with col3:
            st.metric("Completed Today", stats.get('completed_today', 0))
        with col4:
            st.metric("Users Online", stats.get('active_users', 0))
    
    except Exception as e:
        st.warning(f"Could not load stats: {str(e)}")
        logger.error(f"Dashboard stats error: {e}")
    
    # Create new session
    with st.expander("➕ Create New Audit Session"):
        create_session_form()
    
    # Draft sessions (newly created, need to be started)
    st.subheader("📝 Draft Sessions")
    show_draft_sessions()
    
    # Active sessions
    st.subheader("🔄 Active Sessions")
    show_active_sessions()
    
    # Recent completed sessions
    st.subheader("✅ Recent Completed Sessions")
    show_completed_sessions()

def create_session_form():
    """Form to create new audit session (with cached warehouse list)"""
    
    # Warehouse selection outside form for real-time updates
    st.markdown("#### 🏢 Select Warehouse")
    warehouses = cached_get_warehouses()  # Using cached version
    
    if not warehouses:
        st.warning("⚠️ No warehouses available")
        return
        
    warehouse_options = {f"{w['name']} (ID: {w['id']})": w['id'] for w in warehouses}
    warehouse_options_with_empty = {"-- Select Warehouse --": None, **warehouse_options}
    
    selected_warehouse = st.selectbox(
        "Warehouse*", 
        warehouse_options_with_empty.keys(),
        key="warehouse_selection"
    )
    
    selected_warehouse_id = warehouse_options_with_empty[selected_warehouse]
    
    # Show warehouse details if warehouse is selected
    if selected_warehouse_id:
        try:
            warehouse_detail = audit_service.get_warehouse_detail(selected_warehouse_id)
            
            if warehouse_detail:
                st.markdown("#### 📋 Warehouse Information")
                
                # Create info container with nice styling
                with st.container():
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        company_display = warehouse_detail.get('company_name', 'N/A')
                        if warehouse_detail.get('company_local_name'):
                            company_display += f" ({warehouse_detail.get('company_local_name')})"
                        
                        st.markdown(f"**🏢 Company:** {company_display}")
                        st.markdown(f"**🏭 Name:** {warehouse_detail.get('name', 'N/A')}")
                        st.markdown(f"**🌍 Country:** {warehouse_detail.get('country_name', 'N/A')}")
                        st.markdown(f"**📍 Address:** {warehouse_detail.get('address', 'N/A')}")
                    
                    with col2:
                        st.markdown(f"**👤 Manager:** {warehouse_detail.get('manager_name', 'N/A')}")
                        st.markdown(f"**📮 Zipcode:** {warehouse_detail.get('zipcode', 'N/A')}")
                        st.markdown(f"**🗺️ State/Province:** {warehouse_detail.get('state_province', 'N/A')}")
                        if warehouse_detail.get('manager_email'):
                            st.markdown(f"**📧 Manager Email:** {warehouse_detail.get('manager_email')}")
                
                st.markdown("---")
            else:
                st.warning("⚠️ Could not load warehouse details")
                
        except Exception as e:
            st.error(f"❌ Error loading warehouse details: {str(e)}")
            st.write(f"Selected warehouse ID: {selected_warehouse_id}")
    
    # Session creation form
    with st.form("create_session"):
        st.markdown("#### ⚙️ Session Details")
        
        col1, col2 = st.columns(2)
        
        with col1:
            session_name = st.text_input("Session Name*", 
                placeholder="e.g., Audit HN before relocation")
            
        with col2:
            planned_start = st.date_input("Planned Start Date", value=date.today())
            planned_end = st.date_input("Planned End Date", value=date.today())
            
        notes = st.text_area("Notes", placeholder="Additional information about this audit session")
        
        submit = st.form_submit_button("🚀 Create Session", use_container_width=True)
        
        if submit:
            if session_name and selected_warehouse_id:
                try:
                    session_code = audit_service.create_session({
                        'session_name': session_name,
                        'warehouse_id': selected_warehouse_id,
                        'planned_start_date': planned_start,
                        'planned_end_date': planned_end,
                        'notes': notes,
                        'created_by_user_id': st.session_state.user_id
                    })
                    
                    st.success(f"✅ Session created successfully! Code: {session_code}")
                    # Clear cache to ensure fresh data
                    st.cache_data.clear()
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error creating session: {str(e)}")
            else:
                st.warning("⚠️ Please fill in all required fields (Session Name and Warehouse)")

def show_draft_sessions():
    """Display draft sessions with Start button"""
    try:
        sessions = audit_service.get_sessions_by_status('draft')
        
        if sessions:
            for session in sessions:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        st.write(f"**{session['session_name']}**")
                        st.caption(f"Code: {session['session_code']}")
                    
                    with col2:
                        st.write(f"Warehouse: {session.get('warehouse_name', 'N/A')}")
                        st.caption(f"Created: {session.get('created_date', 'N/A')}")
                    
                    with col3:
                        st.write(f"Planned: {session.get('planned_start_date', 'N/A')}")
                        st.caption(f"To: {session.get('planned_end_date', 'N/A')}")
                    
                    with col4:
                        if st.button("🚀 Start", key=f"start_{session['id']}", help="Start this audit session"):
                            try:
                                if audit_service.start_session(session['id'], st.session_state.user_id):
                                    st.success(f"✅ Session {session['session_code']} started!")
                                    st.rerun()
                                else:
                                    st.error("❌ Failed to start session")
                            except Exception as e:
                                st.error(f"❌ Error starting session: {str(e)}")
                    
                    st.markdown("---")
        else:
            st.info("No draft sessions found")
            
    except Exception as e:
        st.error(f"Error loading draft sessions: {str(e)}")

def show_active_sessions():
    """Display active sessions table"""
    try:
        sessions = audit_service.get_sessions_by_status('in_progress')
        
        if sessions:
            for session in sessions:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                    
                    with col1:
                        st.write(f"**{session['session_name']}**")
                        st.caption(f"Code: {session['session_code']}")
                    
                    with col2:
                        st.write(f"Warehouse: {session.get('warehouse_name', 'N/A')}")
                        st.caption(f"Started: {session.get('actual_start_date', 'Not started')}")
                    
                    with col3:
                        # Get session progress
                        try:
                            progress = audit_service.get_session_progress(session['id'])
                            st.write(f"Progress: {progress.get('completion_rate', 0):.1f}%")
                            st.caption(f"Transactions: {progress.get('total_transactions', 0)}")
                        except Exception as e:
                            st.caption("Progress: Unable to load")
                    
                    with col4:
                        if st.button("🛑 Stop", key=f"stop_{session['id']}", help="Complete this audit session"):
                            try:
                                if audit_service.complete_session(session['id'], st.session_state.user_id):
                                    st.success(f"✅ Session {session['session_code']} completed!")
                                    st.rerun()
                                else:
                                    st.error("❌ Failed to complete session")
                            except Exception as e:
                                st.error(f"❌ Error completing session: {str(e)}")
                    
                    st.markdown("---")
        else:
            st.info("No active sessions found")
            
    except Exception as e:
        st.error(f"Error loading active sessions: {str(e)}")

def show_completed_sessions():
    """Display completed sessions"""
    try:
        sessions = audit_service.get_sessions_by_status('completed', limit=5)
        
        if sessions:
            for session in sessions:
                with st.container():
                    col1, col2, col3 = st.columns([4, 3, 3])
                    
                    with col1:
                        st.write(f"**{session['session_name']}**")
                        st.caption(f"Code: {session['session_code']}")
                    
                    with col2:
                        st.write(f"Warehouse: {session.get('warehouse_name', 'N/A')}")
                        st.caption(f"Completed by: {session.get('completed_by_username', 'N/A')}")
                    
                    with col3:
                        st.write(f"Completed: {session.get('completed_date', 'N/A')}")
                        duration = "N/A"
                        if session.get('actual_start_date') and session.get('actual_end_date'):
                            try:
                                start = pd.to_datetime(session['actual_start_date'])
                                end = pd.to_datetime(session['actual_end_date'])
                                duration = str(end - start).split('.')[0]  # Remove microseconds
                            except:
                                pass
                        st.caption(f"Duration: {duration}")
                    
                    st.markdown("---")
        else:
            st.info("No completed sessions found")
            
    except Exception as e:
        st.error(f"Error loading completed sessions: {str(e)}")

def admin_dashboard_page():
    """Admin dashboard with metrics"""
    st.subheader("📊 System Overview")
    
    try:
        # Daily metrics
        daily_stats = audit_service.get_daily_stats()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📈 Session Activity")
            if daily_stats:
                df = pd.DataFrame(daily_stats)
                st.line_chart(df.set_index('audit_date'))
            else:
                st.info("No data available")
        
        with col2:
            st.markdown("#### 👥 User Activity")
            user_stats = audit_service.get_user_activity_stats()
            if user_stats:
                df = pd.DataFrame(user_stats)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No activity data")
                
    except Exception as e:
        st.error(f"Error loading dashboard: {str(e)}")

# ============== USER PAGES ==============

def my_transactions_page():
    """User transactions management page"""
    st.subheader("📝 My Audit Transactions")
    
    # Select active session
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
                create_transaction_form(selected_session_id)
            
            # My transactions for this session
            st.subheader("📦 My Transactions")
            show_my_transactions(selected_session_id)
            
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")

def create_transaction_form(session_id: int):
    """Form to create new transaction"""
    with st.form("create_transaction"):
        col1, col2 = st.columns(2)
        
        with col1:
            transaction_name = st.text_input("Transaction Name*", 
                placeholder="e.g., Zone A1-A3 counting")
            assigned_zones = st.text_input("Assigned Zones", 
                placeholder="e.g., A1,A2,A3 or Freezer Section")
        
        with col2:
            assigned_categories = st.text_input("Assigned Categories", 
                placeholder="e.g., Cold items, Antibiotics")
            notes = st.text_area("Notes", placeholder="Additional notes for this transaction")
        
        submit = st.form_submit_button("📝 Create Transaction", use_container_width=True)
        
        if submit:
            if transaction_name:
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
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error creating transaction: {str(e)}")
            else:
                st.warning("⚠️ Please enter transaction name")

def show_my_transactions(session_id: int):
    """Display user's transactions for session with validation"""
    try:
        transactions = audit_service.get_user_transactions(
            session_id, st.session_state.user_id
        )
        
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
                        st.caption(f"Created: {tx['created_date']}")
                    
                    with col4:
                        if tx['status'] == 'draft':
                            if st.button("✅ Submit", key=f"submit_{tx['id']}"):
                                try:
                                    # Validation before submit
                                    if tx.get('total_items_counted', 0) > 0:
                                        if audit_service.submit_transaction(tx['id'], st.session_state.user_id):
                                            st.success("✅ Transaction submitted successfully!")
                                            # Clear any temp counts
                                            clear_temp_counts()
                                            st.rerun()
                                    else:
                                        st.warning("⚠️ Please count at least one item before submitting")
                                except Exception as e:
                                    st.error(f"❌ Error: {str(e)}")
                    
                    st.markdown("---")
        else:
            st.info("No transactions created yet")
            
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")

# ============== ENHANCED COUNTING PAGE ==============

def counting_page():
    """Enhanced counting interface with batch processing"""
    st.subheader("🔢 Inventory Counting")
    
    # Select draft transaction first
    if 'selected_session_id' not in st.session_state:
        st.warning("⚠️ Please select a session in My Transactions first")
        return
    
    try:
        draft_transactions = audit_service.get_user_transactions(
            st.session_state.selected_session_id, 
            st.session_state.user_id,
            status='draft'
        )
        
        if not draft_transactions:
            st.warning("⚠️ No draft transactions available for counting")
            return
        
        tx_options = {
            f"{tx['transaction_name']} ({tx['transaction_code']})": tx['id']
            for tx in draft_transactions
        }
        
        selected_tx_key = st.selectbox("Select Transaction", tx_options.keys())
        selected_tx_id = tx_options[selected_tx_key]
        
        if selected_tx_id:
            st.session_state.selected_tx_id = selected_tx_id
            
            # Show temp counts summary if any
            if st.session_state.temp_counts:
                summary = get_temp_counts_summary()
                
                col1, col2, col3, col4 = st.columns([2, 1, 1, 2])
                with col1:
                    st.warning(f"⚠️ You have {summary['total']} unsaved counts")
                with col2:
                    st.metric("Products", summary['products'])
                with col3:
                    st.metric("Total Qty", f"{summary['quantity']:.0f}")
                with col4:
                    col_save, col_clear = st.columns(2)
                    with col_save:
                        if st.button("💾 Save All", use_container_width=True, type="primary"):
                            save_temp_counts_to_db(selected_tx_id)
                    with col_clear:
                        if st.button("🗑️ Clear All", use_container_width=True):
                            if st.checkbox("Confirm clear all counts?"):
                                clear_temp_counts()
                                st.rerun()
            
            # Display temp counts preview
            show_temp_counts_preview()
            
            # Product search and counting
            product_search_and_count_enhanced(selected_tx_id)
            
            # Progress display
            st.subheader("📊 Progress")
            show_counting_progress_enhanced(selected_tx_id)
            
    except Exception as e:
        st.error(f"Error in counting page: {str(e)}")

def product_search_and_count_enhanced(transaction_id: int):
    """Enhanced product search with count status indicators"""
    st.markdown("#### 🔍 Product Search & Count")
    
    # Get transaction info
    try:
        tx_info = audit_service.get_transaction_info(transaction_id)
        session_info = audit_service.get_session_info(tx_info['session_id'])
        warehouse_id = session_info['warehouse_id']
        warehouse_name = session_info.get('warehouse_name', 'Unknown')
        
        st.info(f"📍 **Warehouse:** {warehouse_name} | **Transaction:** {tx_info.get('transaction_name', 'N/A')}")
        
        # === FILTERS SECTION ===
        st.markdown("##### 🎯 Filters")
        
        # Brand filter
        brands = cached_get_warehouse_brands(warehouse_id)
        brand_options = ["All Brands"] + [brand['brand'] for brand in brands if brand['brand']]
        selected_brand = st.selectbox(
            "Filter by Brand", 
            brand_options, 
            key="brand_filter_enhanced"
        )
        brand_filter = "" if selected_brand == "All Brands" else selected_brand
        
        # === PRODUCT SELECTION ===
        st.markdown("##### 📦 Select Product")
        
        # Load products with count status
        try:
            # Get all products
            if brand_filter:
                products = cached_search_products_with_filters(
                    warehouse_id, "", brand_filter
                )
            else:
                products = cached_get_warehouse_products(warehouse_id)
            
            # Get count summary for transaction
            counted_products = audit_service.get_transaction_count_summary(transaction_id)
            counted_dict = {cp['product_id']: cp for cp in counted_products}
            
            if products:
                # Create product options with count status
                product_options = ["-- Type to search or select product --"]
                product_data = {}
                
                for p in products:
                    pt_code = p.get('pt_code', 'N/A')
                    product_name = p.get('product_name', 'Unknown')
                    brand = p.get('brand', 'N/A')
                    total_qty = p.get('total_quantity', 0)
                    
                    # Check count status
                    count_info = counted_dict.get(p['product_id'], {})
                    counted_qty = count_info.get('total_counted', 0)
                    
                    # Status indicator
                    if counted_qty > 0:
                        if counted_qty >= total_qty * 0.95:  # 95% or more counted
                            status = "✅"
                        else:
                            status = "🟡"
                        count_text = f" | Counted: {counted_qty:.0f}"
                    else:
                        status = "⭕"
                        count_text = ""
                    
                    # Check if in temp counts
                    temp_count = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                                   if tc.get('product_id') == p['product_id'])
                    if temp_count > 0:
                        status = "📝"
                        count_text += f" | Pending: {temp_count:.0f}"
                    
                    # Format display
                    display_text = f"{status} {pt_code} - {product_name[:40]}{'...' if len(product_name) > 40 else ''} [{brand}] (Sys: {total_qty:.0f}{count_text})"
                    
                    product_options.append(display_text)
                    product_data[display_text] = p
                
                # Product selection
                selected_product_display = st.selectbox(
                    f"Choose Product ({len(products)} available)",
                    product_options,
                    key=f"product_selector_enhanced_{warehouse_id}_{len(products)}",
                    help="🔍 Type to search by PT code or product name"
                )
                
                # Show product details if selected
                if selected_product_display != "-- Type to search or select product --":
                    selected_product = product_data[selected_product_display]
                    st.session_state.last_selected_product = selected_product
                    show_enhanced_product_count_form(transaction_id, selected_product, warehouse_id)
                
                # Filter info
                if brand_filter:
                    st.caption(f"🔍 Filtered by brand: {brand_filter}")
                    
                # Legend
                with st.expander("📊 Status Legend"):
                    st.markdown("""
                    - ⭕ Not counted yet
                    - 🟡 Partially counted
                    - ✅ Fully counted (≥95%)
                    - 📝 Has pending counts (not saved)
                    """)
                
            else:
                st.warning("⚠️ No products available in this warehouse")
        
        except Exception as e:
            st.error(f"Error loading products: {str(e)}")
            logger.error(f"Product loading error: {e}")
    
    except Exception as e:
        st.error(f"Error in product search: {str(e)}")

def show_enhanced_product_count_form(transaction_id: int, product: Dict, warehouse_id: int):
    """Enhanced counting form with batch processing"""
    st.markdown("---")
    st.markdown("#### 📦 Product Details & Counting")
    
    # Get existing counts for this product
    existing_counts = audit_service.get_product_counts(transaction_id, product['product_id'])
    
    # Product information display
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📋 Product Information**")
            st.write(f"**Product:** {product.get('product_name', 'N/A')}")
            st.write(f"**PT Code:** {product.get('pt_code', 'N/A')}")
            st.write(f"**Brand:** {product.get('brand', 'N/A')}")
            st.write(f"**Package Size:** {product.get('package_size', 'N/A')}")
            st.write(f"**UOM:** {product.get('standard_uom', 'N/A')}")
        
        with col2:
            st.markdown("**📊 Count Summary**")
            system_total = product.get('total_quantity', 0)
            counted_total = sum(c['total_counted'] for c in existing_counts)
            pending_total = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                              if tc.get('product_id') == product['product_id'])
            
            col_sys, col_cnt, col_pnd = st.columns(3)
            with col_sys:
                st.metric("System", f"{system_total:.0f}")
            with col_cnt:
                st.metric("Counted", f"{counted_total:.0f}")
            with col_pnd:
                if pending_total > 0:
                    st.metric("Pending", f"{pending_total:.0f}", delta=f"+{pending_total:.0f}")
                else:
                    st.metric("Pending", "0")
            
            # Variance preview
            total_with_pending = counted_total + pending_total
            if total_with_pending > 0:
                variance = total_with_pending - system_total
                variance_pct = (variance / system_total * 100) if system_total > 0 else 0
                
                if abs(variance_pct) > 5:
                    if variance > 0:
                        st.success(f"📈 Variance: +{variance:.0f} ({variance_pct:+.1f}%)")
                    else:
                        st.error(f"📉 Variance: {variance:.0f} ({variance_pct:+.1f}%)")
    
    # Get batch details
    batch_details = audit_service.get_product_batch_details(warehouse_id, product['product_id'])
    
    if batch_details:
        # Get batch count status
        batch_counts = audit_service.get_batch_count_status(transaction_id, product['product_id'])
        count_dict = {bc['batch_no']: bc for bc in batch_counts}
        
        # Show batch details with count status
        with st.expander(f"📋 View All {len(batch_details)} Batches", expanded=True):
            display_batch_details_with_counts(batch_details, count_dict, transaction_id, product['product_id'])
    
    # BATCH SELECTION AND COUNTING FORM
    st.markdown("#### ✏️ Record Count")
    
    # Initialize form values
    selected_batch_data = None
    
    # Batch selection (outside form for reactivity)
    if batch_details:
        batch_options = ["-- Manual Entry --"] + [
            f"{b['batch_no']} (Sys: {b['quantity']:.0f}, Exp: {b.get('expired_date', 'N/A')})" 
            for b in batch_details
        ]
        
        selected_batch_option = st.selectbox(
            "Quick Select Batch", 
            batch_options,
            key=f"batch_selector_{product['product_id']}",
            help="Select from existing batches or choose manual entry"
        )
        
        # Get selected batch data
        if selected_batch_option != "-- Manual Entry --":
            selected_batch_no = selected_batch_option.split(" (")[0]
            for batch in batch_details:
                if batch['batch_no'] == selected_batch_no:
                    selected_batch_data = batch
                    break
    
    # Create unique form key
    form_key = f"count_form_{product['product_id']}_{selected_batch_data['batch_no'] if selected_batch_data else 'manual'}"
    
    # Container for form submission result
    if 'form_submitted' not in st.session_state:
        st.session_state.form_submitted = False
    
    # Counting form
    with st.form(key=form_key, clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            # Batch number
            if selected_batch_data:
                batch_no = selected_batch_data['batch_no']
                st.text_input(
                    "Batch Number", 
                    value=batch_no,
                    disabled=True,
                    help="Auto-populated from selected batch"
                )
            else:
                batch_no = st.text_input(
                    "Batch Number", 
                    placeholder="Enter batch number",
                    key=f"{form_key}_batch"
                )
            
            # Expiry date
            if selected_batch_data and selected_batch_data.get('expired_date'):
                try:
                    exp_date = pd.to_datetime(selected_batch_data['expired_date']).date()
                except:
                    exp_date = None
            else:
                exp_date = None
                
            expired_date = st.date_input("Expiry Date", value=exp_date, key=f"{form_key}_expiry")
            
            # Show system quantity if available
            if selected_batch_data:
                st.info(f"📊 System Quantity: {selected_batch_data['quantity']:.2f}")
                
                # Show if already counted
                batch_count_info = count_dict.get(selected_batch_data['batch_no'], {})
                if batch_count_info.get('total_counted', 0) > 0:
                    st.warning(f"⚠️ Already counted: {batch_count_info['total_counted']:.0f} ({batch_count_info['count_times']} times)")
            
            # Actual quantity
            actual_quantity = st.number_input(
                "Actual Quantity Counted*", 
                min_value=0.0, 
                step=1.0, 
                format="%.2f",
                key=f"{form_key}_qty",
                help="Enter the exact quantity you counted"
            )
        
        with col2:
            # Location method
            location_method = st.radio(
                "Location Entry:",
                ["Quick Location", "Detailed"],
                horizontal=True,
                key=f"{form_key}_loc_method"
            )
            
            # Location input
            if location_method == "Quick Location":
                default_location = ""
                if selected_batch_data:
                    default_location = selected_batch_data.get('location', '')
                    
                quick_location = st.text_input(
                    "Location", 
                    value=default_location,
                    placeholder="e.g., A1-R01-B01",
                    key=f"{form_key}_quick_loc"
                )
                
                # Parse location
                if quick_location and '-' in quick_location:
                    parts = quick_location.split('-')
                    zone_name = parts[0].strip() if len(parts) > 0 else ""
                    rack_name = parts[1].strip() if len(parts) > 1 else ""
                    bin_name = parts[2].strip() if len(parts) > 2 else ""
                else:
                    zone_name = quick_location.strip() if quick_location else ""
                    rack_name = ""
                    bin_name = ""
            else:
                col_z, col_r, col_b = st.columns(3)
                with col_z:
                    zone_name = st.text_input(
                        "Zone", 
                        value=selected_batch_data.get('zone_name', '') if selected_batch_data else "",
                        placeholder="e.g., A1",
                        key=f"{form_key}_zone"
                    )
                with col_r:
                    rack_name = st.text_input(
                        "Rack", 
                        value=selected_batch_data.get('rack_name', '') if selected_batch_data else "",
                        placeholder="e.g., R01",
                        key=f"{form_key}_rack"
                    )
                with col_b:
                    bin_name = st.text_input(
                        "Bin", 
                        value=selected_batch_data.get('bin_name', '') if selected_batch_data else "",
                        placeholder="e.g., B01",
                        key=f"{form_key}_bin"
                    )
            
            # Notes
            actual_notes = st.text_area(
                "Count Notes", 
                placeholder="Any observations (damage, expiry, etc.)",
                height=100,
                key=f"{form_key}_notes"
            )
        
        # Form buttons
        col_add, col_save = st.columns(2)
        
        with col_add:
            add_button = st.form_submit_button(
                f"➕ Add Count ({len(st.session_state.temp_counts)}/20)", 
                use_container_width=True,
                type="secondary",
                disabled=len(st.session_state.temp_counts) >= 20
            )
        
        with col_save:
            save_button = st.form_submit_button(
                "💾 Add & Save All", 
                use_container_width=True,
                type="primary"
            )
    
    # Handle form submission OUTSIDE the form
    if add_button or save_button:
        if actual_quantity > 0:
            # Prepare count data
            count_data = {
                'transaction_id': transaction_id,
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'pt_code': product.get('pt_code', 'N/A'),
                'batch_no': batch_no,
                'expired_date': expired_date,
                'zone_name': zone_name,
                'rack_name': rack_name,
                'bin_name': bin_name,
                'location_notes': '',
                'system_quantity': selected_batch_data['quantity'] if selected_batch_data else 0,
                'system_value_usd': selected_batch_data.get('value_usd', 0) if selected_batch_data else 0,
                'actual_quantity': actual_quantity,
                'actual_notes': actual_notes,
                'created_by_user_id': st.session_state.user_id
            }
            
            if add_button:
                # Add to temp counts
                add_temp_count(count_data)
                st.success(f"✅ Count added! Total pending: {len(st.session_state.temp_counts)}")
                
                # Check limit
                if len(st.session_state.temp_counts) >= 20:
                    st.warning("⚠️ Reached 20 counts limit. Please save now.")
                    # Don't auto-save, let user decide
                
                # Set flag to trigger rerun
                st.session_state.form_submitted = True
                st.rerun()
            
            elif save_button:
                # Add current count and save all
                add_temp_count(count_data)
                st.info(f"💾 Saving {len(st.session_state.temp_counts)} counts...")
                save_temp_counts_to_db(transaction_id)
                
        else:
            st.warning("⚠️ Please enter a quantity greater than 0")
    
    # Quick actions
    st.markdown("#### ⚡ Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📜 View Count History", use_container_width=True):
            show_key = f"{product['product_id']}_history"
            st.session_state.show_count_history[show_key] = not st.session_state.show_count_history.get(show_key, False)
    
    with col2:
        if st.button("➕ Add New Item", use_container_width=True):
            show_new_item_form(transaction_id)
    
    with col3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Show count history if toggled
    show_key = f"{product['product_id']}_history"
    if st.session_state.show_count_history.get(show_key, False):
        show_product_count_history(transaction_id, product['product_id'])

def display_batch_details_with_counts(batch_details: List[Dict], count_dict: Dict, transaction_id: int, product_id: int):
    """Display batch details with count status"""
    # Group batches by expiry status
    expired_batches = []
    expiring_soon = []  # < 90 days
    normal_batches = []
    
    today = date.today()
    
    for batch in batch_details:
        try:
            if batch.get('expired_date'):
                exp_date = pd.to_datetime(batch['expired_date']).date()
                if exp_date < today:
                    expired_batches.append(batch)
                elif exp_date < today + timedelta(days=90):
                    expiring_soon.append(batch)
                else:
                    normal_batches.append(batch)
            else:
                normal_batches.append(batch)
        except:
            normal_batches.append(batch)
    
    # Display batches by group
    if expired_batches:
        st.markdown("🔴 **Expired Batches**")
        for batch in expired_batches:
            display_single_batch(batch, count_dict, 'expired')
    
    if expiring_soon:
        st.markdown("🟡 **Expiring Soon (< 90 days)**")
        for batch in expiring_soon:
            display_single_batch(batch, count_dict, 'expiring')
    
    if normal_batches:
        st.markdown("🟢 **Normal Batches**")
        for batch in normal_batches:
            display_single_batch(batch, count_dict, 'normal')

def display_single_batch(batch: Dict, count_dict: Dict, status: str):
    """Display single batch with count information"""
    batch_no = batch.get('batch_no', 'N/A')
    count_info = count_dict.get(batch_no, {})
    
    # Check if in temp counts
    temp_qty = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                   if tc.get('batch_no') == batch_no)
    
    # Create columns
    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 2])
    
    with col1:
        # Batch info with count status
        if temp_qty > 0:
            st.markdown(f"**📝 Batch:** {batch_no}")
        elif count_info.get('total_counted', 0) > 0:
            st.markdown(f"**✅ Batch:** {batch_no}")
        else:
            st.markdown(f"**⭕ Batch:** {batch_no}")
        
        # Expiry date
        exp_date = batch.get('expired_date', 'N/A')
        if status == 'expired':
            st.caption(f"🔴 Expired: {exp_date}")
        elif status == 'expiring':
            st.caption(f"🟡 Expires: {exp_date}")
        else:
            st.caption(f"🟢 Expires: {exp_date}")
    
    with col2:
        st.metric("System", f"{batch.get('quantity', 0):.0f}")
    
    with col3:
        counted = count_info.get('total_counted', 0)
        if counted > 0:
            st.metric("Counted", f"{counted:.0f}")
        else:
            st.metric("Counted", "0")
    
    with col4:
        if temp_qty > 0:
            st.metric("Pending", f"{temp_qty:.0f}", delta=f"+{temp_qty:.0f}")
        else:
            st.metric("Pending", "0")
    
    with col5:
        location = batch.get('location', 'N/A')
        st.write(f"📍 {location}")
        
        # Show variance if counted
        total_qty = count_info.get('total_counted', 0) + temp_qty
        if total_qty > 0:
            variance = total_qty - batch.get('quantity', 0)
            if variance != 0:
                variance_pct = (variance / batch.get('quantity', 1)) * 100
                if variance > 0:
                    st.caption(f"📈 +{variance:.0f} ({variance_pct:+.1f}%)")
                else:
                    st.caption(f"📉 {variance:.0f} ({variance_pct:+.1f}%)")
    
    st.markdown("---")

def show_temp_counts_preview():
    """Show preview of temporary counts"""
    if st.session_state.temp_counts:
        with st.expander(f"📋 Pending Counts ({len(st.session_state.temp_counts)})", expanded=True):
            for i, count in enumerate(st.session_state.temp_counts):
                col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 1, 1])
                
                with col1:
                    st.write(f"**{count['product_name']}**")
                    st.caption(f"PT: {count.get('pt_code', 'N/A')} | Batch: {count['batch_no']}")
                
                with col2:
                    location = f"{count['zone_name']}"
                    if count.get('rack_name'):
                        location += f"-{count['rack_name']}"
                    if count.get('bin_name'):
                        location += f"-{count['bin_name']}"
                    st.write(f"📍 {location}")
                
                with col3:
                    st.metric("Qty", f"{count['actual_quantity']:.0f}")
                
                with col4:
                    variance = count['actual_quantity'] - count.get('system_quantity', 0)
                    if variance > 0:
                        st.success(f"+{variance:.0f}")
                    elif variance < 0:
                        st.error(f"{variance:.0f}")
                    else:
                        st.info("0")
                
                with col5:
                    if st.button("🗑️", key=f"remove_temp_{i}", help="Remove this count"):
                        remove_temp_count(i)
                        st.rerun()
                
                if i < len(st.session_state.temp_counts) - 1:
                    st.markdown("---")

def save_temp_counts_to_db(transaction_id: int = None):
    """Save all temporary counts to database"""
    if not st.session_state.temp_counts:
        st.warning("No counts to save")
        return
    
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    
    try:
        with progress_placeholder.container():
            progress = st.progress(0)
            status_placeholder.text("Saving counts...")
            
            # Use batch save for better performance
            saved, errors = audit_service.save_batch_counts(st.session_state.temp_counts)
            
            progress.progress(100)
        
        if errors:
            st.error(f"⚠️ Saved {saved} counts with {len(errors)} errors:")
            for error in errors[:5]:  # Show first 5 errors
                st.caption(f"• {error}")
            # Wait before clearing
            time.sleep(2)
        else:
            st.success(f"✅ Successfully saved {saved} counts!")
            st.balloons()
            # Wait for balloons
            time.sleep(1)
        
        # Clear temp counts
        clear_temp_counts()
        
        # Clear progress indicators
        progress_placeholder.empty()
        status_placeholder.empty()
        
        # Clear cache
        st.cache_data.clear()
        
        # Rerun to refresh
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Error saving counts: {str(e)}")
        logger.error(f"Batch save error: {e}")
        # Clear progress indicators on error
        progress_placeholder.empty()
        status_placeholder.empty()

def show_counting_progress_enhanced(transaction_id: int):
    """Enhanced counting progress display"""
    try:
        progress = audit_service.get_transaction_progress(transaction_id)
        recent_counts = audit_service.get_recent_counts(transaction_id, limit=5)
        
        # Progress metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Items Counted", progress.get('items_counted', 0))
        
        with col2:
            st.metric("Total Value", f"${progress.get('total_value', 0):,.2f}")
        
        with col3:
            # Include temp counts
            temp_summary = get_temp_counts_summary()
            st.metric("Pending Counts", temp_summary['total'])
        
        with col4:
            tx_info = audit_service.get_transaction_info(transaction_id)
            st.write(f"**Status:** {tx_info.get('status', 'N/A').title()}")
        
        # Recent counts (only saved ones)
        if recent_counts:
            st.markdown("#### 📋 Recent Saved Counts")
            
            for count in recent_counts[:5]:
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 1, 2])
                    
                    with col1:
                        product_name = count.get('product_name', 'New Item')
                        if count.get('is_new_item'):
                            product_name = f"🆕 {count.get('actual_notes', 'New Item')}"
                        st.write(f"**{product_name}**")
                        st.caption(f"Batch: {count.get('batch_no', 'N/A')}")
                    
                    with col2:
                        st.write(f"**Qty:** {count.get('actual_quantity', 0):.0f}")
                        
                        # Variance
                        variance = count.get('actual_quantity', 0) - count.get('system_quantity', 0)
                        if variance != 0:
                            variance_pct = (variance / count.get('system_quantity', 1)) * 100 if count.get('system_quantity', 0) > 0 else 0
                            if variance > 0:
                                st.caption(f"📈 +{variance:.0f} ({variance_pct:+.1f}%)")
                            else:
                                st.caption(f"📉 {variance:.0f} ({variance_pct:+.1f}%)")
                    
                    with col3:
                        location = count.get('zone_name', '')
                        if count.get('rack_name'):
                            location += f"-{count.get('rack_name')}"
                        st.write(f"📍 {location}")
                    
                    with col4:
                        counted_time = count.get('counted_date', 'N/A')
                        if counted_time != 'N/A':
                            try:
                                counted_time = pd.to_datetime(counted_time).strftime('%H:%M')
                            except:
                                pass
                        st.caption(f"⏰ {counted_time}")
                    
                    st.markdown("---")
        
    except Exception as e:
        st.error(f"Error loading progress: {str(e)}")

def show_product_count_history(transaction_id: int, product_id: int):
    """Show detailed count history for a product"""
    with st.container():
        st.markdown("#### 📜 Count History")
        
        try:
            # Get all batches for this product
            tx_info = audit_service.get_transaction_info(transaction_id)
            session_info = audit_service.get_session_info(tx_info['session_id'])
            warehouse_id = session_info['warehouse_id']
            
            batch_details = audit_service.get_product_batch_details(warehouse_id, product_id)
            
            if batch_details:
                for batch in batch_details:
                    batch_no = batch.get('batch_no', 'N/A')
                    
                    # Get count history for this batch
                    history = audit_service.get_batch_count_history(transaction_id, product_id, batch_no)
                    
                    if history:
                        st.markdown(f"**Batch: {batch_no}**")
                        
                        # Create history table
                        history_data = []
                        for h in history:
                            history_data.append({
                                'Time': pd.to_datetime(h['counted_date']).strftime('%m/%d %H:%M'),
                                'Counter': h.get('counter_name', h.get('counted_by', 'Unknown')),
                                'Quantity': f"{h['actual_quantity']:.0f}",
                                'Location': h.get('location', 'N/A'),
                                'Notes': h.get('actual_notes', '')
                            })
                        
                        df = pd.DataFrame(history_data)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                        
                        st.markdown("---")
            else:
                st.info("No count history available")
                
        except Exception as e:
            st.error(f"Error loading count history: {str(e)}")

def show_new_item_form(transaction_id: int):
    """Form for adding new items not in system"""
    with st.expander("➕ Add New Item Not in System", expanded=True):
        with st.form("new_item_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                product_name = st.text_input("Product Name*")
                brand = st.text_input("Brand")
                batch_no = st.text_input("Batch Number")
            
            with col2:
                actual_quantity = st.number_input("Quantity*", min_value=0.0, step=1.0)
                expired_date = st.date_input("Expiry Date")
                location_notes = st.text_area("Location & Notes")
            
            submit = st.form_submit_button("💾 Add New Item")
            
            if submit and product_name and actual_quantity > 0:
                try:
                    # Add to temp counts
                    count_data = {
                        'transaction_id': transaction_id,
                        'product_id': None,
                        'product_name': f"NEW: {product_name}",
                        'batch_no': batch_no,
                        'expired_date': expired_date,
                        'zone_name': '',
                        'rack_name': '',
                        'bin_name': '',
                        'location_notes': location_notes,
                        'system_quantity': 0,
                        'system_value_usd': 0,
                        'actual_quantity': actual_quantity,
                        'actual_notes': f"NEW ITEM: {product_name} - {brand}",
                        'is_new_item': True,
                        'created_by_user_id': st.session_state.user_id
                    }
                    
                    add_temp_count(count_data)
                    st.success(f"✅ New item added to pending counts!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error adding new item: {str(e)}")

# ============== ADDITIONAL PAGES ==============

def user_management_page():
    """Admin-only user management page"""
    st.subheader("👥 User Management")
    st.info("🚧 User management features coming soon...")
    
    st.markdown("""
    **Planned Features:**
    - View all users and their roles
    - Assign users to specific audit sessions
    - Manage audit permissions
    - User activity tracking
    """)

def team_overview_page():
    """Sales manager team overview"""
    st.subheader("👥 Team Overview")
    
    try:
        user_stats = audit_service.get_user_activity_stats()
        
        if user_stats:
            st.markdown("#### 📊 Team Activity")
            
            for user in user_stats[:10]:
                col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                
                with col1:
                    st.write(f"**{user.get('full_name', user.get('username', 'Unknown'))}**")
                
                with col2:
                    st.metric("Transactions", user.get('transactions_created', 0))
                
                with col3:
                    st.metric("Items Counted", user.get('items_counted', 0))
                
                with col4:
                    last_activity = user.get('last_activity', 'Never')
                    if last_activity != 'Never':
                        last_activity = pd.to_datetime(last_activity).strftime('%m/%d %H:%M')
                    st.write(f"Last: {last_activity}")
                
                st.markdown("---")
        else:
            st.info("No team activity data available")
            
    except Exception as e:
        st.error(f"Error loading team overview: {str(e)}")

def view_assigned_sessions_page():
    """View sessions assigned to user"""
    st.subheader("📋 My Assigned Sessions")
    
    try:
        all_sessions = audit_service.get_all_sessions(limit=20)
        
        if all_sessions:
            st.markdown("#### 📊 Available Sessions")
            
            for session in all_sessions:
                with st.container():
                    col1, col2, col3 = st.columns([4, 3, 3])
                    
                    with col1:
                        st.write(f"**{session['session_name']}**")
                        st.caption(f"Code: {session['session_code']}")
                    
                    with col2:
                        st.write(f"Status: {session['status'].title()}")
                        st.caption(f"Warehouse: {session.get('warehouse_name', 'N/A')}")
                    
                    with col3:
                        st.write(f"Created: {session.get('created_date', 'N/A')}")
                        if session.get('actual_start_date'):
                            st.caption(f"Started: {session.get('actual_start_date')}")
                    
                    st.markdown("---")
        else:
            st.info("No sessions assigned to you")
            
    except Exception as e:
        st.error(f"Error loading assigned sessions: {str(e)}")

def view_own_reports_page():
    """View own audit reports"""
    st.subheader("📈 My Audit Reports")
    
    try:
        user_transactions = audit_service.get_user_transactions_all(st.session_state.user_id)
        
        if user_transactions:
            st.markdown("#### 📊 My Audit Activity")
            
            total_transactions = len(user_transactions)
            completed_transactions = len([t for t in user_transactions if t.get('status') == 'completed'])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Transactions", total_transactions)
            
            with col2:
                st.metric("Completed", completed_transactions)
            
            with col3:
                completion_rate = (completed_transactions / total_transactions * 100) if total_transactions > 0 else 0
                st.metric("Completion Rate", f"{completion_rate:.1f}%")
            
            st.markdown("#### 📋 Transaction History")
            
            for tx in user_transactions:
                col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                
                with col1:
                    st.write(f"**{tx.get('transaction_name', 'N/A')}**")
                    st.caption(f"Session: {tx.get('session_name', 'N/A')}")
                
                with col2:
                    st.write(f"Status: {tx.get('status', 'N/A').title()}")
                
                with col3:
                    st.write(f"Items: {tx.get('total_items_counted', 0)}")
                
                with col4:
                    st.write(f"Created: {tx.get('created_date', 'N/A')}")
                
                st.markdown("---")
        else:
            st.info("No audit activity found")
            
    except Exception as e:
        st.error(f"Error loading your reports: {str(e)}")

def reports_page():
    """Enhanced reports page with role-based access"""
    st.subheader("📈 Audit Reports")
    
    user_role = st.session_state.user_role
    
    if check_permission('export_data'):
        st.markdown("#### 📊 System Reports")
        
        try:
            sessions = audit_service.get_all_sessions(limit=50)
            
            if sessions:
                session_options = {
                    f"{s['session_name']} ({s['session_code']})": s['id']
                    for s in sessions
                }
                
                selected_session_key = st.selectbox("Select Session for Report", session_options.keys())
                selected_session_id = session_options[selected_session_key]
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("📊 Generate Summary", use_container_width=True):
                        generate_summary_report(selected_session_id)
                
                with col2:
                    if st.button("📥 Export to Excel", use_container_width=True):
                        export_detailed_data(selected_session_id)
                
                with col3:
                    if st.button("📈 Variance Analysis", use_container_width=True):
                        show_variance_analysis(selected_session_id)
                
            else:
                st.info("No sessions available for reporting")
                
        except Exception as e:
            st.error(f"Error loading reports: {str(e)}")
    
    elif check_permission('view_own'):
        st.markdown("#### 📋 Your Activity Report")
        view_own_reports_page()
    
    else:
        st.warning("⚠️ You don't have permission to view reports")

def generate_summary_report(session_id: int):
    """Generate summary report for session"""
    try:
        summary = audit_service.get_audit_summary(session_id)
        
        if summary:
            session_info = summary['session_info']
            progress = summary['progress']
            variance_summary = summary['variance_summary']
            
            st.markdown("#### 📋 Session Summary")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Session:** {session_info.get('session_name')}")
                st.markdown(f"**Code:** {session_info.get('session_code')}")
                st.markdown(f"**Warehouse:** {session_info.get('warehouse_name')}")
                st.markdown(f"**Status:** {session_info.get('status', '').title()}")
            
            with col2:
                st.markdown(f"**Created:** {session_info.get('created_date')}")
                st.markdown(f"**Started:** {session_info.get('actual_start_date', 'Not started')}")
                st.markdown(f"**Completed:** {session_info.get('actual_end_date', 'Not completed')}")
            
            st.markdown("#### 📊 Progress Metrics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Transactions", progress.get('total_transactions', 0))
            
            with col2:
                st.metric("Completed", progress.get('completed_transactions', 0))
            
            with col3:
                st.metric("Completion Rate", f"{progress.get('completion_rate', 0):.1f}%")
            
            with col4:
                st.metric("Items Counted", progress.get('total_items', 0))
            
            st.markdown("#### 📈 Variance Summary")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Variance USD", f"${variance_summary['total_variance_usd']:,.2f}")
            
            with col2:
                st.metric("Items with Variance", variance_summary['items_with_variance'])
            
            # Top variance items
            if variance_summary.get('variance_items'):
                st.markdown("**Top Variance Items:**")
                
                for item in variance_summary['variance_items'][:5]:
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                    
                    with col1:
                        st.write(f"{item.get('product_name', 'N/A')}")
                        st.caption(f"PT: {item.get('pt_code', 'N/A')}")
                    
                    with col2:
                        st.write(f"Batch: {item.get('batch_no', 'N/A')}")
                    
                    with col3:
                        variance_qty = item.get('variance_quantity', 0)
                        if variance_qty > 0:
                            st.success(f"+{variance_qty:.0f}")
                        else:
                            st.error(f"{variance_qty:.0f}")
                    
                    with col4:
                        variance_pct = item.get('variance_percentage', 0)
                        st.write(f"{variance_pct:+.1f}%")
                    
                    st.markdown("---")
        
    except Exception as e:
        st.error(f"Error generating summary: {str(e)}")

def export_detailed_data(session_id: int):
    """Export detailed session data"""
    try:
        with st.spinner("Preparing export..."):
            file_path = audit_service.export_session_to_excel(session_id)
            
            # Read file for download
            with open(file_path, 'rb') as f:
                data = f.read()
            
            st.download_button(
                label="📥 Download Excel File",
                data=data,
                file_name=file_path,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.success("✅ Export ready for download!")
            
    except Exception as e:
        st.error(f"Error exporting data: {str(e)}")

def show_variance_analysis(session_id: int):
    """Show detailed variance analysis"""
    try:
        variance_data = audit_service.get_variance_analysis(session_id)
        
        if variance_data:
            st.markdown("#### 📊 Variance Analysis")
            
            # Create DataFrame
            df = pd.DataFrame(variance_data)
            
            # Summary metrics
            total_items = len(df)
            positive_variance = len(df[df['variance_quantity'] > 0])
            negative_variance = len(df[df['variance_quantity'] < 0])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Items with Variance", total_items)
            
            with col2:
                st.metric("Over Count", positive_variance)
            
            with col3:
                st.metric("Under Count", negative_variance)
            
            # Display table
            st.dataframe(
                df[['product_name', 'pt_code', 'batch_no', 'system_quantity', 
                    'actual_quantity', 'variance_quantity', 'variance_percentage']],
                use_container_width=True
            )
            
            # Download option
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Variance Report CSV",
                data=csv,
                file_name=f"variance_analysis_{session_id}.csv",
                mime="text/csv"
            )
        else:
            st.info("No variance data available")
            
    except Exception as e:
        st.error(f"Error loading variance analysis: {str(e)}")

def view_sessions_page():
    """View-only sessions page"""
    st.subheader("👀 Audit Sessions View")
    
    try:
        all_sessions = audit_service.get_all_sessions(limit=20)
        
        if all_sessions:
            for session in all_sessions:
                with st.container():
                    col1, col2, col3 = st.columns([4, 3, 3])
                    
                    with col1:
                        st.write(f"**{session['session_name']}**")
                        st.caption(f"Code: {session['session_code']}")
                    
                    with col2:
                        st.write(f"Status: {session['status'].title()}")
                        st.caption(f"Warehouse: {session.get('warehouse_name', 'N/A')}")
                    
                    with col3:
                        st.write(f"Created: {session.get('created_date', 'N/A')}")
                        if session.get('actual_start_date'):
                            st.caption(f"Started: {session.get('actual_start_date')}")
                    
                    st.markdown("---")
        else:
            st.info("No sessions found")
            
    except Exception as e:
        st.error(f"Error loading sessions: {str(e)}")

if __name__ == "__main__":
    main()
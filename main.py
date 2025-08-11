# main.py - Warehouse Audit System
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
from typing import Dict, List, Optional

# Import existing utilities
from utils.auth import AuthManager
from utils.config import config
from utils.db import get_db_engine

# Import our services
from audit_service import AuditService
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
    """Form to create new audit session"""
    
    # Warehouse selection outside form for real-time updates
    st.markdown("#### 🏢 Select Warehouse")
    warehouses = audit_service.get_warehouses()
    
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
                    # Clear form and refresh to show new session in Draft Sessions
                    st.session_state.pop('warehouse_selection', None)
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
    """Display user's transactions for session"""
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
                                if audit_service.submit_transaction(tx['id'], st.session_state.user_id):
                                    st.success("Transaction submitted!")
                                    st.rerun()
                    
                    st.markdown("---")
        else:
            st.info("No transactions created yet")
            
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")

def counting_page():
    """Counting interface page"""
    st.subheader("🔢 Inventory Counting")
    
    # Select draft transaction
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
            # Product search and counting
            product_search_and_count(selected_tx_id)
            
            # Progress display
            st.subheader("📊 Progress")
            show_counting_progress(selected_tx_id)
            
    except Exception as e:
        st.error(f"Error in counting page: {str(e)}")

def product_search_and_count(transaction_id: int):
    """Enhanced product search and counting interface with auto-filtering"""
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
        
        # Brand filter with auto-update
        brands = audit_service.get_warehouse_brands(warehouse_id)
        brand_options = ["All Brands"] + [brand['brand'] for brand in brands if brand['brand']]
        selected_brand = st.selectbox(
            "Filter by Brand", 
            brand_options, 
            key="brand_filter_auto"
        )
        brand_filter = "" if selected_brand == "All Brands" else selected_brand
        
        # === PRODUCT SELECTION (Moved up to replace search) ===
        st.markdown("##### 📦 Select Product")
        
        # Load and filter products automatically based on current brand filter
        try:
            if brand_filter:
                # Filter by brand only
                products = audit_service.search_products_with_filters(
                    warehouse_id, "", brand_filter
                )
            else:
                # Load all products
                products = audit_service.get_warehouse_products(warehouse_id)
        
            if products:
                # Create product options for searchable selectbox
                product_options = ["-- Type to search or select product --"]
                product_data = {}
                
                for p in products:
                    pt_code = p.get('pt_code', 'N/A')
                    product_name = p.get('product_name', 'Unknown')
                    brand = p.get('brand', 'N/A')
                    total_qty = p.get('total_quantity', 0)
                    
                    # Format: "PT001 - Product Name [Brand] (Qty: 100)"
                    display_text = f"{pt_code} - {product_name[:50]}{'...' if len(product_name) > 50 else ''} [{brand}] (Qty: {total_qty:.0f})"
                    product_options.append(display_text)
                    product_data[display_text] = p
                
                # Searchable product selection (Streamlit selectbox is searchable by default)
                selected_product_display = st.selectbox(
                    f"Choose Product ({len(products)} available)",
                    product_options,
                    key=f"product_selector_{warehouse_id}_{len(products)}",  # Dynamic key for refresh
                    help="🔍 Type to search by PT code or product name, or scroll to browse all products"
                )
                
                # Show product details if selected
                if selected_product_display != "-- Type to search or select product --":
                    selected_product = product_data[selected_product_display]
                    show_enhanced_product_count_form(transaction_id, selected_product, warehouse_id)
                
                # Show filter info
                if brand_filter:
                    st.caption(f"🔍 Filtered by brand: {brand_filter}")
                
            else:
                # No products found with current filters
                if brand_filter:
                    st.warning(f"🔍 No products found for brand '{brand_filter}' in this warehouse")
                    
                    # Show suggestion to clear filter
                    if st.button("🗑️ Clear Brand Filter"):
                        st.session_state.brand_filter_auto = "All Brands"
                        st.rerun()
                else:
                    st.warning("⚠️ No products available in this warehouse")
            
            # === QUICK ACTIONS ===
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # Clear brand filter if active
                if brand_filter and st.button("🗑️ Clear Brand Filter"):
                    st.session_state.brand_filter_auto = "All Brands"
                    st.rerun()
            
            with col2:
                # Add new item
                if st.button("➕ Add New Item Not in System"):
                    show_new_item_form(transaction_id)
                    
        except Exception as e:
            st.error(f"Error loading products: {str(e)}")
            logger.error(f"Product loading error: {e}")
    
    except Exception as e:
        st.error(f"Error in product search: {str(e)}")
        logger.error(f"Product search error: {e}")

def show_enhanced_product_count_form(transaction_id: int, product: Dict, warehouse_id: int):
    """Enhanced product details and counting form"""
    st.markdown("---")
    st.markdown("#### 📦 Product Details & Counting")
    
    # Product information display
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📋 Product Information**")
            st.write(f"**Product:** {product.get('product_name', 'N/A')}")
            st.write(f"**PT Code:** {product.get('pt_code', 'N/A')}")
            st.write(f"**Legacy Code:** {product.get('legacy_code', 'N/A')}")
            st.write(f"**Brand:** {product.get('brand', 'N/A')}")
            st.write(f"**Package Size:** {product.get('package_size', 'N/A')}")
            st.write(f"**UOM:** {product.get('standard_uom', 'N/A')}")
        
        with col2:
            st.markdown("**📊 System Information**")
            st.write(f"**Total Batches:** {product.get('total_batches', 0)}")
            st.write(f"**Total System Qty:** {product.get('total_quantity', 0):.2f}")
            
            # Get specific batch info if available
            try:
                system_inventory = audit_service.get_product_system_inventory(
                    transaction_id, product['product_id']
                )
                
                if system_inventory:
                    st.write(f"**Latest Batch:** {system_inventory.get('batch_no', 'N/A')}")
                    st.write(f"**Expiry Date:** {system_inventory.get('expired_date', 'N/A')}")
                    st.write(f"**Location:** {system_inventory.get('location', 'N/A')}")
                else:
                    st.caption("ℹ️ No specific batch details available")
            except Exception as e:
                st.caption("ℹ️ Could not load detailed batch info")
    
    # Counting form
    with st.form(f"enhanced_count_form_{product['product_id']}"):
        st.markdown("#### ✏️ Record Your Count")
        
        col1, col2 = st.columns(2)
        
        with col1:
            batch_no = st.text_input("Batch Number", 
                placeholder="Enter batch number found on product")
            
            actual_quantity = st.number_input(
                "Actual Quantity Counted*", 
                min_value=0.0, 
                step=1.0, 
                format="%.2f",
                help="Enter the exact quantity you counted"
            )
        
        with col2:
            expired_date = st.date_input("Expiry Date",
                help="Expiry date found on the product")
            
            # Location method selection
            location_method = st.radio(
                "Location Entry:",
                ["Quick Location", "Detailed"],
                horizontal=True,
                help="Quick: A1-R01-B01 format, Detailed: separate fields"
            )
        
        # Location input based on method
        if location_method == "Quick Location":
            quick_location = st.text_input("Location", 
                placeholder="e.g., A1-R01-B01 or Cold Storage Area")
            
            # Parse quick location
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
            # Detailed location entry
            col1, col2, col3 = st.columns(3)
            with col1:
                zone_name = st.text_input("Zone", placeholder="e.g., A1")
            with col2:
                rack_name = st.text_input("Rack", placeholder="e.g., R01")
            with col3:
                bin_name = st.text_input("Bin", placeholder="e.g., B01")
        
        # Additional notes
        col1, col2 = st.columns(2)
        
        with col1:
            location_notes = st.text_area("Location Notes", 
                placeholder="Additional location details...",
                height=80)
        
        with col2:
            actual_notes = st.text_area("Count Notes", 
                placeholder="Observations about items (damage, expiry, etc.)",
                height=80)
        
        # Submit button with better styling
        submit = st.form_submit_button("💾 Save Count", use_container_width=True, type="primary")
        
        if submit:
            if actual_quantity >= 0:
                try:
                    # Show loading spinner
                    with st.spinner("Saving count..."):
                        audit_service.save_count_detail({
                            'transaction_id': transaction_id,
                            'product_id': product['product_id'],
                            'batch_no': batch_no,
                            'expired_date': expired_date,
                            'zone_name': zone_name,
                            'rack_name': rack_name,
                            'bin_name': bin_name,
                            'location_notes': location_notes,
                            'system_quantity': product.get('total_quantity', 0),
                            'system_value_usd': 0,  # Will be calculated later
                            'actual_quantity': actual_quantity,
                            'actual_notes': actual_notes,
                            'created_by_user_id': st.session_state.user_id
                        })
                    
                    st.success("✅ Count saved successfully!")
                    st.balloons()
                    
                    # Auto-clear form by rerunning
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error saving count: {str(e)}")
            else:
                st.warning("⚠️ Please enter a valid quantity (0 or positive number)")

def show_product_count_form(transaction_id: int, product: Dict):
    """Show product details and counting form"""
    st.markdown("#### 📦 Product Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Product:** {product['product_name']}")
        st.write(f"**PT Code:** {product['pt_code']}")
        st.write(f"**Brand:** {product['brand']}")
        st.write(f"**Package:** {product['package_size']}")
    
    with col2:
        # Get system inventory for this product
        system_inventory = audit_service.get_product_system_inventory(
            transaction_id, product['product_id']
        )
        
        if system_inventory:
            st.write(f"**System Qty:** {system_inventory.get('quantity', 0)}")
            st.write(f"**Location:** {system_inventory.get('location', 'N/A')}")
            st.write(f"**Batch:** {system_inventory.get('batch_no', 'N/A')}")
            st.write(f"**Expiry:** {system_inventory.get('expired_date', 'N/A')}")
    
    # Counting form
    with st.form("count_form"):
        st.markdown("#### ✏️ Record Count")
        
        col1, col2 = st.columns(2)
        
        with col1:
            batch_no = st.text_input("Batch Number", 
                value=system_inventory.get('batch_no', '') if system_inventory else '')
            expired_date = st.date_input("Expiry Date",
                value=system_inventory.get('expired_date') if system_inventory else None)
        
        with col2:
            actual_quantity = st.number_input("Actual Quantity*", 
                min_value=0.0, step=1.0, format="%.2f")
        
        # Location details
        st.markdown("##### 📍 Location Details")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            zone_name = st.text_input("Zone", 
                value=system_inventory.get('zone_name', '') if system_inventory else '')
        with col2:
            rack_name = st.text_input("Rack",
                value=system_inventory.get('rack_name', '') if system_inventory else '')
        with col3:
            bin_name = st.text_input("Bin",
                value=system_inventory.get('bin_name', '') if system_inventory else '')
        
        location_notes = st.text_area("Location Notes", 
            placeholder="Additional location details or observations")
        actual_notes = st.text_area("Count Notes", 
            placeholder="Any observations about the counted items")
        
        submit = st.form_submit_button("💾 Save Count", use_container_width=True)
        
        if submit:
            try:
                audit_service.save_count_detail({
                    'transaction_id': transaction_id,
                    'product_id': product['product_id'],
                    'batch_no': batch_no,
                    'expired_date': expired_date,
                    'zone_name': zone_name,
                    'rack_name': rack_name,
                    'bin_name': bin_name,
                    'location_notes': location_notes,
                    'system_quantity': system_inventory.get('quantity', 0) if system_inventory else 0,
                    'system_value_usd': system_inventory.get('value_usd', 0) if system_inventory else 0,
                    'actual_quantity': actual_quantity,
                    'actual_notes': actual_notes,
                    'created_by_user_id': st.session_state.user_id
                })
                
                st.success("✅ Count saved successfully!")
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error saving count: {str(e)}")

def show_new_item_form(transaction_id: int):
    """Form for adding new items not in system"""
    st.markdown("#### ➕ Add New Item")
    
    with st.form("new_item_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            product_name = st.text_input("Product Name*")
            batch_no = st.text_input("Batch Number")
            actual_quantity = st.number_input("Quantity*", min_value=0.0, step=1.0)
        
        with col2:
            brand = st.text_input("Brand")
            expired_date = st.date_input("Expiry Date")
            location_notes = st.text_area("Location & Notes")
        
        submit = st.form_submit_button("💾 Add New Item")
        
        if submit and product_name and actual_quantity > 0:
            try:
                audit_service.save_count_detail({
                    'transaction_id': transaction_id,
                    'product_id': None,  # Will be handled as new item
                    'batch_no': batch_no,
                    'expired_date': expired_date,
                    'location_notes': location_notes,
                    'actual_quantity': actual_quantity,
                    'actual_notes': f"NEW ITEM: {product_name} - {brand}",
                    'is_new_item': True,
                    'created_by_user_id': st.session_state.user_id
                })
                
                st.success("✅ New item added successfully!")
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error adding new item: {str(e)}")

def show_counting_progress(transaction_id: int):
    """Display enhanced counting progress for transaction"""
    try:
        progress = audit_service.get_transaction_progress(transaction_id)
        recent_counts = audit_service.get_recent_counts(transaction_id, limit=10)
        
        # Progress metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Items Counted", progress.get('items_counted', 0))
            st.metric("Total Value", f"${progress.get('total_value', 0):,.2f}")
        
        with col2:
            # Transaction info
            tx_info = audit_service.get_transaction_info(transaction_id)
            if tx_info:
                st.write(f"**Transaction:** {tx_info.get('transaction_name', 'N/A')}")
                st.write(f"**Status:** {tx_info.get('status', 'N/A').title()}")
                st.write(f"**Created:** {tx_info.get('created_date', 'N/A')}")
        
        # Recent counts
        if recent_counts:
            st.markdown("#### 📋 Recent Counts")
            
            # Show in a nice table format
            for i, count in enumerate(recent_counts):
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 1, 2])
                    
                    with col1:
                        product_name = count.get('product_name', 'New Item')
                        if count.get('is_new_item'):
                            product_name = f"🆕 {count.get('actual_notes', 'New Item')}"
                        st.write(f"**{product_name}**")
                        if count.get('pt_code'):
                            st.caption(f"PT: {count.get('pt_code')} | Brand: {count.get('brand_name', 'N/A')}")
                    
                    with col2:
                        st.write(f"**Qty:** {count.get('actual_quantity', 0):.2f}")
                        if count.get('batch_no'):
                            st.caption(f"Batch: {count.get('batch_no')}")
                    
                    with col3:
                        if count.get('zone_name'):
                            location = count.get('zone_name', '')
                            if count.get('rack_name'):
                                location += f"-{count.get('rack_name')}"
                            if count.get('bin_name'):
                                location += f"-{count.get('bin_name')}"
                            st.write(f"📍 {location}")
                    
                    with col4:
                        counted_time = count.get('counted_date', 'N/A')
                        if counted_time != 'N/A':
                            try:
                                import pandas as pd
                                counted_time = pd.to_datetime(counted_time).strftime('%H:%M')
                            except:
                                pass
                        st.caption(f"⏰ {counted_time}")
                    
                    # Variance indicator
                    system_qty = count.get('system_quantity', 0)
                    actual_qty = count.get('actual_quantity', 0)
                    if system_qty > 0:
                        variance = actual_qty - system_qty
                        if variance != 0:
                            variance_pct = (variance / system_qty) * 100
                            if abs(variance_pct) > 5:  # Show significant variances
                                if variance > 0:
                                    st.success(f"📈 +{variance:.1f} ({variance_pct:+.1f}%)")
                                else:
                                    st.error(f"📉 {variance:.1f} ({variance_pct:+.1f}%)")
                    
                    if i < len(recent_counts) - 1:
                        st.markdown("---")
        
        # Quick actions
        st.markdown("#### ⚡ Quick Actions")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Refresh Progress", use_container_width=True):
                st.rerun()
        
        with col2:
            if st.button("📊 View All Counts", use_container_width=True):
                show_all_transaction_counts(transaction_id)
        
        with col3:
            # Submit transaction if user is ready
            tx_info = audit_service.get_transaction_info(transaction_id)
            if tx_info and tx_info.get('status') == 'draft':
                if st.button("✅ Submit Transaction", use_container_width=True):
                    if progress.get('items_counted', 0) > 0:
                        try:
                            if audit_service.submit_transaction(transaction_id, st.session_state.user_id):
                                st.success("🎉 Transaction submitted successfully!")
                                st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error submitting transaction: {str(e)}")
                    else:
                        st.warning("⚠️ Please count at least one item before submitting")
        
    except Exception as e:
        st.warning(f"Could not load progress: {str(e)}")

def show_all_transaction_counts(transaction_id: int):
    """Show all counts for transaction in expandable section"""
    with st.expander("📊 View All Transaction Counts", expanded=False):
        try:
            # Get all counts for transaction
            all_counts = audit_service.get_recent_counts(transaction_id, limit=1000)
            
            if all_counts:
                # Create DataFrame for better display
                import pandas as pd
                
                df_data = []
                for count in all_counts:
                    df_data.append({
                        'Product': count.get('product_name', 'New Item'),
                        'PT Code': count.get('pt_code', 'N/A'),
                        'Brand': count.get('brand_name', 'N/A'),
                        'Batch': count.get('batch_no', 'N/A'),
                        'Actual Qty': count.get('actual_quantity', 0),
                        'System Qty': count.get('system_quantity', 0),
                        'Variance': count.get('actual_quantity', 0) - count.get('system_quantity', 0),
                        'Location': f"{count.get('zone_name', '')}-{count.get('rack_name', '')}-{count.get('bin_name', '')}".strip('-'),
                        'Counted Time': count.get('counted_date', 'N/A')
                    })
                
                df = pd.DataFrame(df_data)
                st.dataframe(df, use_container_width=True)
                
                # Export option
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Counts CSV",
                    data=csv,
                    file_name=f"transaction_counts_{transaction_id}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No counts recorded yet")
                
        except Exception as e:
            st.error(f"Error loading all counts: {str(e)}")

# ============== NEW ROLE-SPECIFIC PAGES ==============

def user_management_page():
    """Admin-only user management page"""
    st.subheader("👥 User Management")
    st.info("🚧 User management features coming soon...")
    
    # Preview of user management features
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
        # Get team activity stats
        user_stats = audit_service.get_user_activity_stats()
        
        if user_stats:
            st.markdown("#### 📊 Team Activity")
            
            # Display team stats
            for user in user_stats[:10]:  # Top 10 active users
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
        # For now, show all sessions user can view
        # TODO: Implement session assignment logic
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
        # Get user's transactions
        user_transactions = audit_service.get_user_transactions_all(st.session_state.user_id)
        
        if user_transactions:
            st.markdown("#### 📊 My Audit Activity")
            
            # Summary metrics
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
            
            # Transaction list
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

# ============== ENHANCED REPORTS PAGE ==============

def reports_page():
    """Enhanced reports page with role-based access"""
    st.subheader("📈 Audit Reports")
    
    user_role = st.session_state.user_role
    
    # Different report access based on role
    if check_permission('export_data'):
        # Full report access for admin/management
        st.markdown("#### 📊 System Reports")
        
        try:
            # Session selector for reports
            sessions = audit_service.get_all_sessions(limit=50)
            
            if sessions:
                session_options = {
                    f"{s['session_name']} ({s['session_code']})": s['id']
                    for s in sessions
                }
                
                selected_session_key = st.selectbox("Select Session for Report", session_options.keys())
                selected_session_id = session_options[selected_session_key]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📊 Generate Summary Report"):
                        generate_summary_report(selected_session_id)
                
                with col2:
                    if st.button("📥 Export Detailed Data"):
                        export_detailed_data(selected_session_id)
                
            else:
                st.info("No sessions available for reporting")
                
        except Exception as e:
            st.error(f"Error loading reports: {str(e)}")
    
    elif check_permission('view_own'):
        # Limited reports for regular users
        st.markdown("#### 📋 Your Activity Report")
        view_own_reports_page()
    
    else:
        st.warning("⚠️ You don't have permission to view reports")

def generate_summary_report(session_id: int):
    """Generate summary report for session"""
    try:
        # Get session info and progress
        session_info = audit_service.get_session_info(session_id)
        session_progress = audit_service.get_session_progress(session_id)
        
        if session_info:
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
            
            # Progress metrics
            st.markdown("#### 📊 Progress Metrics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Transactions", session_progress.get('total_transactions', 0))
            
            with col2:
                st.metric("Completed", session_progress.get('completed_transactions', 0))
            
            with col3:
                st.metric("Completion Rate", f"{session_progress.get('completion_rate', 0):.1f}%")
            
            with col4:
                st.metric("Items Counted", session_progress.get('total_items', 0))
        
    except Exception as e:
        st.error(f"Error generating summary report: {str(e)}")

def export_detailed_data(session_id: int):
    """Export detailed session data"""
    try:
        # Export session data
        report_data = audit_service.get_session_report_data(session_id)
        
        if report_data:
            df = pd.DataFrame(report_data)
            
            # Display summary
            st.subheader("📊 Export Preview")
            st.write(f"Total Records: {len(df)}")
            
            # Show preview
            st.dataframe(df.head(10), use_container_width=True)
            
            # Download link
            csv = df.to_csv(index=False)
            session_code = df.iloc[0]['session_code'] if len(df) > 0 else 'unknown'
            
            st.download_button(
                label="📥 Download Full CSV",
                data=csv,
                file_name=f"audit_export_{session_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No data available for export")
            
    except Exception as e:
        st.error(f"Error exporting data: {str(e)}")

def view_sessions_page():
    """View-only sessions page"""
    st.subheader("👀 Audit Sessions View")
    
    try:
        # Show recent sessions
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
# main.py - Simplified Warehouse Audit System Entry Point
import streamlit as st
import logging
from datetime import datetime

# Import authentication
from utils.auth import AuthManager
from utils.config import config

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

# Role permissions configuration
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
            show_main_menu()
    except Exception as e:
        st.error(f"Application error: {str(e)}")
        logger.error(f"Main app error: {e}")

def show_login_page():
    """Display login page"""
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
                    success, result = auth.authenticate(username, password)
                    
                    if success:
                        auth.login(result)
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error(f"❌ {result.get('error', 'Login failed')}")
                else:
                    st.warning("⚠️ Please enter both username and password")
        
        # Info box
        with st.expander("ℹ️ System Information"):
            st.markdown("""
            **Warehouse Audit System** helps you:
            - 📋 Manage audit sessions
            - 📦 Count inventory items
            - 📊 Track discrepancies
            - 📈 Generate reports
            
            Contact your administrator for access.
            """)

def show_main_menu():
    """Display main menu after login"""
    # Sidebar with user info
    with st.sidebar:
        st.markdown("### 👤 User Info")
        st.write(f"**Name:** {auth.get_user_display_name()}")
        st.write(f"**Role:** {st.session_state.user_role}")
        st.write(f"**Login:** {st.session_state.login_time.strftime('%H:%M')}")
        
        # Show permissions
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
    
    # Main content area
    st.title("📦 Warehouse Audit System")
    
    # Check if user has any audit permissions
    user_role = st.session_state.user_role
    if not AUDIT_ROLES.get(user_role, []):
        show_no_access_page()
    else:
        show_navigation_menu()

def show_no_access_page():
    """Show page for users without access"""
    st.warning("⚠️ You don't have permission to access the Audit System")
    
    st.markdown("""
    ### Contact Administrator
    
    If you need access to the Warehouse Audit System, please contact:
    - **System Administrator** for role permissions
    - **Supply Chain Manager** for audit participation
    
    ### Available Roles:
    - **Executive (Admin/GM/MD)**: Full system access
    - **Management (Supply Chain/Sales Manager)**: Session management
    - **Operational (Sales)**: Audit participation
    - **Viewer**: Read-only access
    """)

def show_navigation_menu():
    """Show navigation menu based on user permissions"""
    st.markdown("### 🚀 Quick Navigation")
    
    col1, col2, col3 = st.columns(3)
    
    # Audit Management - for users with any audit permissions
    with col1:
        if check_permission('manage_sessions') or check_permission('create_transactions') or check_permission('view_all'):
            if st.button("📋 Audit Management", use_container_width=True, type="primary"):
                st.switch_page("pages/audit_management.py")
            st.caption("Manage sessions & transactions")
    
    # Counting Page - for users who can create transactions
    with col2:
        if check_permission('create_transactions'):
            if st.button("📦 Inventory Counting", use_container_width=True):
                st.switch_page("pages/counting.py")
            st.caption("Count inventory items")
    
    # Reports - for users with view or export permissions
    with col3:
        if check_permission('view_all') or check_permission('export_data') or check_permission('view_own'):
            if st.button("📊 Reports & Analytics", use_container_width=True):
                st.switch_page("pages/reports.py")
            st.caption("View reports & export data")
    
    # Additional info based on role
    st.markdown("---")
    st.markdown("### 📌 Getting Started")
    
    user_role = st.session_state.user_role
    
    if user_role in ['admin', 'GM', 'MD', 'supply_chain', 'sales_manager']:
        st.info("""
        **As a Manager/Admin:**
        1. Go to **Audit Management** to create and manage audit sessions
        2. Monitor ongoing audits and team progress
        3. Export reports for analysis
        """)
    elif user_role == 'sales':
        st.info("""
        **As an Auditor:**
        1. Go to **Audit Management** to view active sessions
        2. Create transactions for your counting tasks
        3. Use **Inventory Counting** to record counts
        """)
    else:
        st.info("""
        **As a Viewer:**
        - View audit sessions and reports
        - Monitor audit progress
        - Export data for analysis
        """)
    
    # System stats (if user can view all)
    if check_permission('view_all'):
        show_system_stats()

def show_system_stats():
    """Show basic system statistics"""
    st.markdown("### 📈 System Overview")
    
    try:
        from audit_service import AuditService
        audit_service = AuditService()
        
        stats = audit_service.get_dashboard_stats()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Sessions", stats.get('active_sessions', 0))
        with col2:
            st.metric("Draft Sessions", stats.get('draft_sessions', 0))
        with col3:
            st.metric("Completed Today", stats.get('completed_today', 0))
        with col4:
            st.metric("Active Users", stats.get('active_users', 0))
    except Exception as e:
        logger.error(f"Error loading stats: {e}")
        st.info("Unable to load system statistics")

if __name__ == "__main__":
    main()
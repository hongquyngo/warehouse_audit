# pages/audit_management.py - Audit Session & Transaction Management (No Authentication)
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging

# Import services
from audit_service import AuditService, AuditException

# Setup logging
logger = logging.getLogger(__name__)

# Initialize services
audit_service = AuditService()

# Page config
st.set_page_config(
    page_title="Audit Management",
    page_icon="📋",
    layout="wide"
)

# DEFAULT USER CONFIGURATION
DEFAULT_USER_ID = 2
DEFAULT_USERNAME = "Default User"
DEFAULT_USER_DISPLAY = "Default User"
DEFAULT_USER_ROLE = "admin"  # Give full permissions by default

# Initialize session state
def init_session_state():
    """Initialize session state with defaults"""
    if 'user_id' not in st.session_state:
        st.session_state.user_id = DEFAULT_USER_ID
    if 'username' not in st.session_state:
        st.session_state.username = DEFAULT_USERNAME
    if 'user_display_name' not in st.session_state:
        st.session_state.user_display_name = DEFAULT_USER_DISPLAY
    if 'user_role' not in st.session_state:
        st.session_state.user_role = DEFAULT_USER_ROLE

def main():
    """Main page function"""
    # Initialize session state
    init_session_state()
    
    # Page header
    st.title("📋 Audit Management")
    
    # User info
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.write(f"👤 **User:** {st.session_state.user_display_name}")
        st.caption(f"ID: {st.session_state.user_id}")
    with col2:
        st.write(f"🏷️ **Role:** {st.session_state.user_role}")
    with col3:
        if st.button("🏠 Home"):
            st.switch_page("main.py")
    
    st.markdown("---")
    
    # Show full management interface (since we have admin role by default)
    show_full_management()

def show_full_management():
    """Full management interface for managers"""
    tab1, tab2, tab3 = st.tabs(["📋 Sessions", "📦 Transactions", "📊 Overview"])
    
    with tab1:
        session_management_tab()
    
    with tab2:
        transaction_management_tab()
    
    with tab3:
        overview_tab()

# ============== SESSION MANAGEMENT TAB ==============

def session_management_tab():
    """Session CRUD operations"""
    st.subheader("📋 Session Management")
    
    # Create new session
    with st.expander("➕ Create New Session", expanded=False):
        create_session_form()
    
    # Session lists - single column layout
    st.markdown("#### 📝 Draft Sessions")
    show_sessions_by_status('draft')
    
    st.markdown("#### 📄 Active Sessions")
    show_sessions_by_status('in_progress')
    
    # Completed sessions
    st.markdown("#### ✅ Completed Sessions")
    show_sessions_by_status('completed', limit=10)

def create_session_form():
    """Simple session creation form"""
    with st.form("create_session_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            # Get warehouses
            warehouses = audit_service.get_warehouses()
            if warehouses:
                warehouse_options = {w['name']: w['id'] for w in warehouses}
                selected_warehouse_name = st.selectbox("Warehouse", warehouse_options.keys())
                warehouse_id = warehouse_options[selected_warehouse_name]
            else:
                st.error("No warehouses available")
                return
            
            session_name = st.text_input("Session Name", placeholder="e.g., Monthly Audit Dec 2024")
        
        with col2:
            planned_start = st.date_input("Start Date", value=date.today())
            planned_end = st.date_input("End Date", value=date.today())
        
        notes = st.text_area("Notes", placeholder="Additional information")
        
        if st.form_submit_button("Create Session", type="primary"):
            if session_name and warehouse_id:
                try:
                    session_code = audit_service.create_session({
                        'session_name': session_name,
                        'warehouse_id': warehouse_id,
                        'planned_start_date': planned_start,
                        'planned_end_date': planned_end,
                        'notes': notes,
                        'created_by_user_id': st.session_state.user_id
                    })
                    st.success(f"✅ Session created: {session_code}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            else:
                st.warning("Please fill all required fields")

def show_sessions_by_status(status: str, limit: int = 20):
    """Display sessions by status - Fixed column nesting"""
    try:
        sessions = audit_service.get_sessions_by_status(status, limit)
        
        if sessions:
            for session in sessions:
                with st.container():
                    # Main info
                    st.write(f"**{session['session_name']}**")
                    
                    # Session details in single line
                    info_text = f"Code: {session['session_code']} | Warehouse: {session.get('warehouse_name', 'N/A')}"
                    
                    if status == 'draft':
                        info_text += f" | Planned: {session.get('planned_start_date', 'N/A')}"
                    elif status == 'in_progress':
                        progress = audit_service.get_session_progress(session['id'])
                        info_text += f" | Progress: {progress.get('completion_rate', 0):.0f}%"
                        info_text += f" | Txns: {progress.get('completed_transactions', 0)}/{progress.get('total_transactions', 0)}"
                    else:
                        info_text += f" | Completed: {session.get('completed_date', 'N/A')}"
                    
                    st.caption(info_text)
                    
                    # Action buttons in a row
                    if status == 'draft':
                        if st.button("▶️ Start Session", key=f"start_{session['id']}"):
                            try:
                                audit_service.start_session(session['id'], st.session_state.user_id)
                                st.success("Session started!")
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                    
                    elif status == 'in_progress':
                        col1, col2, col3 = st.columns([1, 1, 4])
                        with col1:
                            if st.button("👁️ View", key=f"view_{session['id']}"):
                                st.session_state.selected_session_id = session['id']
                                st.rerun()
                        
                        with col2:
                            if st.button("⏹️ Stop", key=f"stop_{session['id']}"):
                                try:
                                    audit_service.complete_session(session['id'], st.session_state.user_id)
                                    st.success("Session completed!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(str(e))
                    
                    st.divider()
        else:
            st.info(f"No {status} sessions found")
    
    except Exception as e:
        st.error(f"Error loading sessions: {str(e)}")

# ============== TRANSACTION MANAGEMENT TAB ==============

def transaction_management_tab():
    """Transaction management for all users"""
    st.subheader("📦 Transaction Management")
    
    # Select session
    active_sessions = audit_service.get_sessions_by_status('in_progress')
    
    if not active_sessions:
        st.warning("No active sessions available")
        return
    
    # Session selector
    session_options = {f"{s['session_name']} ({s['session_code']})": s['id'] for s in active_sessions}
    selected_session_name = st.selectbox("Select Active Session", session_options.keys())
    selected_session_id = session_options[selected_session_name]
    
    # Store in session state
    st.session_state.selected_session_id = selected_session_id
    
    # Create transaction
    with st.expander("➕ Create New Transaction"):
        create_transaction_form(selected_session_id)
    
    # Show all transactions for this session
    show_all_session_transactions(selected_session_id)

def create_transaction_form(session_id: int):
    """Simple transaction creation form"""
    with st.form("create_transaction_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            transaction_name = st.text_input("Transaction Name", placeholder="e.g., Zone A Counting")
            assigned_zones = st.text_input("Zones", placeholder="e.g., A1, A2, A3")
        
        with col2:
            assigned_categories = st.text_input("Categories", placeholder="e.g., Medicine, Cosmetics")
            notes = st.text_area("Notes", height=68)
        
        if st.form_submit_button("Create Transaction", type="primary"):
            if transaction_name:
                try:
                    tx_code = audit_service.create_transaction({
                        'session_id': session_id,
                        'transaction_name': transaction_name,
                        'assigned_zones': assigned_zones,
                        'assigned_categories': assigned_categories,
                        'notes': notes,
                        'created_by_user_id': st.session_state.user_id
                    })
                    st.success(f"✅ Transaction created: {tx_code}")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            else:
                st.warning("Please enter transaction name")

def show_all_session_transactions(session_id: int):
    """Show all transactions in a session"""
    st.markdown("#### All Transactions")
    
    try:
        # Get all transactions for the session
        query = """
        SELECT 
            at.*,
            u.username,
            CONCAT(e.first_name, ' ', e.last_name) as user_full_name
        FROM audit_transactions at
        LEFT JOIN users u ON at.created_by_user_id = u.id
        LEFT JOIN employees e ON u.employee_id = e.id
        WHERE at.session_id = :session_id
        AND at.delete_flag = 0
        ORDER BY at.created_date DESC
        """
        
        from utils.db import get_db_engine
        from sqlalchemy import text
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), {'session_id': session_id})
            transactions = [dict(row._mapping) for row in result.fetchall()]
        
        if transactions:
            # Group by status
            draft_txns = [t for t in transactions if t['status'] == 'draft']
            completed_txns = [t for t in transactions if t['status'] == 'completed']
            
            # Show draft transactions
            if draft_txns:
                st.markdown("##### 📝 Draft Transactions")
                for tx in draft_txns:
                    show_transaction_card(tx, show_user=True)
            
            # Show completed transactions
            if completed_txns:
                st.markdown("##### ✅ Completed Transactions")
                for tx in completed_txns:
                    show_transaction_card(tx, show_user=True)
        else:
            st.info("No transactions found")
    
    except Exception as e:
        st.error(f"Error loading transactions: {str(e)}")

def show_transaction_card(tx: dict, show_user: bool = False):
    """Display transaction card - Fixed column nesting"""
    with st.container():
        st.write(f"**{tx['transaction_name']}**")
        
        # Transaction info
        info_text = f"Code: {tx['transaction_code']}"
        if tx.get('assigned_zones'):
            info_text += f" | Zones: {tx['assigned_zones']}"
        if show_user and tx.get('user_full_name'):
            info_text += f" | User: {tx['user_full_name']}"
        if tx['status'] == 'completed':
            info_text += f" | Items: {tx.get('total_items_counted', 0)}"
        
        st.caption(info_text)
        
        # Action buttons for draft transactions
        if tx['status'] == 'draft':
            col1, col2, col3 = st.columns([1, 1, 4])
            
            with col1:
                if st.button("📦 Count", key=f"count_{tx['id']}"):
                    st.session_state.selected_tx_id = tx['id']
                    st.switch_page("pages/counting.py")
            
            with col2:
                if tx.get('total_items_counted', 0) > 0:
                    if st.button("✅ Submit", key=f"submit_{tx['id']}"):
                        try:
                            audit_service.submit_transaction(tx['id'], st.session_state.user_id)
                            st.success("Transaction submitted!")
                            st.rerun()
                        except Exception as e:
                            st.error(str(e))
        
        st.divider()

# ============== MY TRANSACTIONS TAB ==============

def my_transactions_tab():
    """User's own transactions across all sessions"""
    st.subheader("📦 My Transactions")
    
    # Get user's recent transactions
    transactions = audit_service.get_user_transactions_all(st.session_state.user_id)
    
    if transactions:
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        
        total_txns = len(transactions)
        completed_txns = len([t for t in transactions if t['status'] == 'completed'])
        draft_txns = len([t for t in transactions if t['status'] == 'draft'])
        
        with col1:
            st.metric("Total", total_txns)
        with col2:
            st.metric("Completed", completed_txns)
        with col3:
            st.metric("Draft", draft_txns)
        
        st.markdown("---")
        
        # Transaction list
        for tx in transactions[:20]:  # Show recent 20
            with st.container():
                st.write(f"**{tx['transaction_name']}**")
                st.caption(f"Session: {tx.get('session_name', 'N/A')} | Code: {tx['transaction_code']} | Status: {tx['status'].title()} | Items: {tx.get('total_items_counted', 0)}")
                
                if tx['status'] == 'draft':
                    if st.button("📦 Continue Counting", key=f"continue_{tx['id']}"):
                        st.session_state.selected_tx_id = tx['id']
                        st.session_state.selected_session_id = tx['session_id']
                        st.switch_page("pages/counting.py")
                
                st.divider()
    else:
        st.info("No transactions found. Create one from an active session.")

# ============== OVERVIEW TAB ==============

def overview_tab():
    """System overview for managers"""
    st.subheader("📊 System Overview")
    
    try:
        # Get statistics
        stats = audit_service.get_dashboard_stats()
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Sessions", stats.get('active_sessions', 0))
        with col2:
            st.metric("Draft Sessions", stats.get('draft_sessions', 0))
        with col3:
            st.metric("Completed Today", stats.get('completed_today', 0))
        with col4:
            st.metric("Active Users", stats.get('active_users', 0))
        
        st.markdown("---")
        
        # User activity
        st.markdown("#### 👥 User Activity (Last 30 days)")
        
        user_stats = audit_service.get_user_activity_stats()
        
        if user_stats:
            df = pd.DataFrame(user_stats)
            df = df[['full_name', 'transactions_created', 'items_counted', 'total_quantity_counted', 'last_activity']]
            df.columns = ['User', 'Transactions', 'Items', 'Quantity', 'Last Activity']
            
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No user activity data")
        
        # Session progress summary
        st.markdown("#### 📈 Active Session Progress")
        
        active_sessions = audit_service.get_sessions_by_status('in_progress')
        
        if active_sessions:
            for session in active_sessions[:5]:
                progress = audit_service.get_session_progress(session['id'])
                
                st.write(f"**{session['session_name']}**")
                st.progress(progress.get('completion_rate', 0) / 100)
                st.caption(f"{progress.get('completion_rate', 0):.0f}% - Transactions: {progress.get('completed_transactions', 0)}/{progress.get('total_transactions', 0)} | Items: {progress.get('total_items', 0)}")
                st.divider()
    
    except Exception as e:
        st.error(f"Error loading overview: {str(e)}")

if __name__ == "__main__":
    main()
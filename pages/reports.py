# pages/reports.py - Reports & Analytics Page
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
import io

# Import services
from utils.auth import AuthManager
from audit_service import AuditService

# Setup logging
logger = logging.getLogger(__name__)

# Initialize services
auth = AuthManager()
audit_service = AuditService()

# Page config
st.set_page_config(
    page_title="Reports & Analytics",
    page_icon="📊",
    layout="wide"
)

# Role permissions
AUDIT_ROLES = {
    'admin': ['manage_sessions', 'view_all', 'create_transactions', 'export_data', 'user_management'],
    'GM': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'MD': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'supply_chain': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'sales_manager': ['manage_sessions', 'view_all', 'create_transactions', 'export_data'],
    'sales': ['create_transactions', 'view_own', 'view_assigned_sessions'],
    'viewer': ['view_own', 'view_assigned_sessions'],
}

def check_permission(action: str) -> bool:
    """Check if current user has permission for action"""
    if 'user_role' not in st.session_state:
        return False
    return action in AUDIT_ROLES.get(st.session_state.user_role, [])

def main():
    """Main reports page"""
    # Check authentication
    if not auth.check_session():
        st.error("Please login first")
        st.stop()
    
    # Page header
    st.title("📊 Reports & Analytics")
    
    # Navigation
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.write(f"👤 **User:** {auth.get_user_display_name()}")
    with col2:
        st.write(f"🏷️ **Role:** {st.session_state.user_role}")
    with col3:
        if st.button("🏠 Home"):
            st.switch_page("main.py")
    
    st.markdown("---")
    
    # Show reports based on permissions
    if check_permission('view_all'):
        show_full_reports()
    elif check_permission('view_own'):
        show_user_reports()
    else:
        st.warning("⚠️ You don't have permission to view reports")

def show_full_reports():
    """Full reports for managers"""
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Session Reports", "📈 Variance Analysis", "👥 User Reports", "📥 Export Data"])
    
    with tab1:
        session_reports_tab()
    
    with tab2:
        variance_analysis_tab()
    
    with tab3:
        user_reports_tab()
    
    with tab4:
        export_data_tab()

def show_user_reports():
    """Reports for regular users"""
    tab1, tab2 = st.tabs(["📊 My Activity", "📈 My Performance"])
    
    with tab1:
        my_activity_tab()
    
    with tab2:
        my_performance_tab()

# ============== SESSION REPORTS TAB ==============

def session_reports_tab():
    """Session-based reports"""
    st.subheader("📊 Session Reports")
    
    # Get all sessions
    sessions = audit_service.get_all_sessions(limit=100)
    
    if not sessions:
        st.info("No sessions found")
        return
    
    # Session selector
    session_options = {
        f"{s['session_name']} ({s['session_code']}) - {s['status'].title()}": s['id']
        for s in sessions
    }
    
    selected_session_name = st.selectbox("Select Session", session_options.keys())
    session_id = session_options[selected_session_name]
    
    # Get session data
    try:
        session_info = audit_service.get_session_info(session_id)
        progress = audit_service.get_session_progress(session_id)
        
        # Display session info
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Status", session_info['status'].title())
        
        with col2:
            st.metric("Total Transactions", progress.get('total_transactions', 0))
        
        with col3:
            st.metric("Items Counted", progress.get('total_items', 0))
        
        with col4:
            st.metric("Total Value", f"${progress.get('total_value', 0):,.2f}")
        
        # Progress chart
        if progress.get('total_transactions', 0) > 0:
            st.markdown("#### Progress Overview")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.progress(progress.get('completion_rate', 0) / 100)
                st.caption(f"Completion Rate: {progress.get('completion_rate', 0):.1f}%")
            
            with col2:
                st.write(f"**Completed:** {progress.get('completed_transactions', 0)}")
                st.write(f"**Pending:** {progress.get('total_transactions', 0) - progress.get('completed_transactions', 0)}")
        
        # Detailed report
        st.markdown("#### Detailed Report")
        
        report_data = audit_service.get_session_report_data(session_id)
        
        if report_data:
            # Convert to DataFrame
            df = pd.DataFrame(report_data)
            
            # Summary stats
            col1, col2, col3 = st.columns(3)
            
            with col1:
                unique_products = df['product_id'].nunique()
                st.metric("Unique Products", unique_products)
            
            with col2:
                total_variance = df['variance_value_usd'].sum()
                st.metric("Total Variance (USD)", f"${total_variance:,.2f}")
            
            with col3:
                items_with_variance = len(df[df['variance_quantity'] != 0])
                st.metric("Items with Variance", items_with_variance)
            
            # Display data table
            st.dataframe(
                df[['product_name', 'batch_no', 'zone_name', 'system_quantity', 
                    'actual_quantity', 'variance_quantity', 'variance_percentage']],
                use_container_width=True
            )
        else:
            st.info("No count data available for this session")
    
    except Exception as e:
        st.error(f"Error loading session report: {str(e)}")

# ============== VARIANCE ANALYSIS TAB ==============

def variance_analysis_tab():
    """Variance analysis reports"""
    st.subheader("📈 Variance Analysis")
    
    # Get sessions with counts
    sessions = audit_service.get_sessions_by_status('completed', limit=50)
    
    if not sessions:
        st.info("No completed sessions found")
        return
    
    # Session selector
    session_options = {
        f"{s['session_name']} ({s['session_code']})": s['id']
        for s in sessions
    }
    
    selected_session_name = st.selectbox("Select Completed Session", session_options.keys())
    session_id = session_options[selected_session_name]
    
    # Get variance data
    try:
        variance_data = audit_service.get_variance_analysis(session_id)
        
        if variance_data:
            df = pd.DataFrame(variance_data)
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_items = len(df)
                st.metric("Items with Variance", total_items)
            
            with col2:
                over_count = len(df[df['variance_quantity'] > 0])
                st.metric("Over Count", over_count)
            
            with col3:
                under_count = len(df[df['variance_quantity'] < 0])
                st.metric("Under Count", under_count)
            
            with col4:
                total_variance = df['variance_value'].sum()
                st.metric("Total Variance (USD)", f"${total_variance:,.2f}")
            
            # Charts
            st.markdown("#### Variance Distribution")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Top 10 positive variances
                top_positive = df[df['variance_quantity'] > 0].nlargest(10, 'variance_value')
                if not top_positive.empty:
                    st.markdown("##### Top Over Counts")
                    st.bar_chart(
                        top_positive.set_index('product_name')['variance_value']
                    )
            
            with col2:
                # Top 10 negative variances
                top_negative = df[df['variance_quantity'] < 0].nsmallest(10, 'variance_value')
                if not top_negative.empty:
                    st.markdown("##### Top Under Counts")
                    st.bar_chart(
                        top_negative.set_index('product_name')['variance_value'].abs()
                    )
            
            # Detailed table
            st.markdown("#### Variance Details")
            
            # Add filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                variance_type = st.selectbox("Variance Type", ["All", "Over Count", "Under Count"])
            
            with col2:
                min_variance = st.number_input("Min Variance %", value=0)
            
            with col3:
                sort_by = st.selectbox("Sort By", ["Variance Value", "Variance %", "Product Name"])
            
            # Filter data
            filtered_df = df.copy()
            
            if variance_type == "Over Count":
                filtered_df = filtered_df[filtered_df['variance_quantity'] > 0]
            elif variance_type == "Under Count":
                filtered_df = filtered_df[filtered_df['variance_quantity'] < 0]
            
            if min_variance > 0:
                filtered_df = filtered_df[filtered_df['variance_percentage'].abs() >= min_variance]
            
            # Sort
            sort_column = {
                "Variance Value": 'variance_value',
                "Variance %": 'variance_percentage',
                "Product Name": 'product_name'
            }[sort_by]
            
            filtered_df = filtered_df.sort_values(sort_column, ascending=False if sort_by != "Product Name" else True)
            
            # Display table
            st.dataframe(
                filtered_df[['product_name', 'pt_code', 'batch_no', 
                           'system_quantity', 'actual_quantity', 
                           'variance_quantity', 'variance_percentage', 'variance_value']],
                use_container_width=True
            )
        else:
            st.info("No variance data available")
    
    except Exception as e:
        st.error(f"Error loading variance analysis: {str(e)}")

# ============== USER REPORTS TAB ==============

def user_reports_tab():
    """User activity reports"""
    st.subheader("👥 User Activity Reports")
    
    try:
        # Get user activity stats
        user_stats = audit_service.get_user_activity_stats()
        
        if user_stats:
            df = pd.DataFrame(user_stats)
            
            # Summary
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Active Users", len(df))
            
            with col2:
                st.metric("Total Transactions", df['transactions_created'].sum())
            
            with col3:
                st.metric("Total Items Counted", df['items_counted'].sum())
            
            # User performance chart
            st.markdown("#### Top Performers")
            
            # Top 10 by items counted
            top_users = df.nlargest(10, 'items_counted')
            
            st.bar_chart(
                top_users.set_index('full_name')['items_counted']
            )
            
            # Detailed table
            st.markdown("#### User Details")
            
            st.dataframe(
                df[['full_name', 'transactions_created', 'items_counted', 
                    'total_quantity_counted', 'last_activity']],
                use_container_width=True
            )
        else:
            st.info("No user activity data available")
    
    except Exception as e:
        st.error(f"Error loading user reports: {str(e)}")

# ============== EXPORT DATA TAB ==============

def export_data_tab():
    """Export data functionality"""
    st.subheader("📥 Export Data")
    
    # Session selector
    sessions = audit_service.get_all_sessions(limit=100)
    
    if not sessions:
        st.info("No sessions found")
        return
    
    session_options = {
        f"{s['session_name']} ({s['session_code']}) - {s['status'].title()}": s['id']
        for s in sessions
    }
    
    selected_session_name = st.selectbox("Select Session to Export", session_options.keys())
    session_id = session_options[selected_session_name]
    
    # Export options
    st.markdown("#### Export Options")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Export to Excel", use_container_width=True):
            export_to_excel(session_id)
    
    with col2:
        if st.button("📄 Export to CSV", use_container_width=True):
            export_to_csv(session_id)
    
    with col3:
        if st.button("📈 Export Variance Report", use_container_width=True):
            export_variance_report(session_id)

def export_to_excel(session_id: int):
    """Export session data to Excel"""
    try:
        with st.spinner("Preparing Excel file..."):
            file_path = audit_service.export_session_to_excel(session_id)
            
            with open(file_path, 'rb') as f:
                data = f.read()
            
            st.download_button(
                label="📥 Download Excel File",
                data=data,
                file_name=f"audit_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.success("✅ Excel file ready for download!")
    
    except Exception as e:
        st.error(f"Error exporting to Excel: {str(e)}")

def export_to_csv(session_id: int):
    """Export session data to CSV"""
    try:
        with st.spinner("Preparing CSV file..."):
            report_data = audit_service.get_session_report_data(session_id)
            
            if report_data:
                df = pd.DataFrame(report_data)
                csv = df.to_csv(index=False)
                
                st.download_button(
                    label="📥 Download CSV File",
                    data=csv,
                    file_name=f"audit_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                st.success("✅ CSV file ready for download!")
            else:
                st.warning("No data to export")
    
    except Exception as e:
        st.error(f"Error exporting to CSV: {str(e)}")

def export_variance_report(session_id: int):
    """Export variance analysis report"""
    try:
        with st.spinner("Preparing variance report..."):
            variance_data = audit_service.get_variance_analysis(session_id)
            
            if variance_data:
                df = pd.DataFrame(variance_data)
                csv = df.to_csv(index=False)
                
                st.download_button(
                    label="📥 Download Variance Report",
                    data=csv,
                    file_name=f"variance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                st.success("✅ Variance report ready for download!")
            else:
                st.warning("No variance data to export")
    
    except Exception as e:
        st.error(f"Error exporting variance report: {str(e)}")

# ============== USER REPORTS ==============

def my_activity_tab():
    """User's own activity report"""
    st.subheader("📊 My Activity")
    
    try:
        # Get user's transactions
        transactions = audit_service.get_user_transactions_all(st.session_state.user_id)
        
        if transactions:
            # Summary metrics
            total_txns = len(transactions)
            completed_txns = len([t for t in transactions if t['status'] == 'completed'])
            total_items = sum(t.get('total_items_counted', 0) for t in transactions)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Transactions", total_txns)
            
            with col2:
                st.metric("Completed", completed_txns)
            
            with col3:
                st.metric("Items Counted", total_items)
            
            # Activity timeline
            st.markdown("#### Activity Timeline")
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(transactions)
            df['created_date'] = pd.to_datetime(df['created_date'])
            df['date'] = df['created_date'].dt.date
            
            # Group by date
            daily_counts = df.groupby('date').size()
            
            st.line_chart(daily_counts)
            
            # Recent transactions
            st.markdown("#### Recent Transactions")
            
            for tx in transactions[:10]:
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"**{tx['transaction_name']}**")
                    st.caption(f"Session: {tx.get('session_name', 'N/A')}")
                
                with col2:
                    st.write(f"Status: {tx['status'].title()}")
                
                with col3:
                    st.write(f"Items: {tx.get('total_items_counted', 0)}")
                
                st.divider()
        else:
            st.info("No activity found")
    
    except Exception as e:
        st.error(f"Error loading activity: {str(e)}")

def my_performance_tab():
    """User's performance metrics"""
    st.subheader("📈 My Performance")
    
    st.info("🚧 Performance metrics coming soon...")
    
    st.markdown("""
    **Planned Features:**
    - Counting accuracy trends
    - Average items per hour
    - Comparison with team average
    - Personal best records
    """)

if __name__ == "__main__":
    main()
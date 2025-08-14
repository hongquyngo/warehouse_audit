# Simplified 4auditor.py - Warehouse Audit System
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
from typing import Dict, List, Optional
from sqlalchemy import text
from utils.db import get_db_engine
from audit_service import AuditService
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
audit_service = AuditService()
queries = AuditQueries()

# ============== SESSION STATE ==============

def init_session_state():
    """Initialize session state variables"""
    defaults = {
        'temp_counts': [],
        'selected_product': None,
        'selected_batch': None,
        'selected_session_id': None,
        'selected_tx_id': None,
        'user_id': 1,  # Default user for simplified version
        'username': 'auditor',
        'last_action': None,
        'show_team_counts': False,
    }
    
    for key, default in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

# ============== HELPER FUNCTIONS ==============

def get_team_count_total(session_id: int, product_id: int) -> float:
    """Get total team count for a product across all transactions"""
    summary = audit_service.get_product_total_summary(session_id, product_id)
    return summary.get('grand_total_counted', 0)

def get_product_status(product: Dict, session_id: int) -> str:
    """Determine product status based on TEAM counted quantity"""
    product_id = product['product_id']
    system_qty = product.get('total_quantity', 0)
    
    # Get total team counts
    team_counted_qty = get_team_count_total(session_id, product_id)
    
    # Check for pending counts in current session
    temp_qty = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                   if tc.get('product_id') == product_id)
    
    # Determine status based on team totals
    if temp_qty > 0:
        return "🔴"  # Has pending counts
    elif team_counted_qty >= system_qty * 0.95:
        return "✅"  # Fully counted (95%+)
    elif team_counted_qty > 0:
        return "🟡"  # Partially counted
    else:
        return "⭕"  # Not counted

# ============== MAIN UI FUNCTIONS ==============

def show_transaction_selector():
    """Show transaction selector"""
    if not st.session_state.selected_session_id:
        st.warning("⚠️ Please select a session first")
        return None
    
    # Get user's draft transactions
    transactions = audit_service.get_user_transactions(
        st.session_state.selected_session_id,
        st.session_state.user_id,
        status='draft'
    )
    
    if not transactions:
        st.info("No draft transactions. Create one in the Transactions tab.")
        return None
    
    # Transaction selector
    tx_options = {f"{t['transaction_name']} ({t['transaction_code']})": t for t in transactions}
    selected_tx_key = st.selectbox(
        "Select Transaction",
        list(tx_options.keys())
    )
    
    selected_tx = tx_options[selected_tx_key]
    st.session_state.selected_tx_id = selected_tx['id']
    return selected_tx

def show_product_selector(warehouse_id: int):
    """Show product selector with team-based status"""
    products = audit_service.get_warehouse_products(warehouse_id)
    
    # Build product options
    product_options = ["-- Select Product --"]
    
    for p in products:
        # Get status based on TEAM totals
        status = get_product_status(p, st.session_state.selected_session_id)
        
        # Get team totals for display
        team_total = get_team_count_total(st.session_state.selected_session_id, p['product_id'])
        system_qty = p.get('total_quantity', 0)
        
        # Format display
        display = f"{status} {p.get('pt_code', 'N/A')} - {p.get('product_name', 'Unknown')[:40]}"
        if team_total > 0:
            display += f" [Team: {team_total:.0f}/{system_qty:.0f}]"
        
        product_options.append((display, p))
    
    # Product selector
    col1, col2 = st.columns([5, 1])
    with col1:
        selected_option = st.selectbox(
            "Select Product",
            [opt[0] for opt in product_options],
            help="⭕ Not counted | 🟡 Partially counted | ✅ Fully counted | 🔴 Has pending"
        )
    
    with col2:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    # Get selected product
    if selected_option != "-- Select Product --":
        for display, product in product_options[1:]:
            if display == selected_option:
                st.session_state.selected_product = product
                return product
    
    return None

def show_batch_selector(warehouse_id: int, product_id: int):
    """Show batch selector"""
    batches = audit_service.get_product_batch_details(warehouse_id, product_id)
    
    if not batches:
        return None
    
    batch_options = ["-- Manual Entry --"]
    batch_map = {}
    
    for batch in batches:
        option = f"{batch['batch_no']} (Qty: {batch['quantity']:.0f}, Loc: {batch.get('location', 'N/A')})"
        batch_options.append(option)
        batch_map[option] = batch
    
    selected_batch = st.selectbox(
        "Select Batch (Optional)",
        batch_options,
        help="You can count the same batch multiple times"
    )
    
    if selected_batch != "-- Manual Entry --":
        return batch_map.get(selected_batch)
    
    return None

def show_count_form(selected_batch: Dict = None):
    """Show counting form"""
    col1, col2 = st.columns(2)
    
    with col1:
        batch_no = st.text_input(
            "Batch Number",
            value=selected_batch['batch_no'] if selected_batch else "",
            placeholder="Enter batch number"
        )
        
        expiry = st.date_input(
            "Expiry Date",
            value=pd.to_datetime(selected_batch['expired_date']).date() if selected_batch and selected_batch.get('expired_date') else None,
            min_value=date(2020, 1, 1),
            max_value=date(2030, 12, 31)
        )
        
        qty = st.number_input(
            "Actual Quantity*",
            min_value=0.0,
            step=1.0,
            format="%.2f"
        )
    
    with col2:
        location = st.text_input(
            "Location",
            value=selected_batch.get('location', '') if selected_batch else "",
            placeholder="e.g., A1-R01-B01"
        )
        
        notes = st.text_area(
            "Notes",
            placeholder="Any observations"
        )
    
    # Add button
    if st.button("➕ Add Count", type="primary", disabled=qty <= 0):
        # Parse location
        zone, rack, bin_loc = '', '', ''
        if location and '-' in location:
            parts = location.split('-', 2)
            zone = parts[0] if len(parts) > 0 else ''
            rack = parts[1] if len(parts) > 1 else ''
            bin_loc = parts[2] if len(parts) > 2 else ''
        else:
            zone = location
        
        # Create count
        count = {
            'transaction_id': st.session_state.selected_tx_id,
            'product_id': st.session_state.selected_product['product_id'],
            'product_name': st.session_state.selected_product['product_name'],
            'batch_no': batch_no,
            'expired_date': expiry,
            'zone_name': zone,
            'rack_name': rack,
            'bin_name': bin_loc,
            'system_quantity': selected_batch['quantity'] if selected_batch else 0,
            'system_value_usd': selected_batch.get('value_usd', 0) if selected_batch else 0,
            'actual_quantity': qty,
            'actual_notes': notes,
            'created_by_user_id': st.session_state.user_id,
            'time': datetime.now().strftime('%H:%M:%S')
        }
        
        st.session_state.temp_counts.append(count)
        st.success(f"✅ Added count #{len(st.session_state.temp_counts)}")
        st.rerun()

def show_pending_counts():
    """Display pending counts"""
    if not st.session_state.temp_counts:
        return
    
    st.markdown(f"### 📋 Pending Counts ({len(st.session_state.temp_counts)})")
    
    # Group by product
    grouped = {}
    for count in st.session_state.temp_counts:
        product_name = count['product_name']
        if product_name not in grouped:
            grouped[product_name] = []
        grouped[product_name].append(count)
    
    # Display grouped counts
    for product_name, counts in grouped.items():
        total_qty = sum(c['actual_quantity'] for c in counts)
        st.markdown(f"**{product_name}** - {len(counts)} records, Total: {total_qty:.0f}")
        
        for i, count in enumerate(counts):
            idx = st.session_state.temp_counts.index(count)
            col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 2, 1])
            
            with col1:
                st.text(f"Batch: {count['batch_no'] or 'N/A'}")
            with col2:
                st.text(f"Qty: {count['actual_quantity']:.0f}")
            with col3:
                location = f"{count['zone_name']}"
                if count['rack_name']:
                    location += f"-{count['rack_name']}"
                if count['bin_name']:
                    location += f"-{count['bin_name']}"
                st.text(location)
            with col4:
                st.text(count['time'])
            with col5:
                if st.button("❌", key=f"del_{idx}"):
                    st.session_state.temp_counts.pop(idx)
                    st.rerun()
    
    # Save buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("💾 Save All", type="primary", use_container_width=True):
            try:
                saved, errors = audit_service.save_batch_counts(st.session_state.temp_counts)
                if errors:
                    st.warning(f"Saved {saved} counts with {len(errors)} errors")
                    for error in errors:
                        st.error(error)
                else:
                    st.success(f"✅ Saved {saved} counts!")
                    st.session_state.temp_counts = []
                    st.rerun()
            except Exception as e:
                st.error(f"Error saving: {str(e)}")
    
    with col2:
        if st.button("🗑️ Clear All", use_container_width=True):
            st.session_state.temp_counts = []
            st.rerun()

def show_team_counts_simple(session_id: int, product_id: int):
    """Show team counts in a simple format without nested expanders"""
    summary = audit_service.get_product_total_summary(session_id, product_id)
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("👥 Total Users", summary['total_users'])
    with col2:
        st.metric("📋 Transactions", summary['total_transactions'])
    with col3:
        st.metric("📦 Batches", summary['total_batches'])
    with col4:
        st.metric("📢 Total Quantity", f"{summary['grand_total_counted']:.0f}")
    
    # Get all counts
    all_counts = audit_service.get_product_counts_all_transactions(session_id, product_id)
    
    if all_counts:
        st.markdown("#### Count Details by Transaction")
        
        # Group by transaction
        current_tx = None
        for count in all_counts:
            # Transaction header
            if current_tx != count['transaction_id']:
                current_tx = count['transaction_id']
                st.markdown(f"**{count['transaction_code']} - {count['transaction_name']}** "
                          f"({'✅ Completed' if count['transaction_status'] == 'completed' else '🔓 Draft'})")
            
            # Count details
            col1, col2, col3, col4 = st.columns([2, 2, 1, 2])
            with col1:
                st.text(f"  • {count['counter_name']} (@{count['counted_by']})")
            with col2:
                st.text(f"Batch: {count['batch_no'] or 'N/A'}")
            with col3:
                st.text(f"Qty: {count['total_counted']:.0f}")
            with col4:
                locations = count['locations'].split(',') if count['locations'] else []
                st.text(f"Locations: {len(locations)}")

# ============== MAIN PAGES ==============

def counting_page():
    """Main counting page"""
    st.subheader("🚀 Fast Counting Mode")
    
    init_session_state()
    
    # Select transaction
    selected_tx = show_transaction_selector()
    if not selected_tx:
        return
    
    warehouse_id = selected_tx['warehouse_id']
    
    # Show pending counts
    show_pending_counts()
    
    st.markdown("### 📦 Product Selection")
    
    # Select product
    selected_product = show_product_selector(warehouse_id)
    if not selected_product:
        st.info("👆 Please select a product above")
        return
    
    # Product info
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"**{selected_product['product_name']}**")
        st.caption(f"PT Code: {selected_product.get('pt_code', 'N/A')} | Brand: {selected_product.get('brand', 'N/A')}")
    with col2:
        st.metric("System Total", f"{selected_product.get('total_quantity', 0):.0f}")
    
    # Team counts toggle
    if st.checkbox("👥 Show Team Counts", key="team_counts_toggle"):
        show_team_counts_simple(
            st.session_state.selected_session_id,
            selected_product['product_id']
        )
    
    st.markdown("---")
    
    # Select batch (optional)
    selected_batch = show_batch_selector(warehouse_id, selected_product['product_id'])
    
    st.markdown("### ✏️ Count Entry")
    
    # Count form
    show_count_form(selected_batch)

def transactions_page():
    """Transactions management page"""
    st.subheader("📋 My Audit Transactions")
    
    # Get active sessions
    sessions = audit_service.get_sessions_by_status('in_progress')
    
    if not sessions:
        st.warning("No active audit sessions available")
        return
    
    # Session selector
    session_options = {f"{s['session_name']} ({s['session_code']})": s['id'] for s in sessions}
    selected_session_key = st.selectbox("Select Active Session", list(session_options.keys()))
    st.session_state.selected_session_id = session_options[selected_session_key]
    
    # Create new transaction
    with st.expander("➕ Create New Transaction"):
        with st.form("create_transaction"):
            tx_name = st.text_input("Transaction Name*", placeholder="e.g., Zone A1-A3 counting")
            zones = st.text_input("Assigned Zones", placeholder="e.g., A1,A2,A3")
            notes = st.text_area("Notes")
            
            if st.form_submit_button("Create Transaction"):
                if tx_name:
                    try:
                        tx_code = audit_service.create_transaction({
                            'session_id': st.session_state.selected_session_id,
                            'transaction_name': tx_name,
                            'assigned_zones': zones,
                            'notes': notes,
                            'created_by_user_id': st.session_state.user_id
                        })
                        st.success(f"✅ Transaction created! Code: {tx_code}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    # Display transactions
    st.markdown("### My Transactions")
    transactions = audit_service.get_user_transactions(
        st.session_state.selected_session_id,
        st.session_state.user_id
    )
    
    if transactions:
        for tx in transactions:
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            
            with col1:
                st.write(f"**{tx['transaction_name']}**")
                st.caption(f"Code: {tx['transaction_code']}")
            
            with col2:
                status_icon = "✅" if tx['status'] == 'completed' else "🟡"
                st.write(f"{status_icon} {tx['status'].title()}")
            
            with col3:
                st.write(f"Items: {tx.get('total_items_counted', 0)}")
            
            with col4:
                if tx['status'] == 'draft' and tx.get('total_items_counted', 0) > 0:
                    if st.button("Submit", key=f"submit_{tx['id']}"):
                        try:
                            audit_service.submit_transaction(tx['id'], st.session_state.user_id)
                            st.success("Transaction submitted!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
            
            st.markdown("---")
    else:
        st.info("No transactions created yet")

# ============== MAIN APP ==============

def main():
    """Main application entry"""
    st.title("📦 Warehouse Audit System - Simplified")
    
    # Initialize session state
    init_session_state()
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 👤 User Info")
        st.write(f"**User:** {st.session_state.username}")
        st.write(f"**User ID:** {st.session_state.user_id}")
        
        st.markdown("---")
        st.markdown("### ⚡ Performance Mode")
        st.info("Fast Counting Mode Active")
        st.caption("• Multiple counts per batch")
        st.caption("• Team-based status")
        st.caption("• Simplified interface")
    
    # Main tabs
    tab1, tab2 = st.tabs(["📋 Transactions", "🚀 Fast Counting"])
    
    with tab1:
        transactions_page()
    
    with tab2:
        counting_page()

if __name__ == "__main__":
    main()
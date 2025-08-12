# simplified_4auditor.py - Warehouse Audit System (Simplified - No Auth)
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import logging
from typing import Dict, List, Optional, Tuple
from functools import lru_cache
from sqlalchemy import text

# Import existing utilities
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
audit_service = AuditService()
queries = AuditQueries()

# ============== USER MANAGEMENT ==============

@st.cache_data(ttl=3600)
def get_all_users():
    """Get all active users from database"""
    try:
        engine = get_db_engine()
        query = """
        SELECT 
            u.id as user_id,
            u.username,
            u.employee_id,
            u.role_id,
            COALESCE(r.name, 'viewer') as user_role,
            u.is_active,
            COALESCE(CONCAT(e.first_name, ' ', e.last_name), u.username) as full_name,
            e.email
        FROM users u
        LEFT JOIN roles r ON u.role_id = r.id
        LEFT JOIN employees e ON u.employee_id = e.id
        WHERE u.is_active = 1
        AND u.delete_flag = 0
        ORDER BY u.username
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            users = []
            for row in result:
                users.append({
                    'user_id': row.user_id,
                    'username': row.username,
                    'full_name': row.full_name if row.full_name else row.username,
                    'user_role': row.user_role,
                    'email': row.email,
                    'employee_id': row.employee_id
                })
            return users
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        return []

def get_current_user():
    """Get current selected user or default admin"""
    if 'selected_user_id' not in st.session_state:
        st.session_state.selected_user_id = 1  # Default admin user
    
    # Get user info
    users = get_all_users()
    current_user = next((u for u in users if u['user_id'] == st.session_state.selected_user_id), None)
    
    if not current_user:
        # Fallback to admin if user not found
        current_user = {
            'user_id': 1,
            'username': 'admin',
            'full_name': 'Administrator',
            'user_role': 'admin',
            'email': 'admin@company.com',
            'employee_id': None
        }
    
    # Update session state with user info
    st.session_state.user_id = current_user['user_id']
    st.session_state.username = current_user['username']
    st.session_state.user_role = current_user['user_role']
    st.session_state.employee_name = current_user['full_name']
    st.session_state.employee_email = current_user.get('email', '')
    st.session_state.employee_id = current_user.get('employee_id')
    
    return current_user

# ============== SESSION STATE INITIALIZATION ==============

def init_session_state():
    """Initialize session state variables"""
    defaults = {
        # User management
        'selected_user_id': 1,  # Default admin
        
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
        
        # Display states
        'show_batch_details': {},
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

# ============== CALLBACKS ==============

def on_product_change():
    """Callback when product is selected"""
    selected = st.session_state.product_select
    if selected and selected != "-- Select Product --":
        # Parse product data from selection
        product_data = st.session_state.products_map.get(selected)
        if product_data:
            st.session_state.selected_product = product_data
            st.session_state.selected_batch = None
            st.session_state.form_batch_no = ''
            st.session_state.form_location = ''
            st.session_state.form_expiry = None

def on_batch_change():
    """Callback when batch is selected"""
    selected = st.session_state.batch_select
    if selected and selected != "-- Manual Entry --":
        # Extract batch number from selection
        batch_no = selected.split(" (")[0].replace("🔴", "").replace("🟡", "").replace("🟢", "").strip()
        batch_data = st.session_state.batches_map.get(batch_no)
        if batch_data:
            st.session_state.selected_batch = batch_data
            st.session_state.form_batch_no = batch_no
            st.session_state.form_location = batch_data.get('location', '')
            # Set expiry date if available
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
    """Add count to temporary list"""
    # Get form values
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
        
        st.session_state.temp_counts.append(count)
        st.session_state.last_action = f"✅ Added count #{len(st.session_state.temp_counts)}"
        st.session_state.last_action_time = datetime.now()
        
        # Clear form inputs
        st.session_state.qty_input = 0
        st.session_state.notes_input = ''

def save_counts_callback():
    """Save all counts to database"""
    if st.session_state.temp_counts:
        try:
            st.session_state.last_action = "💾 Saving..."
            saved, errors = audit_service.save_batch_counts(st.session_state.temp_counts)
            
            if errors:
                st.session_state.last_action = f"⚠️ Saved {saved} counts with {len(errors)} errors"
            else:
                st.session_state.last_action = f"✅ Successfully saved {saved} counts!"
                st.session_state.temp_counts = []
                # Clear relevant caches
                get_count_summary.clear()
            
            st.session_state.last_action_time = datetime.now()
            
        except Exception as e:
            st.session_state.last_action = f"❌ Error: {str(e)}"
            st.session_state.last_action_time = datetime.now()
            logger.error(f"Save error: {e}")

# ============== DISPLAY FUNCTIONS ==============

def display_batch_count_summary(transaction_id: int, product_id: int):
    """Display summary of all counts for a product"""
    try:
        # Get batch count status
        batch_counts = audit_service.get_batch_count_status(transaction_id, product_id)
        
        if batch_counts:
            st.markdown("#### 📊 Count Summary by Batch")
            
            for batch in batch_counts:
                col1, col2, col3, col4 = st.columns([2, 1, 1, 2])
                
                with col1:
                    st.write(f"**Batch:** {batch['batch_no']}")
                    st.caption(f"Last: {pd.to_datetime(batch['last_counted']).strftime('%H:%M')}")
                
                with col2:
                    st.metric("Records", batch.get('count_records', batch.get('count_times', 0)))
                
                with col3:
                    st.metric("Total Qty", f"{batch['total_counted']:.0f}")
                
                with col4:
                    locations = batch.get('locations_counted', '').split(',')
                    st.write(f"**Locations:** {len(locations)}")
                    st.caption(", ".join(locations[:2]) + ("..." if len(locations) > 2 else ""))
                
                # Show details button
                if st.button(f"View Details", key=f"details_{batch['batch_no']}"):
                    st.session_state.show_batch_details[f"{product_id}_{batch['batch_no']}"] = True
                    st.rerun()
                
                # Show details if expanded
                if st.session_state.show_batch_details.get(f"{product_id}_{batch['batch_no']}", False):
                    show_batch_count_details(transaction_id, product_id, batch['batch_no'])
                
                st.markdown("---")
    
    except Exception as e:
        st.error(f"Error loading count summary: {str(e)}")

def show_batch_count_details(transaction_id: int, product_id: int, batch_no: str):
    """Show all individual count records for a batch"""
    with st.expander(f"📋 Count Details for Batch {batch_no}", expanded=True):
        try:
            # Get count history
            details = audit_service.get_batch_count_history(transaction_id, product_id, batch_no)
            
            if details:
                # Summary metrics
                total_qty = sum(d.get('actual_quantity', 0) for d in details)
                total_records = len(details)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Records", total_records)
                with col2:
                    st.metric("Total Quantity", f"{total_qty:.0f}")
                with col3:
                    unique_locations = len(set(d.get('location', '') for d in details))
                    st.metric("Locations", unique_locations)
                
                # Detail table
                st.markdown("##### Individual Count Records")
                
                for i, detail in enumerate(details):
                    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 2, 2])
                    
                    with col1:
                        st.text(f"#{i+1} - {detail.get('location', 'N/A')}")
                        st.caption(pd.to_datetime(detail['counted_date']).strftime('%m/%d %H:%M'))
                    
                    with col2:
                        st.text(f"Qty: {detail.get('actual_quantity', 0):.0f}")
                    
                    with col3:
                        # Calculate variance if system quantity available
                        if 'system_quantity' in detail:
                            variance = detail.get('actual_quantity', 0) - detail.get('system_quantity', 0)
                        else:
                            variance = 0
                            
                        if variance > 0:
                            st.text(f"📈 +{variance:.0f}")
                        elif variance < 0:
                            st.text(f"📉 {variance:.0f}")
                        else:
                            st.text("✓ 0")
                    
                    with col4:
                        st.text(detail.get('counter_name', detail.get('counted_by', 'Unknown')))
                    
                    with col5:
                        if detail.get('actual_notes'):
                            st.caption(detail['actual_notes'][:50] + ("..." if len(detail['actual_notes']) > 50 else ""))
                    
                    if i < len(details) - 1:
                        st.markdown("---")
                        
            # Close button
            if st.button("Close", key=f"close_{product_id}_{batch_no}"):
                st.session_state.show_batch_details[f"{product_id}_{batch_no}"] = False
                st.rerun()
            
        except Exception as e:
            st.error(f"Error loading details: {str(e)}")

def render_temp_counts_enhanced():
    """Display temporary counts with batch grouping"""
    if st.session_state.temp_counts:
        st.markdown(f"### 📋 Pending Counts ({len(st.session_state.temp_counts)})")
        
        # Group by product and batch
        grouped = {}
        for count in st.session_state.temp_counts:
            key = f"{count['product_id']}_{count['batch_no']}"
            if key not in grouped:
                grouped[key] = {
                    'product_name': count['product_name'],
                    'product_id': count['product_id'],
                    'batch_no': count['batch_no'],
                    'counts': []
                }
            grouped[key]['counts'].append(count)
        
        # Display grouped
        for key, group in grouped.items():
            total_qty = sum(c['actual_quantity'] for c in group['counts'])
            
            with st.expander(
                f"{group['product_name']} - Batch: {group['batch_no'] or 'N/A'} "
                f"({len(group['counts'])} records, Total: {total_qty:.0f})",
                expanded=True
            ):
                for i, count in enumerate(group['counts']):
                    idx = st.session_state.temp_counts.index(count)
                    
                    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 2, 1])
                    
                    with col1:
                        location = f"{count['zone_name']}"
                        if count['rack_name']:
                            location += f"-{count['rack_name']}"
                        if count['bin_name']:
                            location += f"-{count['bin_name']}"
                        st.text(f"📍 {location}")
                        st.caption(f"Time: {count['time']}")
                    
                    with col2:
                        st.text(f"Qty: {count['actual_quantity']:.0f}")
                    
                    with col3:
                        variance = count['actual_quantity'] - count['system_quantity']
                        if variance > 0:
                            st.text(f"📈 +{variance:.0f}")
                        elif variance < 0:
                            st.text(f"📉 {variance:.0f}")
                        else:
                            st.text("✓ 0")
                    
                    with col4:
                        if count.get('actual_notes'):
                            st.caption(count['actual_notes'][:50])
                    
                    with col5:
                        if st.button("❌", key=f"del_{idx}"):
                            st.session_state.temp_counts.pop(idx)
                            st.session_state.last_action = "🗑️ Removed count"
                            st.session_state.last_action_time = datetime.now()
                            st.rerun()
                    
                    if i < len(group['counts']) - 1:
                        st.markdown("---")

# ============== MAIN COUNTING INTERFACE ==============

@st.fragment(run_every=None)
def counting_form_fragment():
    """Isolated counting form to prevent full page reruns"""
    
    if not st.session_state.selected_product:
        st.info("👆 Please select a product above")
        return
    
    # Product info display
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(f"**{st.session_state.selected_product['product_name']}**")
        st.caption(f"PT Code: {st.session_state.selected_product.get('pt_code', 'N/A')} | Brand: {st.session_state.selected_product.get('brand', 'N/A')}")
    with col2:
        st.metric("System Total", f"{st.session_state.selected_product.get('total_quantity', 0):.0f}")
    
    st.markdown("---")
    
    # Form inputs
    col1, col2 = st.columns(2)
    
    with col1:
        # Batch number input
        batch_no = st.text_input(
            "Batch Number",
            key="batch_input",
            value=st.session_state.form_batch_no,
            placeholder="Enter batch or select from dropdown"
        )
        
        # Expiry date
        expiry = st.date_input(
            "Expiry Date",
            key="expiry_input",
            value=st.session_state.form_expiry,
            min_value=date(2020, 1, 1),
            max_value=date(2030, 12, 31)
        )
        
        # Quantity
        qty = st.number_input(
            "Actual Quantity*",
            min_value=0.0,
            step=1.0,
            key="qty_input",
            format="%.2f"
        )
        
        # Show system qty if batch selected
        if st.session_state.selected_batch:
            col_sys, col_var = st.columns(2)
            with col_sys:
                st.info(f"System: {st.session_state.selected_batch['quantity']:.0f}")
            with col_var:
                if qty > 0:
                    variance = qty - st.session_state.selected_batch['quantity']
                    if variance > 0:
                        st.success(f"Variance: +{variance:.0f}")
                    elif variance < 0:
                        st.error(f"Variance: {variance:.0f}")
    
    with col2:
        # Location
        location = st.text_input(
            "Location",
            key="loc_input",
            value=st.session_state.form_location,
            placeholder="e.g., A1-R01-B01"
        )
        
        # Notes
        notes = st.text_area(
            "Notes",
            key="notes_input",
            height=100,
            placeholder="Any observations (damage, near expiry, etc.)"
        )
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        add_btn = st.button(
            f"➕ Add Count ({len(st.session_state.temp_counts)}/20)",
            on_click=add_count_callback,
            use_container_width=True,
            type="primary",
            disabled=len(st.session_state.temp_counts) >= 20
        )
    
    with col2:
        save_btn = st.button(
            f"💾 Save All ({len(st.session_state.temp_counts)})",
            on_click=save_counts_callback,
            use_container_width=True,
            disabled=len(st.session_state.temp_counts) == 0
        )
    
    with col3:
        if st.button("🗑️ Clear All", use_container_width=True):
            st.session_state.temp_counts = []
            st.session_state.last_action = "🗑️ Cleared all pending counts"
            st.session_state.last_action_time = datetime.now()

def counting_page():
    """Main counting page"""
    st.subheader("🚀 Fast Counting Mode - Multiple Counts per Batch Supported")
    
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
    
    # Show action status messages
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
    
    # Display temporary counts with enhanced grouping
    render_temp_counts_enhanced()
    
    st.markdown("### 📦 Product Selection")
    
    # Get products and count summary
    products = get_warehouse_products(warehouse_id)
    count_summary = get_count_summary(selected_tx['id'])
    count_map = {c['product_id']: c for c in count_summary}
    
    # Build product options with count status
    product_options = ["-- Select Product --"]
    products_map = {}
    
    for p in products:
        # Get count info
        count_info = count_map.get(p['product_id'], {})
        counted_qty = count_info.get('total_counted', 0)
        count_records = count_info.get('count_records', 0)
        
        # Check temp counts
        temp_qty = sum(tc['actual_quantity'] for tc in st.session_state.temp_counts 
                       if tc.get('product_id') == p['product_id'])
        temp_records = len([tc for tc in st.session_state.temp_counts 
                           if tc.get('product_id') == p['product_id']])
        
        # Determine status
        system_qty = p.get('total_quantity', 0)
        if temp_qty > 0:
            status = "📝"  # Has pending counts
        elif counted_qty >= system_qty * 0.95:
            status = "✅"  # Fully counted (95%+)
        elif counted_qty > 0:
            status = "🟡"  # Partially counted
        else:
            status = "⭕"  # Not counted
        
        # Format display
        display = f"{status} {p.get('pt_code', 'N/A')} - {p.get('product_name', 'Unknown')[:40]}"
        if len(p.get('product_name', '')) > 40:
            display += "..."
        if counted_qty > 0:
            display += f" [{count_records} records, {counted_qty:.0f}/{system_qty:.0f}]"
        
        product_options.append(display)
        products_map[display] = p
    
    st.session_state.products_map = products_map
    
    # Product selector with refresh button
    col1, col2 = st.columns([5, 1])
    with col1:
        st.selectbox(
            "Select Product",
            product_options,
            key="product_select",
            on_change=on_product_change,
            help="⭕ Not counted | 🟡 Partially counted | ✅ Fully counted | 📝 Has pending counts"
        )
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            # Clear caches
            get_warehouse_products.clear()
            get_count_summary.clear()
            st.rerun()
    
    # Show count summary for selected product
    if st.session_state.selected_product and 'product_id' in st.session_state.selected_product:
        # Check if product already has counts
        product_count_info = count_map.get(st.session_state.selected_product['product_id'], {})
        if product_count_info.get('count_records', 0) > 0:
            with st.expander("📊 View Existing Counts for This Product", expanded=False):
                display_batch_count_summary(selected_tx['id'], st.session_state.selected_product['product_id'])
    
    # Batch selector (only show if product selected)
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
            
            st.info("💡 **Multiple Counts Allowed**: You can count the same batch multiple times from different locations/boxes")
        
        st.markdown("### ✏️ Count Entry")
    
    # Counting form (isolated fragment)
    counting_form_fragment()
    
    # Progress summary
    with st.expander("📊 Transaction Progress", expanded=False):
        try:
            progress = audit_service.get_transaction_progress(selected_tx['id'])
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Items Counted", progress.get('items_counted', 0))
            with col2:
                st.metric("Total Value", f"${progress.get('total_value', 0):,.0f}")
            with col3:
                st.metric("Pending Counts", len(st.session_state.temp_counts))
            
            # Show recent counts
            recent_counts = audit_service.get_recent_counts(selected_tx['id'], limit=5)
            if recent_counts:
                st.markdown("#### Recent Saved Counts")
                for count in recent_counts:
                    col1, col2, col3 = st.columns([3, 1, 2])
                    with col1:
                        st.text(f"{count.get('product_name', 'N/A')[:40]}...")
                        st.caption(f"Batch: {count.get('batch_no', 'N/A')}")
                    with col2:
                        st.text(f"Qty: {count.get('actual_quantity', 0):.0f}")
                    with col3:
                        st.text(pd.to_datetime(count.get('counted_date')).strftime('%H:%M'))
        
        except Exception as e:
            st.error(f"Error loading progress: {str(e)}")

def show_transactions_page():
    """Transactions management page"""
    st.subheader("📝 My Audit Transactions")
    
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

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry point"""
    init_session_state()
    
    # Sidebar with user selection
    with st.sidebar:
        st.markdown("### 👤 User Selection")
        
        # Get all users
        users = get_all_users()
        
        if users:
            # Create user options
            user_options = {f"{u['username']} - {u['full_name']} ({u['user_role']})": u['user_id'] for u in users}
            
            # Add default admin option if not in list
            if 1 not in [u['user_id'] for u in users]:
                user_options = {"admin - Administrator (admin)": 1, **user_options}
            
            # User selector
            selected_user_key = st.selectbox(
                "Select User",
                list(user_options.keys()),
                index=0,
                help="Select user to perform audit operations"
            )
            
            # Update selected user
            selected_user_id = user_options[selected_user_key]
            if st.session_state.selected_user_id != selected_user_id:
                st.session_state.selected_user_id = selected_user_id
                st.rerun()
        else:
            st.warning("No users found, using default admin")
        
        # Get current user info
        current_user = get_current_user()
        
        st.markdown("---")
        st.markdown("### 📊 Current User Info")
        st.write(f"**Name:** {current_user['full_name']}")
        st.write(f"**Username:** {current_user['username']}")
        st.write(f"**Role:** {current_user['user_role']}")
        if current_user.get('email'):
            st.write(f"**Email:** {current_user['email']}")
        
        st.markdown("---")
        
        # Performance info
        st.markdown("### ⚡ Performance Mode")
        st.info("Fast Counting Mode Active")
        st.caption("• Minimal page reloads")
        st.caption("• Batch operations")
        st.caption("• Multiple counts per batch")
    
    # Main content
    st.title("📦 Warehouse Audit System")
    st.caption("Simplified Version - No Authentication Required")
    
    # Show current user in main area
    st.info(f"👤 Operating as: **{current_user['full_name']}** ({current_user['username']}) - Role: {current_user['user_role']}")
    
    # Main tabs
    tab1, tab2 = st.tabs(["📝 Transactions", "🚀 Fast Counting"])
    
    with tab1:
        show_transactions_page()
    
    with tab2:
        counting_page()

if __name__ == "__main__":
    main()
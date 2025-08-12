# main_warehouse_physical_count.py - Simplified Product Selection
# Records both existing products found in warehouse and new items not in ERP
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging
import time
from typing import Dict, List, Optional, Tuple
import json

# Import existing utilities
from utils.auth import AuthManager
from utils.config import config
from utils.db import get_db_engine

# Import services
from audit_service import AuditService, AuditException
from audit_queries import AuditQueries

# Import SQLAlchemy text
from sqlalchemy import text

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Warehouse Physical Count",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize services
auth = AuthManager()
audit_service = AuditService()

# ============== SESSION STATE INITIALIZATION ==============
def init_session_state():
    """Initialize all session state variables"""
    if 'new_items_list' not in st.session_state:
        st.session_state.new_items_list = []
    
    if 'item_counter' not in st.session_state:
        st.session_state.item_counter = 0
    
    if 'last_save_time' not in st.session_state:
        st.session_state.last_save_time = None
    
    if 'show_preview' not in st.session_state:
        st.session_state.show_preview = True
    
    if 'default_location' not in st.session_state:
        st.session_state.default_location = {'zone': '', 'rack': '', 'bin': ''}
    
    if 'form_data' not in st.session_state:
        st.session_state.form_data = {}

# ============== SIMPLIFIED CACHE FUNCTIONS ==============

@st.cache_data(ttl=3600)
def get_all_products():
    """Get all active products from database"""
    try:
        query = """
        SELECT 
            p.id,
            p.name as product_name,
            p.pt_code,
            COALESCE(p.legacy_pt_code, '') as legacy_code,
            COALESCE(p.package_size, '') as package_size,
            p.brand_id,
            COALESCE(b.brand_name, '') as brand_name
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE p.delete_flag = 0
        ORDER BY p.pt_code, p.name
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            products = [dict(row._mapping) for row in result.fetchall()]
            logger.info(f"Loaded {len(products)} products from database")
            return products
    except Exception as e:
        logger.error(f"Error getting products: {e}")
        st.error(f"Failed to load products: {str(e)}")
        return []

# ============== ITEM MANAGEMENT FUNCTIONS ==============

def add_new_item(item_data: Dict):
    """Add item to list with validation"""
    # Validate required fields
    if not item_data.get('product_name'):
        raise ValueError("Product name is required")
    
    if item_data.get('actual_quantity', 0) <= 0:
        raise ValueError("Quantity must be greater than 0")
    
    # Handle date conversion
    if item_data.get('expired_date') and hasattr(item_data['expired_date'], 'isoformat'):
        item_data['expired_date'] = item_data['expired_date'].isoformat()
    
    # Add metadata
    item_data['temp_id'] = f"new_{st.session_state.item_counter}_{int(time.time() * 1000)}"
    item_data['added_time'] = datetime.now()
    
    # Add to list
    st.session_state.new_items_list.append(item_data)
    st.session_state.item_counter += 1
    
    return item_data['temp_id']

def remove_item(temp_id: str):
    """Remove item from list"""
    st.session_state.new_items_list = [
        item for item in st.session_state.new_items_list 
        if item.get('temp_id') != temp_id
    ]

def clear_all_items():
    """Clear all items from list"""
    st.session_state.new_items_list = []
    st.session_state.item_counter = 0

def get_items_summary() -> Dict:
    """Get summary statistics for physical items"""
    if not st.session_state.new_items_list:
        return {
            'total_items': 0,
            'total_quantity': 0,
            'unique_products': 0,
            'total_batches': 0,
            'items_in_erp': 0,
            'items_not_in_erp': 0
        }
    
    total_quantity = sum(item.get('actual_quantity', 0) for item in st.session_state.new_items_list)
    unique_products = len(set(item.get('product_name', '').upper() for item in st.session_state.new_items_list))
    total_batches = len(set((item.get('product_name', ''), item.get('batch_no', '')) 
                           for item in st.session_state.new_items_list))
    
    # Count by whether product exists in ERP
    items_in_erp = sum(1 for item in st.session_state.new_items_list 
                       if item.get('product_id') is not None)
    items_not_in_erp = len(st.session_state.new_items_list) - items_in_erp
    
    return {
        'total_items': len(st.session_state.new_items_list),
        'total_quantity': total_quantity,
        'unique_products': unique_products,
        'total_batches': total_batches,
        'items_in_erp': items_in_erp,
        'items_not_in_erp': items_not_in_erp
    }

def save_items_to_db(transaction_id: int) -> Tuple[int, List[str]]:
    """Save all physical items found in warehouse to database"""
    if not st.session_state.new_items_list:
        return 0, ["No items to save"]
    
    # Prepare data for database
    count_list = []
    for item in st.session_state.new_items_list:
        # Handle expired_date conversion
        expired_date = item.get('expired_date')
        if expired_date:
            try:
                if isinstance(expired_date, str):
                    expired_date = datetime.fromisoformat(expired_date).date()
            except:
                logger.warning(f"Failed to parse expiry date: {expired_date}")
                expired_date = None
        
        # Create appropriate notes based on whether product exists in ERP
        if item.get('product_id'):
            # Physical item that exists in ERP product master
            actual_notes = f"PHYSICAL COUNT - IN ERP: {item.get('reference_pt_code', '')} - {item.get('product_name', '')} - {item.get('notes', '')}"
        else:
            # Physical item NOT in ERP product master
            actual_notes = f"PHYSICAL COUNT - NOT IN ERP: {item.get('product_name', '')} - {item.get('brand', '')} - {item.get('notes', '')}"
        
        count_data = {
            'transaction_id': transaction_id,
            'product_id': item.get('product_id'),  # Will be actual ID or None
            'batch_no': item.get('batch_no', ''),
            'expired_date': expired_date,
            'zone_name': item.get('zone_name', ''),
            'rack_name': item.get('rack_name', ''),
            'bin_name': item.get('bin_name', ''),
            'location_notes': item.get('location_notes', ''),
            'system_quantity': 0,  # Always 0 for physical count items not in ERP inventory
            'system_value_usd': 0,  # Always 0 for physical count items not in ERP inventory
            'actual_quantity': item.get('actual_quantity', 0),
            'actual_notes': actual_notes,
            'is_new_item': True,  # ALWAYS TRUE - all are physical items not in ERP inventory
            'created_by_user_id': st.session_state.user_id
        }
        count_list.append(count_data)
    
    # Save using batch save
    saved, errors = audit_service.save_batch_counts(count_list)
    
    if saved > 0:
        st.session_state.last_save_time = datetime.now()
        clear_all_items()
    
    return saved, errors

# ============== UI COMPONENTS ==============

def show_header():
    """Display header with user info"""
    col1, col2, col3 = st.columns([6, 2, 2])
    
    with col1:
        st.title("📦 Warehouse Physical Count - Items NOT in ERP Inventory")
        st.caption("Record physical items found in warehouse that are not in ERP inventory data")
    
    with col2:
        st.metric("User", auth.get_user_display_name())
    
    with col3:
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()

def show_summary_bar():
    """Display summary statistics bar"""
    summary = get_items_summary()
    
    if summary['total_items'] > 0:
        col1, col2, col3, col4, col5 = st.columns([2, 1.5, 1.5, 1.5, 2.5])
        
        with col1:
            status_color = "🟡" if summary['total_items'] < 10 else "🔴"
            st.markdown(f"### {status_color} Pending Items")
        
        with col2:
            st.metric("Items", summary['total_items'], 
                     delta=f"/{20} max" if summary['total_items'] < 20 else "MAX")
        
        with col3:
            st.metric("Quantity", f"{summary['total_quantity']:.0f}")
        
        with col4:
            st.metric("Products", summary['unique_products'])
        
        with col5:
            col_save, col_clear, col_export = st.columns(3)
            
            with col_save:
                save_disabled = summary['total_items'] == 0
                if st.button("💾 Save All", use_container_width=True, 
                           type="primary", disabled=save_disabled):
                    st.session_state.trigger_save = True
            
            with col_clear:
                if st.button("🗑️ Clear", use_container_width=True):
                    st.session_state.show_clear_confirm = True
            
            with col_export:
                if st.button("📥 Export", use_container_width=True):
                    export_items_to_csv()
        
        st.markdown("---")

def show_transaction_selector():
    """Show transaction selector"""
    st.markdown("### 📋 Select Transaction")
    
    # Get user's draft transactions
    if 'selected_session_id' not in st.session_state:
        sessions = audit_service.get_sessions_by_status('in_progress')
        if sessions:
            st.session_state.selected_session_id = sessions[0]['id']
    
    if 'selected_session_id' in st.session_state:
        transactions = audit_service.get_user_transactions(
            st.session_state.selected_session_id,
            st.session_state.user_id,
            status='draft'
        )
        
        if transactions:
            tx_options = {
                f"{tx['transaction_name']} ({tx['transaction_code']})": tx
                for tx in transactions
            }
            
            selected_key = st.selectbox(
                "Select your draft transaction:",
                options=list(tx_options.keys()),
                help="Select the transaction to add new items to"
            )
            
            selected_tx = tx_options[selected_key]
            st.session_state.selected_tx_id = selected_tx['id']
            
            # Show transaction info
            col1, col2, col3 = st.columns(3)
            with col1:
                st.info(f"📍 Warehouse: {selected_tx.get('warehouse_name', 'N/A')}")
            with col2:
                st.info(f"🏷️ Zones: {selected_tx.get('assigned_zones', 'All')}")
            with col3:
                st.info(f"📦 Items Counted: {selected_tx.get('total_items_counted', 0)}")
            
            return selected_tx['id']
        else:
            st.warning("⚠️ No draft transactions available. Please create one first.")
            return None
    else:
        st.warning("⚠️ No active session found. Please select a session.")
        return None

def show_entry_form():
    """Show simplified entry form"""
    st.markdown("### ✏️ Add Item")
    
    # Load all products once
    all_products = get_all_products()
    
    # Main form
    with st.form("new_item_form", clear_on_submit=True):
        # Product selection
        st.markdown("**Product (if exists in ERP)**")
        
        # Create options for selectbox with ALL products
        product_options = {"-- Not in ERP / New Product --": None}
        
        # Add all products to options
        for p in all_products:
            display_name = f"{p['pt_code']} - {p['product_name']}"
            if p.get('brand_name'):
                display_name += f" | {p['brand_name']}"
            if p.get('package_size'):
                display_name += f" ({p['package_size']})"
            product_options[display_name] = p
        
        # Product selector
        selected_product_key = st.selectbox(
            "Select Product",
            options=list(product_options.keys()),
            help="Type to search in the dropdown. Select 'Not in ERP' if product doesn't exist",
            index=0  # Default to first option
        )
        
        selected_product = product_options.get(selected_product_key)
        
        # Show selected product info
        if selected_product:
            st.success(f"✅ ERP Product Selected: {selected_product['pt_code']} - {selected_product['product_name']}")
        else:
            st.info("ℹ️ Product not in ERP - Enter details manually below")
        
        # Form fields
        col1, col2 = st.columns(2)
        
        with col1:
            # Product name - auto-filled or manual entry
            if selected_product:
                product_name = st.text_input(
                    "Product Name*", 
                    value=selected_product.get('product_name', ''),
                    disabled=True
                )
                actual_product_name = selected_product.get('product_name', '')
            else:
                product_name = st.text_input(
                    "Product Name*", 
                    placeholder="Enter product name"
                )
                actual_product_name = product_name
            
            # Brand - auto-filled or manual entry
            if selected_product:
                brand_name = st.text_input(
                    "Brand", 
                    value=selected_product.get('brand_name', ''),
                    disabled=True
                )
                actual_brand = selected_product.get('brand_name', '')
            else:
                brand_name = st.text_input(
                    "Brand", 
                    placeholder="Enter brand name"
                )
                actual_brand = brand_name
            
            batch_no = st.text_input("Batch Number", placeholder="Enter batch number")
            
            # Package size
            if selected_product:
                package_size = st.text_input(
                    "Package Size", 
                    value=selected_product.get('package_size', ''),
                    disabled=True
                )
            else:
                package_size = st.text_input(
                    "Package Size", 
                    placeholder="e.g., 100 tablets, 500ml"
                )
        
        with col2:
            quantity = st.number_input(
                "Quantity*", 
                min_value=0.0, 
                step=1.0, 
                format="%.2f",
                help="Physical quantity found in warehouse"
            )
            
            expired_date = st.date_input(
                "Expiry Date", 
                value=None,
                help="Leave empty if no expiry date"
            )
            
            # Location inputs
            st.markdown("**Location**")
            col_z, col_r, col_b = st.columns(3)
            with col_z:
                zone = st.text_input("Zone", value=st.session_state.default_location['zone'])
            with col_r:
                rack = st.text_input("Rack", value=st.session_state.default_location['rack'])
            with col_b:
                bin_name = st.text_input("Bin", value=st.session_state.default_location['bin'])
        
        notes = st.text_area(
            "Additional Notes", 
            placeholder="Any observations, damage, special conditions, etc."
        )
        
        # Submit buttons
        col_submit, col_reset = st.columns([3, 1])
        
        with col_submit:
            submitted = st.form_submit_button(
                "➕ Add Physical Item",
                use_container_width=True,
                type="primary",
                disabled=len(st.session_state.new_items_list) >= 20
            )
        
        with col_reset:
            reset = st.form_submit_button("🔄 Reset Form", use_container_width=True)
        
        # Handle form submission
        if submitted:
            if not actual_product_name:
                st.error("❌ Product name is required!")
            elif quantity <= 0:
                st.error("❌ Quantity must be greater than 0!")
            elif not zone:
                st.error("❌ Zone is required for location!")
            else:
                try:
                    # Prepare item data
                    if selected_product:
                        # Product exists in ERP master data
                        product_id = selected_product['id']
                        notes_prefix = f"IN ERP: {selected_product['pt_code']} - "
                    else:
                        # Product not in ERP master data
                        product_id = None
                        notes_prefix = f"NOT IN ERP: "
                    
                    item_data = {
                        'product_name': actual_product_name,
                        'brand': actual_brand,
                        'batch_no': batch_no,
                        'package_size': package_size,
                        'actual_quantity': quantity,
                        'expired_date': expired_date,
                        'zone_name': zone,
                        'rack_name': rack,
                        'bin_name': bin_name,
                        'notes': notes_prefix + notes if notes else notes_prefix + "Physical count",
                        'product_id': product_id,
                        'is_new_item': True,  # Always TRUE - all are physical items not in ERP inventory
                        'reference_pt_code': selected_product['pt_code'] if selected_product else None,
                        'created_by_user_id': st.session_state.user_id
                    }
                    
                    add_new_item(item_data)
                    
                    # Success message
                    if product_id:
                        st.success(f"✅ Added: {selected_product['pt_code']} - {actual_product_name} (Qty: {quantity})")
                    else:
                        st.success(f"✅ Added: {actual_product_name} - NOT IN ERP (Qty: {quantity})")
                    
                    # Update default location
                    st.session_state.default_location = {'zone': zone, 'rack': rack, 'bin': bin_name}
                    
                    time.sleep(1)
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        
        if reset:
            st.rerun()

def show_items_preview():
    """Display preview of pending items"""
    if not st.session_state.new_items_list:
        return
    
    st.markdown("### 📋 Pending Items")
    
    # Display as a table
    items_data = []
    for item in st.session_state.new_items_list:
        items_data.append({
            'Type': '📦 ERP' if item.get('product_id') else '❓ New',
            'Product': item.get('product_name', ''),
            'PT Code': item.get('reference_pt_code', '-'),
            'Brand': item.get('brand', '-'),
            'Batch': item.get('batch_no', '-'),
            'Quantity': f"{item.get('actual_quantity', 0):.0f}",
            'Location': f"{item.get('zone_name', '')}-{item.get('rack_name', '')}-{item.get('bin_name', '')}",
            'temp_id': item.get('temp_id')
        })
    
    # Create DataFrame
    df = pd.DataFrame(items_data)
    
    # Display with action column
    for idx, row in df.iterrows():
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1, 3, 2, 2, 2, 2, 2, 1])
        
        with col1:
            st.write(row['Type'])
        with col2:
            st.write(row['Product'])
        with col3:
            st.write(row['PT Code'])
        with col4:
            st.write(row['Brand'])
        with col5:
            st.write(row['Batch'])
        with col6:
            st.write(row['Quantity'])
        with col7:
            st.write(row['Location'])
        with col8:
            if st.button("🗑️", key=f"del_{row['temp_id']}", help="Remove"):
                remove_item(row['temp_id'])
                st.rerun()
        
        if idx < len(df) - 1:
            st.divider()

def export_items_to_csv():
    """Export items to CSV"""
    if not st.session_state.new_items_list:
        return
    
    # Create DataFrame
    df_data = []
    for item in st.session_state.new_items_list:
        expiry_date = item.get('expired_date', '')
        if expiry_date:
            try:
                if isinstance(expiry_date, str) and expiry_date != '':
                    expiry_date = datetime.fromisoformat(expiry_date).strftime('%Y-%m-%d')
            except:
                pass
        
        df_data.append({
            'ERP Status': 'In ERP Master' if item.get('product_id') else 'Not in ERP',
            'Product ID': item.get('product_id', ''),
            'Product Name': item.get('product_name', ''),
            'PT Code': item.get('reference_pt_code', ''),
            'Brand': item.get('brand', ''),
            'Batch Number': item.get('batch_no', ''),
            'Quantity': item.get('actual_quantity', 0),
            'Expiry Date': expiry_date,
            'Zone': item.get('zone_name', ''),
            'Rack': item.get('rack_name', ''),
            'Bin': item.get('bin_name', ''),
            'Notes': item.get('notes', ''),
            'Added Time': item.get('added_time', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('added_time') else ''
        })
    
    df = pd.DataFrame(df_data)
    csv = df.to_csv(index=False)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"warehouse_physical_items_{timestamp}.csv",
        mime="text/csv"
    )

def handle_save_action(transaction_id: int):
    """Handle save action with progress"""
    if 'trigger_save' in st.session_state and st.session_state.trigger_save:
        st.session_state.trigger_save = False
        
        with st.spinner(f"Saving {len(st.session_state.new_items_list)} items..."):
            progress_bar = st.progress(0)
            
            # Simulate progress
            for i in range(50):
                progress_bar.progress(i / 100)
                time.sleep(0.01)
            
            # Save to database
            saved, errors = save_items_to_db(transaction_id)
            
            progress_bar.progress(100)
            time.sleep(0.5)
            
            if errors and saved == 0:
                st.error(f"❌ Failed to save items")
                for error in errors[:3]:
                    st.caption(f"• {error}")
            elif errors and saved > 0:
                st.warning(f"⚠️ Saved {saved} items with {len(errors)} errors")
            else:
                st.success(f"✅ Successfully saved {saved} items!")
                st.balloons()
                time.sleep(1)
                st.rerun()

def handle_clear_confirmation():
    """Handle clear all confirmation"""
    if 'show_clear_confirm' in st.session_state and st.session_state.show_clear_confirm:
        st.session_state.show_clear_confirm = False
        
        with st.container():
            st.warning("⚠️ Are you sure you want to clear all pending items?")
            col1, col2, col3 = st.columns([1, 1, 3])
            
            with col1:
                if st.button("✅ Yes, Clear All", type="primary"):
                    clear_all_items()
                    st.success("✅ All items cleared!")
                    time.sleep(0.5)
                    st.rerun()
            
            with col2:
                if st.button("❌ Cancel"):
                    st.rerun()

def show_statistics():
    """Show statistics and analytics"""
    st.markdown("### 📊 Session Statistics")
    
    summary = get_items_summary()
    
    # Main metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Items", summary['total_items'])
    
    with col2:
        st.metric("Total Quantity", f"{summary['total_quantity']:.0f}")
    
    with col3:
        st.metric("Unique Products", summary['unique_products'])
    
    with col4:
        st.metric("Total Batches", summary['total_batches'])
    
    # Breakdown by type
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("📦 Items in ERP Master", summary['items_in_erp'], 
                  help="Physical items that exist in ERP product master")
    
    with col2:
        st.metric("❓ Items NOT in ERP", summary['items_not_in_erp'],
                  help="Physical items not found in ERP product master")
    
    if st.session_state.new_items_list:
        # Product distribution chart
        st.markdown("#### 📈 Top Products by Quantity")
        
        product_qty = {}
        for item in st.session_state.new_items_list:
            product = item.get('product_name', 'Unknown')
            qty = item.get('actual_quantity', 0)
            product_qty[product] = product_qty.get(product, 0) + qty
        
        sorted_products = sorted(product_qty.items(), key=lambda x: x[1], reverse=True)[:10]
        
        if sorted_products:
            df = pd.DataFrame(sorted_products, columns=['Product', 'Total Quantity'])
            st.bar_chart(df.set_index('Product'))

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry for warehouse physical count"""
    init_session_state()
    
    # Check authentication
    if not auth.check_session():
        show_login_page()
    else:
        show_main_app()

def show_login_page():
    """Display login page"""
    st.title("🔐 Login - Warehouse Physical Count")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter username")
            password = st.text_input("Password", type="password", placeholder="Enter password")
            submit = st.form_submit_button("Login", use_container_width=True)
            
            if submit and username and password:
                success, result = auth.authenticate(username, password)
                if success:
                    auth.login(result)
                    st.success("✅ Login successful!")
                    st.rerun()
                else:
                    st.error(f"❌ {result.get('error', 'Login failed')}")

def show_main_app():
    """Main application interface"""
    # Header
    show_header()
    
    # Summary bar
    show_summary_bar()
    
    # Transaction selector
    transaction_id = show_transaction_selector()
    
    if transaction_id:
        # Main content in tabs
        tab1, tab2, tab3 = st.tabs(["📝 Add Items", "📋 Review & Save", "📊 Statistics"])
        
        with tab1:
            # Entry form
            show_entry_form()
            
            # Show compact preview in sidebar
            with st.sidebar:
                st.markdown("### 📦 Quick Preview")
                summary = get_items_summary()
                st.metric("Pending Items", summary['total_items'])
                
                if st.session_state.new_items_list:
                    for item in st.session_state.new_items_list[-5:]:  # Show last 5
                        status = "📦" if item.get('product_id') else "❓"
                        st.caption(f"{status} {item['product_name'][:20]}... - Qty: {item['actual_quantity']:.0f}")
        
        with tab2:
            # Items preview
            show_items_preview()
            
            # Save section
            if st.session_state.new_items_list:
                st.markdown("---")
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.info(f"💡 Ready to save {len(st.session_state.new_items_list)} items to transaction")
                
                with col2:
                    if st.button("💾 Save to Database", use_container_width=True, type="primary"):
                        st.session_state.trigger_save = True
        
        with tab3:
            show_statistics()
        
        # Handle save action
        handle_save_action(transaction_id)
        
        # Handle clear confirmation
        handle_clear_confirmation()
    
    # Footer
    st.markdown("---")
    if st.session_state.last_save_time:
        st.caption(f"Last save: {st.session_state.last_save_time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()
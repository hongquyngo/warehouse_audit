# main_new_item_optimized.py - Optimized New Item Management for Warehouse Audit
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
    page_title="New Item Management - Warehouse Audit",
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
    
    if 'selected_brand_id' not in st.session_state:
        st.session_state.selected_brand_id = None
    
    if 'selected_product_id' not in st.session_state:
        st.session_state.selected_product_id = None

# ============== CACHE FUNCTIONS ==============

@st.cache_data(ttl=3600)
def get_all_brands():
    """Get all active brands from database"""
    try:
        query = """
        SELECT 
            id, 
            brand_name,
            COALESCE(company_id, 0) as company_id
        FROM brands 
        WHERE delete_flag = 0 
        AND brand_name IS NOT NULL
        AND brand_name != ''
        ORDER BY brand_name
        """
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query))
            brands = [dict(row._mapping) for row in result.fetchall()]
            logger.info(f"Loaded {len(brands)} brands from database")
            
            # Debug: Show first few brands
            if brands:
                logger.info(f"Sample brands: {[b['brand_name'] for b in brands[:5]]}")
            
            return brands
    except Exception as e:
        logger.error(f"Error getting brands: {e}")
        st.error(f"Failed to load brands: {str(e)}")
        return []

@st.cache_data(ttl=1800)
def get_products_by_brand(brand_id: Optional[int] = None):
    """Get products filtered by brand if provided"""
    try:
        if brand_id:
            query = """
            SELECT 
                p.id,
                p.name as product_name,
                p.pt_code,
                COALESCE(p.legacy_pt_code, '') as legacy_pt_code,
                COALESCE(p.package_size, '') as package_size,
                p.brand_id,
                b.brand_name
            FROM products p
            LEFT JOIN brands b ON p.brand_id = b.id
            WHERE p.delete_flag = 0
            AND p.brand_id = :brand_id
            ORDER BY p.pt_code, p.name
            """
            params = {'brand_id': brand_id}
        else:
            # Return empty list if no brand selected
            return []
        
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            products = [dict(row._mapping) for row in result.fetchall()]
            logger.info(f"Found {len(products)} products for brand_id={brand_id}")
            return products
    except Exception as e:
        logger.error(f"Error getting products: {e}")
        st.error(f"Database error: {str(e)}")
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
    """Get summary statistics"""
    if not st.session_state.new_items_list:
        return {
            'total_items': 0,
            'total_quantity': 0,
            'unique_products': 0,
            'total_batches': 0
        }
    
    total_quantity = sum(item.get('actual_quantity', 0) for item in st.session_state.new_items_list)
    unique_products = len(set(item.get('product_name', '').upper() for item in st.session_state.new_items_list))
    total_batches = len(set((item.get('product_name', ''), item.get('batch_no', '')) 
                           for item in st.session_state.new_items_list))
    
    return {
        'total_items': len(st.session_state.new_items_list),
        'total_quantity': total_quantity,
        'unique_products': unique_products,
        'total_batches': total_batches
    }

def save_items_to_db(transaction_id: int) -> Tuple[int, List[str]]:
    """Save all items to database"""
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
                    # Convert ISO format string to date
                    expired_date = datetime.fromisoformat(expired_date).date()
            except:
                logger.warning(f"Failed to parse expiry date: {expired_date}")
                expired_date = None
        
        count_data = {
            'transaction_id': transaction_id,
            'product_id': None,  # New item, no product_id
            'batch_no': item.get('batch_no', ''),
            'expired_date': expired_date,
            'zone_name': item.get('zone_name', ''),
            'rack_name': item.get('rack_name', ''),
            'bin_name': item.get('bin_name', ''),
            'location_notes': item.get('location_notes', ''),
            'system_quantity': 0,  # New item has no system quantity
            'system_value_usd': 0,
            'actual_quantity': item.get('actual_quantity', 0),
            'actual_notes': f"NEW ITEM: {item.get('product_name', '')} - {item.get('brand', '')} - {item.get('notes', '')}",
            'is_new_item': True,
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
        st.title("📦 New Item Management")
        st.caption("Add items found in warehouse but not in ERP system")
    
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
    """Show optimized entry form"""
    st.markdown("### ✏️ Add New Item")
    
    # Direct to detailed form (no mode selection)
    show_detailed_entry_form()



def show_detailed_entry_form():
    """Detailed entry form with dynamic brand/product selection"""
    
    # Get brands and products
    brands = get_all_brands()
    
    # Debug
    if not brands:
        st.warning("⚠️ No brands found in database. Please check database connection.")
        logger.warning("No brands returned from database")
    else:
        st.caption(f"Found {len(brands)} brands in database")
    
    # Create brand options - filter out any None values
    brand_options = {"-- Select Brand --": None}
    for brand in brands:
        if brand.get('brand_name'):  # Only add if brand_name exists
            brand_options[brand['brand_name']] = brand['id']
    
    # Brand/Product selection row
    col_brand, col_product = st.columns(2)
    
    with col_brand:
        selected_brand_name = st.selectbox(
            "Brand",
            options=list(brand_options.keys()),
            key="brand_selector",
            help="Select a brand to filter products"
        )
        
        selected_brand_id = brand_options[selected_brand_name]
        
        # Update session state if brand changed
        if selected_brand_id != st.session_state.selected_brand_id:
            st.session_state.selected_brand_id = selected_brand_id
            st.session_state.selected_product_id = None  # Reset product selection
    
    with col_product:
        # Get products based on brand filter
        if st.session_state.selected_brand_id:
            products = get_products_by_brand(st.session_state.selected_brand_id)
        else:
            products = []
        
        # Debug
        if st.session_state.selected_brand_id and not products:
            st.caption("No products found for selected brand")
        
        # Create product options
        product_options = {"-- Select brand first --": None}
        if products:
            product_options = {"-- Select Product (Optional) --": None}
            for p in products:
                display_name = f"{p['pt_code']} - {p['product_name']}"
                if p.get('package_size'):
                    display_name += f" ({p['package_size']})"
                product_options[display_name] = p
        
        selected_product_key = st.selectbox(
            "Product (Optional - for reference)",
            options=list(product_options.keys()),
            key="product_selector",
            help="Select a product for reference (optional)"
        )
        
        selected_product = product_options.get(selected_product_key)
    
    # Main form
    with st.form("detailed_entry_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            # Product name - pre-fill if product selected
            default_product_name = ""
            default_package_size = ""
            
            if selected_product:
                default_product_name = selected_product.get('product_name', '')
                default_package_size = selected_product.get('package_size', '')
            
            product_name = st.text_input(
                "Product Name*", 
                value=default_product_name,
                placeholder="Enter product name"
            )
            
            # Brand name - show selected brand
            brand_name = selected_brand_name if selected_brand_name != "-- Select Brand --" else ""
            brand_display = st.text_input(
                "Brand", 
                value=brand_name,
                disabled=True,
                help="Selected from dropdown above"
            )
            
            batch_no = st.text_input("Batch Number", placeholder="Enter batch number")
            package_size = st.text_input(
                "Package Size", 
                value=default_package_size,
                placeholder="e.g., 100 tablets"
            )
        
        with col2:
            quantity = st.number_input("Quantity*", min_value=0.0, step=1.0, format="%.2f")
            expired_date = st.date_input("Expiry Date", value=None)
            
            # Location inputs
            st.markdown("**Location**")
            col_z, col_r, col_b = st.columns(3)
            with col_z:
                zone = st.text_input("Zone", value=st.session_state.default_location['zone'])
            with col_r:
                rack = st.text_input("Rack", value=st.session_state.default_location['rack'])
            with col_b:
                bin_name = st.text_input("Bin", value=st.session_state.default_location['bin'])
        
        notes = st.text_area("Additional Notes", placeholder="Any observations or special conditions")
        
        # Reference info if product selected
        if selected_product:
            st.info(f"📋 Reference: {selected_product['pt_code']} - This is a NEW physical item not in system inventory")
        
        # Submit button
        col_submit, col_reset = st.columns([3, 1])
        
        with col_submit:
            submitted = st.form_submit_button(
                "➕ Add to List",
                use_container_width=True,
                type="primary",
                disabled=len(st.session_state.new_items_list) >= 20
            )
        
        with col_reset:
            reset = st.form_submit_button("🔄 Reset", use_container_width=True)
        
        if submitted and product_name and quantity > 0:
            try:
                item_data = {
                    'product_name': product_name,
                    'brand': brand_name,
                    'batch_no': batch_no,
                    'package_size': package_size,
                    'actual_quantity': quantity,
                    'expired_date': expired_date,
                    'zone_name': zone,
                    'rack_name': rack,
                    'bin_name': bin_name,
                    'notes': notes,
                    'reference_product_id': selected_product['id'] if selected_product else None,
                    'reference_pt_code': selected_product['pt_code'] if selected_product else None,
                    'created_by_user_id': st.session_state.user_id
                }
                
                add_new_item(item_data)
                st.success(f"✅ Added: {product_name}")
                
                # Update default location
                if zone:
                    st.session_state.default_location = {'zone': zone, 'rack': rack, 'bin': bin_name}
                
                # Clear selections
                st.session_state.selected_product_id = None
                
                time.sleep(0.5)
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
        
        if reset:
            # Clear selections
            st.session_state.selected_brand_id = None
            st.session_state.selected_product_id = None
            st.rerun()

def show_items_preview():
    """Display preview of pending items"""
    if not st.session_state.new_items_list:
        return
    
    st.markdown("### 📋 Pending Items")
    
    # Display options
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        view_mode = st.radio("View", ["Compact", "Detailed"], horizontal=True)
    with col2:
        sort_by = st.selectbox("Sort by", ["Added Time", "Product Name", "Quantity"])
    with col3:
        st.write("")  # Spacer
    
    # Sort items
    items = st.session_state.new_items_list.copy()
    if sort_by == "Product Name":
        items.sort(key=lambda x: x.get('product_name', ''))
    elif sort_by == "Quantity":
        items.sort(key=lambda x: x.get('actual_quantity', 0), reverse=True)
    else:  # Added Time
        items.reverse()  # Most recent first
    
    # Display items
    if view_mode == "Compact":
        show_compact_preview(items)
    else:
        show_detailed_preview(items)

def show_compact_preview(items: List[Dict]):
    """Compact view of items"""
    for idx, item in enumerate(items):
        col1, col2, col3, col4, col5 = st.columns([3, 1.5, 1.5, 1.5, 1])
        
        with col1:
            product_display = f"**{item['product_name']}**"
            if item.get('brand'):
                product_display += f" - {item['brand']}"
            st.write(product_display)
            
            # Show reference if exists
            if item.get('reference_pt_code'):
                st.caption(f"📋 Ref: {item['reference_pt_code']}")
            
            # Location info
            location = f"{item.get('zone_name', '')}-{item.get('rack_name', '')}-{item.get('bin_name', '')}".strip('-')
            if location and location != '--':
                st.caption(f"📍 {location}")
        
        with col2:
            st.write(f"Qty: **{item['actual_quantity']:.0f}**")
        
        with col3:
            if item.get('batch_no'):
                st.caption(f"Batch: {item['batch_no']}")
        
        with col4:
            added_time = item.get('added_time', datetime.now())
            time_diff = datetime.now() - added_time
            if time_diff.seconds < 60:
                st.caption("Just now")
            elif time_diff.seconds < 3600:
                st.caption(f"{time_diff.seconds // 60}m ago")
            else:
                st.caption(f"{time_diff.seconds // 3600}h ago")
        
        with col5:
            if st.button("🗑️", key=f"del_{item['temp_id']}", help="Remove this item"):
                remove_item(item['temp_id'])
                st.rerun()
        
        if idx < len(items) - 1:
            st.markdown("---")

def show_detailed_preview(items: List[Dict]):
    """Detailed view of items"""
    for idx, item in enumerate(items):
        with st.expander(f"{item['product_name']} - Qty: {item['actual_quantity']:.0f}", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Product:** {item['product_name']}")
                st.write(f"**Brand:** {item.get('brand', 'N/A')}")
                st.write(f"**Batch:** {item.get('batch_no', 'N/A')}")
                st.write(f"**Package Size:** {item.get('package_size', 'N/A')}")
                if item.get('reference_pt_code'):
                    st.write(f"**Reference PT Code:** {item['reference_pt_code']}")
            
            with col2:
                st.write(f"**Quantity:** {item['actual_quantity']:.2f}")
                
                # Handle expiry date display
                expiry = item.get('expired_date', 'N/A')
                if expiry and expiry != 'N/A':
                    try:
                        if isinstance(expiry, str):
                            expiry_date = datetime.fromisoformat(expiry).strftime('%Y-%m-%d')
                        else:
                            expiry_date = expiry
                        st.write(f"**Expiry:** {expiry_date}")
                    except:
                        st.write(f"**Expiry:** {expiry}")
                else:
                    st.write("**Expiry:** N/A")
                
                location = f"{item.get('zone_name', '')}-{item.get('rack_name', '')}-{item.get('bin_name', '')}".strip('-')
                st.write(f"**Location:** {location if location else 'N/A'}")
                st.write(f"**Notes:** {item.get('notes', 'N/A')}")
            
            if st.button(f"Remove", key=f"del_detail_{item['temp_id']}", type="secondary"):
                remove_item(item['temp_id'])
                st.rerun()

def export_items_to_csv():
    """Export items to CSV"""
    if not st.session_state.new_items_list:
        return
    
    # Create DataFrame
    df_data = []
    for item in st.session_state.new_items_list:
        # Handle expiry date formatting
        expiry_date = item.get('expired_date', '')
        if expiry_date:
            try:
                if isinstance(expiry_date, str) and expiry_date != '':
                    expiry_date = datetime.fromisoformat(expiry_date).strftime('%Y-%m-%d')
            except:
                pass  # Keep original value if conversion fails
        
        df_data.append({
            'Product Name': item.get('product_name', ''),
            'Brand': item.get('brand', ''),
            'Batch Number': item.get('batch_no', ''),
            'Quantity': item.get('actual_quantity', 0),
            'Expiry Date': expiry_date,
            'Zone': item.get('zone_name', ''),
            'Rack': item.get('rack_name', ''),
            'Bin': item.get('bin_name', ''),
            'Reference PT Code': item.get('reference_pt_code', ''),
            'Notes': item.get('notes', ''),
            'Added Time': item.get('added_time', '').strftime('%Y-%m-%d %H:%M:%S') if item.get('added_time') else ''
        })
    
    df = pd.DataFrame(df_data)
    csv = df.to_csv(index=False)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"new_items_{timestamp}.csv",
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

# ============== MAIN APPLICATION ==============

def main():
    """Main application entry"""
    init_session_state()
    
    # Test database connection first
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Test query
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
    except Exception as e:
        st.error(f"❌ Database connection failed: {str(e)}")
        st.stop()
    
    # Check authentication
    if not auth.check_session():
        show_login_page()
    else:
        show_main_app()

def show_login_page():
    """Display login page"""
    st.title("🔐 Login - New Item Management")
    
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
                        st.caption(f"• {item['product_name'][:20]}... - Qty: {item['actual_quantity']:.0f}")
                
                # Debug info
                with st.expander("🔧 Debug Info"):
                    st.write("Session State:")
                    st.write(f"- Brand ID: {st.session_state.get('selected_brand_id', 'None')}")
                    st.write(f"- Product ID: {st.session_state.get('selected_product_id', 'None')}")
                    st.write(f"- Items Count: {len(st.session_state.new_items_list)}")
                    
                    if st.button("🔄 Clear Cache", help="Clear cached data and reload"):
                        st.cache_data.clear()
                        st.success("Cache cleared!")
                        st.rerun()
        
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

def show_statistics():
    """Show statistics and analytics"""
    st.markdown("### 📊 Session Statistics")
    
    summary = get_items_summary()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Items", summary['total_items'])
    
    with col2:
        st.metric("Total Quantity", f"{summary['total_quantity']:.0f}")
    
    with col3:
        st.metric("Unique Products", summary['unique_products'])
    
    with col4:
        st.metric("Total Batches", summary['total_batches'])
    
    if st.session_state.new_items_list:
        # Product distribution
        st.markdown("#### 📈 Top Products by Quantity")
        
        # Group by product
        product_qty = {}
        for item in st.session_state.new_items_list:
            product = item.get('product_name', 'Unknown')
            qty = item.get('actual_quantity', 0)
            product_qty[product] = product_qty.get(product, 0) + qty
        
        # Sort and display top 10
        sorted_products = sorted(product_qty.items(), key=lambda x: x[1], reverse=True)[:10]
        
        df = pd.DataFrame(sorted_products, columns=['Product', 'Total Quantity'])
        st.bar_chart(df.set_index('Product'))
        
        # Location distribution
        st.markdown("#### 📍 Items by Location")
        
        location_count = {}
        for item in st.session_state.new_items_list:
            zone = item.get('zone_name', 'Unknown')
            location_count[zone] = location_count.get(zone, 0) + 1
        
        df_location = pd.DataFrame(location_count.items(), columns=['Zone', 'Count'])
        st.bar_chart(df_location.set_index('Zone'))

if __name__ == "__main__":
    main()
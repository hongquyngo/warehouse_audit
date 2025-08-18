# pages/counting.py - Inventory Counting Page
import streamlit as st
import pandas as pd
from datetime import datetime, date
import logging

# Import services
from utils.auth import AuthManager
from audit_service import AuditService
from typing import Dict, List

# Setup logging
logger = logging.getLogger(__name__)

# Initialize services
auth = AuthManager()
audit_service = AuditService()

# Page config
st.set_page_config(
    page_title="Inventory Counting",
    page_icon="📦",
    layout="wide"
)

# Constants
MAX_BATCH_COUNTS = 10

def main():
    """Main counting page"""
    # Check authentication
    if not auth.check_session():
        st.error("Please login first")
        st.stop()
    
    # Initialize temp counts in session state
    if 'temp_counts' not in st.session_state:
        st.session_state.temp_counts = []
    
    # Page header
    st.title("📦 Inventory Counting")
    
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
    
    # Check if transaction is selected
    if 'selected_tx_id' not in st.session_state:
        st.warning("⚠️ No transaction selected")
        if st.button("Go to Audit Management"):
            st.switch_page("pages/audit_management.py")
        st.stop()
    
    # Main counting interface
    show_counting_interface()

def show_counting_interface():
    """Main counting interface"""
    tx_id = st.session_state.selected_tx_id
    
    # Get transaction info
    try:
        tx_info = audit_service.get_transaction_info(tx_id)
        
        if not tx_info:
            st.error("Transaction not found")
            return
        
        # Check if transaction is draft
        if tx_info['status'] != 'draft':
            st.warning("⚠️ This transaction is already submitted")
            if st.button("Back to Audit Management"):
                st.switch_page("pages/audit_management.py")
            return
        
        # Display transaction info
        session_info = audit_service.get_session_info(tx_info['session_id'])
        warehouse_id = session_info['warehouse_id']
        
        # Transaction header
        col1, col2, col3 = st.columns([3, 2, 1])
        
        with col1:
            st.info(f"📋 **Transaction:** {tx_info['transaction_name']}")
            st.caption(f"Session: {session_info['session_name']}")
        
        with col2:
            progress = audit_service.get_transaction_progress(tx_id)
            st.metric("Items Counted", progress.get('items_counted', 0))
        
        with col3:
            if st.button("📊 Back", use_container_width=True):
                st.switch_page("pages/audit_management.py")
        
        # Tabs for different counting methods
        tab1, tab2, tab3 = st.tabs(["🔍 Single Count", "📋 Batch Count", "📊 Progress"])
        
        with tab1:
            single_count_tab(tx_id, warehouse_id)
        
        with tab2:
            batch_count_tab(tx_id, warehouse_id)
        
        with tab3:
            progress_tab(tx_id)
    
    except Exception as e:
        st.error(f"Error: {str(e)}")

def single_count_tab(tx_id: int, warehouse_id: int):
    """Single item counting"""
    st.subheader("🔍 Count Single Item")
    
    # Product search
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input("Search Product", placeholder="Enter PT code or product name")
    
    with col2:
        brand_filter = st.selectbox("Brand", ["All"] + get_warehouse_brands(warehouse_id))
    
    # Search products
    if search_term or brand_filter != "All":
        products = search_products(warehouse_id, search_term, brand_filter if brand_filter != "All" else "")
        
        if products:
            # Select product
            product_options = {
                f"{p['pt_code']} - {p['product_name']} [{p['brand']}]": p 
                for p in products[:50]  # Limit to 50 results
            }
            
            selected_product_key = st.selectbox(
                f"Select Product ({len(products)} found)", 
                ["-- Select --"] + list(product_options.keys())
            )
            
            if selected_product_key != "-- Select --":
                selected_product = product_options[selected_product_key]
                show_count_form(tx_id, selected_product, warehouse_id)
        else:
            st.info("No products found")
    
    # Add new item option
    with st.expander("➕ Add Item Not in System"):
        add_new_item_form(tx_id)

def batch_count_tab(tx_id: int, warehouse_id: int):
    """Batch counting for multiple items"""
    st.subheader("📋 Batch Count")
    
    # Show pending counts
    if st.session_state.temp_counts:
        st.warning(f"⚠️ You have {len(st.session_state.temp_counts)} unsaved counts")
        
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("💾 Save All Counts", type="primary", use_container_width=True):
                save_batch_counts(tx_id)
        with col2:
            if st.button("🗑️ Clear All", use_container_width=True):
                if st.checkbox("Confirm clear"):
                    st.session_state.temp_counts = []
                    st.rerun()
        
        # Display temp counts
        st.markdown("#### Pending Counts")
        for i, count in enumerate(st.session_state.temp_counts):
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                st.write(f"{count['product_name']}")
                st.caption(f"Batch: {count.get('batch_no', 'N/A')}")
            
            with col2:
                st.write(f"Qty: {count['actual_quantity']}")
            
            with col3:
                st.write(f"📍 {count.get('location', 'N/A')}")
            
            with col4:
                if st.button("❌", key=f"remove_{i}"):
                    st.session_state.temp_counts.pop(i)
                    st.rerun()
    
    # Quick add form
    st.markdown("#### Quick Add Count")
    quick_count_form(tx_id, warehouse_id)

def progress_tab(tx_id: int):
    """Show counting progress"""
    st.subheader("📊 Counting Progress")
    
    # Get progress stats
    progress = audit_service.get_transaction_progress(tx_id)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Items", progress.get('items_counted', 0))
    with col2:
        st.metric("Total Value", f"${progress.get('total_value', 0):,.2f}")
    with col3:
        pending = len(st.session_state.temp_counts)
        st.metric("Pending Counts", pending)
    
    # Recent counts
    st.markdown("#### Recent Counts")
    recent_counts = audit_service.get_recent_counts(tx_id, limit=20)
    
    if recent_counts:
        for count in recent_counts:
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                product_name = count.get('product_name', 'New Item')
                st.write(f"**{product_name}**")
                st.caption(f"PT: {count.get('pt_code', 'N/A')} | Batch: {count.get('batch_no', 'N/A')}")
            
            with col2:
                st.write(f"Qty: {count['actual_quantity']:.0f}")
                variance = count['actual_quantity'] - count.get('system_quantity', 0)
                if variance != 0:
                    st.caption(f"Var: {variance:+.0f}")
            
            with col3:
                location = f"{count.get('zone_name', '')}"
                if count.get('rack_name'):
                    location += f"-{count['rack_name']}"
                st.write(f"📍 {location}")
            
            with col4:
                counted_time = pd.to_datetime(count['counted_date']).strftime('%H:%M')
                st.caption(f"⏰ {counted_time}")
            
            st.divider()
    else:
        st.info("No counts recorded yet")
    
    # Submit transaction button
    if progress.get('items_counted', 0) > 0:
        st.markdown("---")
        if st.button("✅ Submit Transaction", type="primary", use_container_width=True):
            try:
                # Save any pending counts first
                if st.session_state.temp_counts:
                    save_batch_counts(tx_id)
                
                # Submit transaction
                audit_service.submit_transaction(tx_id, st.session_state.user_id)
                st.success("✅ Transaction submitted successfully!")
                
                # Clear states and redirect
                st.session_state.temp_counts = []
                if 'selected_tx_id' in st.session_state:
                    del st.session_state.selected_tx_id
                
                st.balloons()
                st.switch_page("pages/audit_management.py")
            except Exception as e:
                st.error(f"Error: {str(e)}")

def show_count_form(tx_id: int, product: Dict, warehouse_id: int):
    """Show counting form for selected product"""
    st.markdown("---")
    st.markdown(f"#### 📦 Count: {product['product_name']}")
    
    # Get batch details
    batch_details = audit_service.get_product_batch_details(warehouse_id, product['product_id'])
    
    with st.form(f"count_form_{product['product_id']}"):
        col1, col2 = st.columns(2)
        
        with col1:
            # Batch selection
            if batch_details:
                batch_options = ["-- Manual Entry --"] + [
                    f"{b['batch_no']} (Qty: {b['quantity']:.0f})" 
                    for b in batch_details
                ]
                selected_batch = st.selectbox("Batch", batch_options)
                
                if selected_batch != "-- Manual Entry --":
                    batch_no = selected_batch.split(" (")[0]
                    # Find batch data
                    batch_data = next((b for b in batch_details if b['batch_no'] == batch_no), None)
                else:
                    batch_no = st.text_input("Batch Number")
                    batch_data = None
            else:
                batch_no = st.text_input("Batch Number")
                batch_data = None
            
            # Expiry date
            if batch_data and batch_data.get('expired_date'):
                expired_date = pd.to_datetime(batch_data['expired_date']).date()
            else:
                expired_date = st.date_input("Expiry Date")
            
            # Quantity
            actual_quantity = st.number_input(
                "Actual Quantity", 
                min_value=0.0, 
                step=1.0,
                help="Enter the quantity you counted"
            )
        
        with col2:
            # Location
            location = st.text_input(
                "Location", 
                value=batch_data.get('location', '') if batch_data else '',
                placeholder="e.g., A1-R01-B01"
            )
            
            # Parse location
            if location and '-' in location:
                parts = location.split('-')
                zone = parts[0] if len(parts) > 0 else ""
                rack = parts[1] if len(parts) > 1 else ""
                bin_loc = parts[2] if len(parts) > 2 else ""
            else:
                zone = location
                rack = ""
                bin_loc = ""
            
            # Notes
            notes = st.text_area("Notes", placeholder="Any observations")
        
        # Submit button
        col1, col2 = st.columns(2)
        
        with col1:
            add_more = st.form_submit_button("➕ Add & Continue", use_container_width=True)
        
        with col2:
            save_now = st.form_submit_button("💾 Save Count", type="primary", use_container_width=True)
        
        if add_more or save_now:
            if actual_quantity > 0:
                # Prepare count data
                count_data = {
                    'transaction_id': tx_id,
                    'product_id': product['product_id'],
                    'product_name': product['product_name'],
                    'pt_code': product.get('pt_code', 'N/A'),
                    'batch_no': batch_no,
                    'expired_date': expired_date,
                    'zone_name': zone,
                    'rack_name': rack,
                    'bin_name': bin_loc,
                    'location': location,
                    'system_quantity': batch_data['quantity'] if batch_data else 0,
                    'system_value_usd': batch_data.get('value_usd', 0) if batch_data else 0,
                    'actual_quantity': actual_quantity,
                    'actual_notes': notes,
                    'created_by_user_id': st.session_state.user_id
                }
                
                if add_more:
                    # Add to temp counts
                    st.session_state.temp_counts.append(count_data)
                    st.success(f"✅ Added to batch ({len(st.session_state.temp_counts)} pending)")
                    st.rerun()
                else:
                    # Save immediately
                    try:
                        audit_service.save_count_detail(count_data)
                        st.success("✅ Count saved successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            else:
                st.warning("Please enter quantity greater than 0")

def quick_count_form(tx_id: int, warehouse_id: int):
    """Quick count form for batch counting"""
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        # Quick product search
        search = st.text_input("Quick Search", placeholder="PT code or name")
        
        if search:
            products = search_products(warehouse_id, search, "")
            if products:
                product = products[0]  # Take first match
                st.caption(f"Found: {product['product_name']}")
            else:
                product = None
                st.caption("No product found")
        else:
            product = None
    
    with col2:
        batch_no = st.text_input("Batch", placeholder="Batch no")
    
    with col3:
        quantity = st.number_input("Qty", min_value=0.0, step=1.0)
    
    with col4:
        location = st.text_input("Loc", placeholder="A1-R01")
    
    if st.button("➕ Add to Batch", use_container_width=True, disabled=not product or quantity <= 0):
        if product and quantity > 0:
            # Parse location
            if location and '-' in location:
                parts = location.split('-')
                zone = parts[0] if len(parts) > 0 else ""
                rack = parts[1] if len(parts) > 1 else ""
                bin_loc = parts[2] if len(parts) > 2 else ""
            else:
                zone = location
                rack = ""
                bin_loc = ""
            
            count_data = {
                'transaction_id': tx_id,
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'pt_code': product.get('pt_code', 'N/A'),
                'batch_no': batch_no,
                'expired_date': None,
                'zone_name': zone,
                'rack_name': rack,
                'bin_name': bin_loc,
                'location': location,
                'system_quantity': 0,
                'system_value_usd': 0,
                'actual_quantity': quantity,
                'actual_notes': '',
                'created_by_user_id': st.session_state.user_id
            }
            
            st.session_state.temp_counts.append(count_data)
            st.success(f"✅ Added ({len(st.session_state.temp_counts)} pending)")
            st.rerun()

def add_new_item_form(tx_id: int):
    """Form for adding items not in system"""
    with st.form("new_item_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            product_name = st.text_input("Product Name*")
            brand = st.text_input("Brand")
            batch_no = st.text_input("Batch Number")
        
        with col2:
            quantity = st.number_input("Quantity*", min_value=0.0, step=1.0)
            expired_date = st.date_input("Expiry Date")
            location = st.text_input("Location")
        
        notes = st.text_area("Notes / Description")
        
        if st.form_submit_button("Add New Item", type="primary"):
            if product_name and quantity > 0:
                try:
                    count_data = {
                        'transaction_id': tx_id,
                        'product_id': None,
                        'batch_no': batch_no,
                        'expired_date': expired_date,
                        'zone_name': location,
                        'rack_name': '',
                        'bin_name': '',
                        'location_notes': location,
                        'system_quantity': 0,
                        'system_value_usd': 0,
                        'actual_quantity': quantity,
                        'actual_notes': f"NEW ITEM: {product_name} - {brand}. {notes}",
                        'is_new_item': True,
                        'created_by_user_id': st.session_state.user_id
                    }
                    
                    audit_service.save_count_detail(count_data)
                    st.success("✅ New item added successfully!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            else:
                st.warning("Please enter product name and quantity")

def save_batch_counts(tx_id: int):
    """Save all temporary counts"""
    if not st.session_state.temp_counts:
        st.warning("No counts to save")
        return
    
    try:
        with st.spinner("Saving counts..."):
            saved, errors = audit_service.save_batch_counts(st.session_state.temp_counts)
        
        if errors:
            st.error(f"Saved {saved} counts with {len(errors)} errors")
            for error in errors[:3]:
                st.caption(f"• {error}")
        else:
            st.success(f"✅ Successfully saved {saved} counts!")
        
        # Clear temp counts
        st.session_state.temp_counts = []
        st.rerun()
        
    except Exception as e:
        st.error(f"Error saving counts: {str(e)}")

def get_warehouse_brands(warehouse_id: int) -> List[str]:
    """Get list of brands in warehouse"""
    try:
        brands = audit_service.get_warehouse_brands(warehouse_id)
        return [b['brand'] for b in brands if b['brand']]
    except:
        return []

def search_products(warehouse_id: int, search_term: str, brand_filter: str) -> List[Dict]:
    """Search products in warehouse"""
    try:
        return audit_service.search_products_with_filters(
            warehouse_id, 
            search_term, 
            brand_filter
        )
    except:
        return []

if __name__ == "__main__":
    main()
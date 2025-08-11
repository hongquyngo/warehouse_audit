# audit_service.py - Business Logic for Warehouse Audit System
import pandas as pd
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Any
import logging
from sqlalchemy import text
from utils.db import get_db_engine
from audit_queries import AuditQueries

logger = logging.getLogger(__name__)

class AuditService:
    """Service class for audit business logic"""
    
    def __init__(self):
        self.queries = AuditQueries()
    
    def _execute_query(self, query: str, params: Dict = None, fetch: str = 'all') -> Any:
        """Execute database query with error handling"""
        try:
            engine = get_db_engine()
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                
                if fetch == 'all':
                    rows = [dict(row._mapping) for row in result.fetchall()]
                    return self._convert_decimals(rows)
                elif fetch == 'one':
                    row = result.fetchone()
                    if row:
                        row_dict = dict(row._mapping)
                        return self._convert_decimals(row_dict)
                    return None
                elif fetch == 'none':
                    conn.commit()
                    return True
                else:
                    return result
                    
        except Exception as e:
            logger.error(f"Database query error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise e
    
    def _convert_decimals(self, data):
        """Convert decimal.Decimal objects to float/int for Streamlit compatibility"""
        if isinstance(data, dict):
            return {key: self._convert_decimals(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._convert_decimals(item) for item in data]
        elif isinstance(data, Decimal):
            # Convert Decimal to int if it's a whole number, otherwise to float
            if data % 1 == 0:
                return int(data)
            else:
                return float(data)
        else:
            return data
    
    def _generate_code(self, prefix: str, table: str, code_field: str) -> str:
        """Generate unique code for sessions/transactions"""
        today = datetime.now().strftime('%Y%m%d')
        
        # Get last sequence number for today
        query = f"""
        SELECT {code_field} FROM {table} 
        WHERE {code_field} LIKE :pattern 
        ORDER BY {code_field} DESC LIMIT 1
        """
        
        pattern = f"{prefix}_{today}_%"
        result = self._execute_query(query, {'pattern': pattern}, fetch='one')
        
        if result:
            last_code = result[code_field]
            last_seq = int(last_code.split('_')[-1])
            new_seq = last_seq + 1
        else:
            new_seq = 1
        
        return f"{prefix}_{today}_{new_seq:03d}"
    
    # ============== SESSION MANAGEMENT ==============
    
    def create_session(self, session_data: Dict) -> str:
        """Create new audit session"""
        try:
            # Generate session code
            session_code = self._generate_code('AUDIT', 'audit_sessions', 'session_code')
            
            # Insert session
            query = self.queries.INSERT_SESSION
            params = {
                'session_code': session_code,
                'session_name': session_data['session_name'],
                'warehouse_id': session_data['warehouse_id'],
                'planned_start_date': session_data['planned_start_date'],
                'planned_end_date': session_data['planned_end_date'],
                'notes': session_data.get('notes', ''),
                'created_by_user_id': session_data['created_by_user_id']
            }
            
            self._execute_query(query, params, fetch='none')
            
            logger.info(f"Session created: {session_code}")
            return session_code
            
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            raise e
    
    def start_session(self, session_id: int, user_id: int) -> bool:
        """Start audit session"""
        try:
            query = self.queries.START_SESSION
            params = {
                'session_id': session_id,
                'user_id': user_id
            }
            
            self._execute_query(query, params, fetch='none')
            
            logger.info(f"Session {session_id} started by user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting session: {e}")
            raise e
    
    def complete_session(self, session_id: int, user_id: int) -> bool:
        """Complete audit session"""
        try:
            query = self.queries.COMPLETE_SESSION
            params = {
                'session_id': session_id,
                'user_id': user_id
            }
            
            self._execute_query(query, params, fetch='none')
            
            logger.info(f"Session {session_id} completed by user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error completing session: {e}")
            raise e
    
    def get_sessions_by_status(self, status: str, limit: int = 50) -> List[Dict]:
        """Get sessions by status"""
        try:
            query = self.queries.GET_SESSIONS_BY_STATUS
            params = {'status': status, 'limit': limit}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting sessions by status {status}: {e}")
            return []
    
    def get_all_sessions(self, limit: int = 50) -> List[Dict]:
        """Get all sessions"""
        try:
            query = self.queries.GET_ALL_SESSIONS
            params = {'limit': limit}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting all sessions: {e}")
            return []
    
    def get_session_info(self, session_id: int) -> Dict:
        """Get session information"""
        try:
            query = self.queries.GET_SESSION_INFO
            params = {'session_id': session_id}
            
            return self._execute_query(query, params, fetch='one')
            
        except Exception as e:
            logger.error(f"Error getting session info: {e}")
            return {}
    
    def get_session_progress(self, session_id: int) -> Dict:
        """Get session progress statistics"""
        try:
            query = self.queries.GET_SESSION_PROGRESS
            params = {'session_id': session_id}
            
            result = self._execute_query(query, params, fetch='one')
            
            return result or {
                'total_transactions': 0,
                'completed_transactions': 0,
                'completion_rate': 0,
                'total_items': 0,
                'total_value': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting session progress: {e}")
            return {
                'total_transactions': 0,
                'completed_transactions': 0,
                'completion_rate': 0,
                'total_items': 0,
                'total_value': 0
            }
    
    # ============== TRANSACTION MANAGEMENT ==============
    
    def create_transaction(self, transaction_data: Dict) -> str:
        """Create new audit transaction"""
        try:
            # Generate transaction code
            transaction_code = self._generate_code('TXN', 'audit_transactions', 'transaction_code')
            
            # Insert transaction
            query = self.queries.INSERT_TRANSACTION
            params = {
                'transaction_code': transaction_code,
                'session_id': transaction_data['session_id'],
                'transaction_name': transaction_data['transaction_name'],
                'assigned_zones': transaction_data.get('assigned_zones', ''),
                'assigned_categories': transaction_data.get('assigned_categories', ''),
                'notes': transaction_data.get('notes', ''),
                'created_by_user_id': transaction_data['created_by_user_id']
            }
            
            self._execute_query(query, params, fetch='none')
            
            logger.info(f"Transaction created: {transaction_code}")
            return transaction_code
            
        except Exception as e:
            logger.error(f"Error creating transaction: {e}")
            raise e
    
    def submit_transaction(self, transaction_id: int, user_id: int) -> bool:
        """Submit transaction for completion"""
        try:
            query = self.queries.SUBMIT_TRANSACTION
            params = {
                'transaction_id': transaction_id,
                'user_id': user_id,
                'submit_time': datetime.now()
            }
            
            self._execute_query(query, params, fetch='none')
            
            # Update transaction counts
            self._update_transaction_counts(transaction_id)
            
            logger.info(f"Transaction {transaction_id} submitted by user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error submitting transaction: {e}")
            raise e
    
    def get_user_transactions(self, session_id: int, user_id: int, status: str = None) -> List[Dict]:
        """Get user's transactions for a session"""
        try:
            if status:
                query = self.queries.GET_USER_TRANSACTIONS_BY_STATUS
                params = {'session_id': session_id, 'user_id': user_id, 'status': status}
            else:
                query = self.queries.GET_USER_TRANSACTIONS
                params = {'session_id': session_id, 'user_id': user_id}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting user transactions: {e}")
            return []
    
    def get_user_transactions_all(self, user_id: int) -> List[Dict]:
        """Get all transactions created by user across all sessions"""
        try:
            query = """
            SELECT 
                at.*,
                ass.session_name,
                ass.warehouse_id,
                wh.name as warehouse_name
            FROM audit_transactions at
            JOIN audit_sessions ass ON at.session_id = ass.id
            LEFT JOIN warehouses wh ON ass.warehouse_id = wh.id
            WHERE at.created_by_user_id = :user_id
            AND at.delete_flag = 0
            ORDER BY at.created_date DESC
            LIMIT 50
            """
            
            params = {'user_id': user_id}
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting user transactions: {e}")
            return []
    
    def get_transaction_info(self, transaction_id: int) -> Dict:
        """Get transaction information"""
        try:
            query = self.queries.GET_TRANSACTION_INFO
            params = {'transaction_id': transaction_id}
            
            return self._execute_query(query, params, fetch='one')
            
        except Exception as e:
            logger.error(f"Error getting transaction info: {e}")
            return {}
    
    def get_transaction_progress(self, transaction_id: int) -> Dict:
        """Get transaction progress"""
        try:
            query = self.queries.GET_TRANSACTION_PROGRESS
            params = {'transaction_id': transaction_id}
            
            result = self._execute_query(query, params, fetch='one')
            
            return result or {
                'items_counted': 0,
                'total_value': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting transaction progress: {e}")
            return {
                'items_counted': 0,
                'total_value': 0
            }
    
    def _update_transaction_counts(self, transaction_id: int):
        """Update transaction total counts"""
        try:
            query = self.queries.UPDATE_TRANSACTION_COUNTS
            params = {'transaction_id': transaction_id}
            
            self._execute_query(query, params, fetch='none')
            
        except Exception as e:
            logger.error(f"Error updating transaction counts: {e}")
    
    # ============== COUNT DETAILS MANAGEMENT ==============
    
    def save_count_detail(self, count_data: Dict) -> bool:
        """Save individual count detail"""
        try:
            # Check if this product already counted in this transaction
            existing_query = self.queries.CHECK_EXISTING_COUNT
            existing_params = {
                'transaction_id': count_data['transaction_id'],
                'product_id': count_data.get('product_id'),
                'batch_no': count_data.get('batch_no', ''),
                'is_new_item': count_data.get('is_new_item', False)
            }
            
            existing = self._execute_query(existing_query, existing_params, fetch='one')
            
            if existing:
                # Update existing count
                query = self.queries.UPDATE_COUNT_DETAIL
                params = {
                    'count_id': existing['id'],
                    'actual_quantity': count_data['actual_quantity'],
                    'actual_notes': count_data.get('actual_notes', ''),
                    'zone_name': count_data.get('zone_name', ''),
                    'rack_name': count_data.get('rack_name', ''),
                    'bin_name': count_data.get('bin_name', ''),
                    'location_notes': count_data.get('location_notes', ''),
                    'modified_by_user_id': count_data['created_by_user_id'],
                    'modified_date': datetime.now()
                }
            else:
                # Insert new count
                query = self.queries.INSERT_COUNT_DETAIL
                params = {
                    'transaction_id': count_data['transaction_id'],
                    'product_id': count_data.get('product_id'),
                    'batch_no': count_data.get('batch_no', ''),
                    'expired_date': count_data.get('expired_date'),
                    'zone_name': count_data.get('zone_name', ''),
                    'rack_name': count_data.get('rack_name', ''),
                    'bin_name': count_data.get('bin_name', ''),
                    'location_notes': count_data.get('location_notes', ''),
                    'system_quantity': count_data.get('system_quantity', 0),
                    'system_value_usd': count_data.get('system_value_usd', 0),
                    'actual_quantity': count_data['actual_quantity'],
                    'actual_notes': count_data.get('actual_notes', ''),
                    'is_new_item': count_data.get('is_new_item', False),
                    'created_by_user_id': count_data['created_by_user_id'],
                    'counted_date': datetime.now()
                }
            
            self._execute_query(query, params, fetch='none')
            
            logger.info(f"Count detail saved for transaction {count_data['transaction_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving count detail: {e}")
            raise e
    
    def get_recent_counts(self, transaction_id: int, limit: int = 10) -> List[Dict]:
        """Get recent counts for transaction"""
        try:
            query = self.queries.GET_RECENT_COUNTS
            params = {'transaction_id': transaction_id, 'limit': limit}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting recent counts: {e}")
            return []
    
    # ============== PRODUCT AND INVENTORY ==============
    
    def get_warehouses(self) -> List[Dict]:
        """Get all warehouses"""
        try:
            query = self.queries.GET_WAREHOUSES
            return self._execute_query(query)
            
        except Exception as e:
            logger.error(f"Error getting warehouses: {e}")
            return []
    
    def get_warehouse_detail(self, warehouse_id: int) -> Dict:
        """Get detailed warehouse information"""
        try:
            # Try detailed query first
            query = self.queries.GET_WAREHOUSE_DETAIL
            params = {'warehouse_id': warehouse_id}
            
            result = self._execute_query(query, params, fetch='one')
            
            if result:
                return result
            
            # Fallback to basic query if detailed query fails
            logger.warning(f"Detailed warehouse query failed, trying basic query for warehouse {warehouse_id}")
            
            basic_query = self.queries.GET_WAREHOUSE_BASIC
            basic_result = self._execute_query(basic_query, params, fetch='one')
            
            if basic_result:
                # Convert basic result to match expected format
                return {
                    'id': basic_result.get('id'),
                    'name': basic_result.get('name'),
                    'address': basic_result.get('address'),
                    'zipcode': basic_result.get('zipcode'),
                    'company_name': f"Company ID: {basic_result.get('company_id', 'N/A')}",
                    'company_local_name': 'N/A',
                    'country_name': f"Country ID: {basic_result.get('country_id', 'N/A')}",
                    'state_province': f"State ID: {basic_result.get('state_id', 'N/A')}",
                    'manager_name': f"Manager ID: {basic_result.get('manager_id', 'N/A')}",
                    'manager_email': 'N/A',
                    'created_date': basic_result.get('created_date'),
                    'modified_date': basic_result.get('modified_date')
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting warehouse detail: {e}")
            # Try basic query as last resort
            try:
                basic_query = self.queries.GET_WAREHOUSE_BASIC
                params = {'warehouse_id': warehouse_id}
                basic_result = self._execute_query(basic_query, params, fetch='one')
                
                if basic_result:
                    return {
                        'id': basic_result.get('id'),
                        'name': basic_result.get('name'),
                        'address': basic_result.get('address'),
                        'zipcode': basic_result.get('zipcode'),
                        'company_name': 'Basic info only',
                        'company_local_name': 'N/A',
                        'country_name': 'N/A',
                        'state_province': 'N/A',
                        'manager_name': 'N/A',
                        'manager_email': 'N/A'
                    }
                    
            except Exception as e2:
                logger.error(f"Basic warehouse query also failed: {e2}")
            
            return {}
    
    def get_warehouse_detail(self, warehouse_id: int) -> Dict:
        """Get detailed warehouse information"""
        try:
            query = self.queries.GET_WAREHOUSE_DETAIL
            params = {'warehouse_id': warehouse_id}
            
            return self._execute_query(query, params, fetch='one')
            
        except Exception as e:
            logger.error(f"Error getting warehouse detail: {e}")
            return {}
    
    def search_products(self, search_term: str, warehouse_id: int) -> List[Dict]:
        """Search products in warehouse (legacy method for backward compatibility)"""
        try:
            query = self.queries.SEARCH_PRODUCTS
            params = {
                'warehouse_id': warehouse_id,
                'search_term': f"%{search_term}%"
            }
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return []
    
    def get_warehouse_products(self, warehouse_id: int) -> List[Dict]:
        """Get all products available in warehouse"""
        try:
            query = self.queries.GET_WAREHOUSE_PRODUCTS
            params = {'warehouse_id': warehouse_id}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting warehouse products: {e}")
            return []
    
    def get_warehouse_brands(self, warehouse_id: int) -> List[Dict]:
        """Get all brands available in warehouse"""
        try:
            query = self.queries.GET_WAREHOUSE_BRANDS
            params = {'warehouse_id': warehouse_id}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting warehouse brands: {e}")
            return []
    
    def search_products_with_filters(self, warehouse_id: int, search_term: str = "", brand_filter: str = "") -> List[Dict]:
        """Search products with brand filter and search term"""
        try:
            query = self.queries.SEARCH_PRODUCTS_WITH_FILTERS
            params = {
                'warehouse_id': warehouse_id,
                'search_term': f"%{search_term}%" if search_term else "",
                'brand_filter': brand_filter
            }
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error searching products with filters: {e}")
            return []
    
    def get_product_system_inventory(self, transaction_id: int, product_id: int) -> Dict:
        """Get system inventory for product in transaction context"""
        try:
            # First get session and warehouse info
            tx_info = self.get_transaction_info(transaction_id)
            session_info = self.get_session_info(tx_info['session_id'])
            warehouse_id = session_info['warehouse_id']
            
            query = self.queries.GET_PRODUCT_SYSTEM_INVENTORY
            params = {
                'warehouse_id': warehouse_id,
                'product_id': product_id
            }
            
            return self._execute_query(query, params, fetch='one')
            
        except Exception as e:
            logger.error(f"Error getting product system inventory: {e}")
            return {}
    
    # ============== DASHBOARD AND STATS ==============
    
    def get_dashboard_stats(self) -> Dict:
        """Get dashboard statistics"""
        try:
            query = self.queries.GET_DASHBOARD_STATS
            result = self._execute_query(query, fetch='one')
            
            return result or {
                'active_sessions': 0,
                'draft_sessions': 0,
                'completed_today': 0,
                'active_users': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting dashboard stats: {e}")
            return {
                'active_sessions': 0,
                'draft_sessions': 0,
                'completed_today': 0,
                'active_users': 0
            }
    
    def get_daily_stats(self, days: int = 7) -> List[Dict]:
        """Get daily statistics for charts"""
        try:
            query = self.queries.GET_DAILY_STATS
            params = {'days': days}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting daily stats: {e}")
            return []
    
    def get_user_activity_stats(self) -> List[Dict]:
        """Get user activity statistics"""
        try:
            query = self.queries.GET_USER_ACTIVITY_STATS
            
            return self._execute_query(query)
            
        except Exception as e:
            logger.error(f"Error getting user activity stats: {e}")
            return []
    
    # ============== REPORTING ==============
    
    def get_session_report_data(self, session_id: int) -> List[Dict]:
        """Get comprehensive session data for reporting"""
        try:
            query = self.queries.GET_SESSION_REPORT_DATA
            params = {'session_id': session_id}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting session report data: {e}")
            return []
    
    def export_session_to_excel(self, session_id: int, file_path: str = None) -> str:
        """Export session data to Excel file"""
        try:
            # Get session data
            session_info = self.get_session_info(session_id)
            report_data = self.get_session_report_data(session_id)
            
            if not report_data:
                raise Exception("No data available for export")
            
            # Create DataFrame
            df = pd.DataFrame(report_data)
            
            # Generate file path if not provided
            if not file_path:
                session_code = session_info.get('session_code', 'unknown')
                file_path = f"audit_export_{session_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            # Export to Excel
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name='Audit_Data', index=False)
                
                # Summary sheet
                summary_data = {
                    'Session Info': [
                        session_info.get('session_name', ''),
                        session_info.get('session_code', ''),
                        session_info.get('warehouse_name', ''),
                        session_info.get('actual_start_date', ''),
                        session_info.get('actual_end_date', '')
                    ],
                    'Values': [
                        'Session Name',
                        'Session Code', 
                        'Warehouse',
                        'Start Date',
                        'End Date'
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            logger.info(f"Session {session_id} exported to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error exporting session to Excel: {e}")
            raise e
    
    # ============== UTILITIES ==============
    
    def validate_session_data(self, session_data: Dict) -> List[str]:
        """Validate session data"""
        errors = []
        
        if not session_data.get('session_name'):
            errors.append("Session name is required")
        
        if not session_data.get('warehouse_id'):
            errors.append("Warehouse is required")
        
        if not session_data.get('planned_start_date'):
            errors.append("Planned start date is required")
        
        if not session_data.get('planned_end_date'):
            errors.append("Planned end date is required")
        
        # Date validation
        start_date = session_data.get('planned_start_date')
        end_date = session_data.get('planned_end_date')
        
        if start_date and end_date:
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            if end_date < start_date:
                errors.append("End date cannot be before start date")
        
        return errors
    
    def validate_transaction_data(self, transaction_data: Dict) -> List[str]:
        """Validate transaction data"""
        errors = []
        
        if not transaction_data.get('session_id'):
            errors.append("Session ID is required")
        
        if not transaction_data.get('transaction_name'):
            errors.append("Transaction name is required")
        
        if not transaction_data.get('created_by_user_id'):
            errors.append("User ID is required")
        
        return errors
    
    def get_audit_summary(self, session_id: int) -> Dict:
        """Get comprehensive audit summary"""
        try:
            session_info = self.get_session_info(session_id)
            session_progress = self.get_session_progress(session_id)
            
            # Get variance analysis
            variance_query = self.queries.GET_VARIANCE_ANALYSIS
            variance_params = {'session_id': session_id}
            variance_data = self._execute_query(variance_query, variance_params)
            
            # Calculate summary metrics
            total_variance = sum(item.get('variance_value', 0) for item in variance_data)
            items_with_variance = len([item for item in variance_data if item.get('variance_quantity', 0) != 0])
            
            return {
                'session_info': session_info,
                'progress': session_progress,
                'variance_summary': {
                    'total_variance_usd': total_variance,
                    'items_with_variance': items_with_variance,
                    'variance_items': variance_data[:10]  # Top 10 variance items
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting audit summary: {e}")
            return {}
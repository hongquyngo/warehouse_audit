# audit_service.py - Optimized Business Logic for Warehouse Audit System
import pandas as pd
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
import logging
from sqlalchemy import text
from contextlib import contextmanager
from utils.db import get_db_engine
from audit_queries import AuditQueries
import time

logger = logging.getLogger(__name__)

# Custom exceptions
class AuditException(Exception):
    """Base exception for audit system"""
    pass

class SessionNotFoundException(AuditException):
    """Raised when session not found"""
    pass

class InvalidTransactionStateException(AuditException):
    """Raised when transaction is in invalid state for operation"""
    pass

class CountValidationException(AuditException):
    """Raised when count data validation fails"""
    pass

class AuditService:
    """Optimized service class for audit business logic"""
    
    def __init__(self):
        self.queries = AuditQueries()
        self._connection_pool = None
    
    @contextmanager
    def _get_db_transaction(self):
        """Context manager for database transactions with connection pooling"""
        engine = get_db_engine()
        conn = engine.connect()
        trans = conn.begin()
        try:
            yield conn
            trans.commit()
        except Exception:
            trans.rollback()
            raise
        finally:
            conn.close()
    
    def _execute_query(self, query: str, params: Dict = None, fetch: str = 'all', use_transaction: bool = False) -> Any:
        """Execute database query with error handling and optional transaction"""
        try:
            if use_transaction:
                raise NotImplementedError("Use _get_db_transaction context manager instead")
            
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
            logger.error(f"Query: {query[:200]}...")  # Log first 200 chars
            logger.error(f"Params: {params}")
            raise AuditException(f"Database error: {str(e)}")
    
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
            # Validate data
            errors = self.validate_session_data(session_data)
            if errors:
                raise CountValidationException(f"Validation errors: {', '.join(errors)}")
            
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
            # Check session exists and is in draft status
            session_info = self.get_session_info(session_id)
            if not session_info:
                raise SessionNotFoundException(f"Session {session_id} not found")
            
            if session_info['status'] != 'draft':
                raise InvalidTransactionStateException(f"Session must be in draft status to start")
            
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
            # Check session exists and is in progress
            session_info = self.get_session_info(session_id)
            if not session_info:
                raise SessionNotFoundException(f"Session {session_id} not found")
            
            if session_info['status'] != 'in_progress':
                raise InvalidTransactionStateException(f"Session must be in progress to complete")
            
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
            # Validate data
            errors = self.validate_transaction_data(transaction_data)
            if errors:
                raise CountValidationException(f"Validation errors: {', '.join(errors)}")
            
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
            # Check transaction exists and has counts
            tx_info = self.get_transaction_info(transaction_id)
            if not tx_info:
                raise InvalidTransactionStateException(f"Transaction {transaction_id} not found")
            
            if tx_info['status'] != 'draft':
                raise InvalidTransactionStateException(f"Transaction must be in draft status to submit")
            
            # Check if has counts
            progress = self.get_transaction_progress(transaction_id)
            if progress.get('items_counted', 0) == 0:
                raise CountValidationException("Transaction must have at least one count before submission")
            
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
    
    # ============== OPTIMIZED COUNT DETAILS MANAGEMENT ==============
    
    def save_count_detail(self, count_data: Dict) -> bool:
        """Save individual count detail - ALWAYS CREATE NEW RECORD"""
        try:
            # Validate count data
            if count_data.get('actual_quantity', 0) < 0:
                raise CountValidationException("Actual quantity cannot be negative")
            
            # ALWAYS INSERT NEW - No checking for existing
            # This allows multiple counts for same batch at different locations/times
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


    def save_batch_counts(self, count_list: List[Dict]) -> Tuple[int, List[str]]:
        """Optimized batch save - ALWAYS INSERT NEW RECORDS"""
        saved_count = 0
        errors = []
        transaction_id = None
        
        try:
            # Start timing
            start_time = time.time()
            
            with self._get_db_transaction() as conn:
                for i, count_data in enumerate(count_list):
                    try:
                        # Validate each count
                        if count_data.get('actual_quantity', 0) <= 0:
                            errors.append(f"Row {i+1}: Actual quantity must be greater than 0")
                            continue
                        
                        # Store transaction_id for later update
                        if transaction_id is None:
                            transaction_id = count_data['transaction_id']
                        
                        # Parse location if needed
                        if 'location' in count_data and not count_data.get('zone_name'):
                            location = count_data['location']
                            if '-' in location:
                                parts = location.split('-')
                                count_data['zone_name'] = parts[0].strip() if len(parts) > 0 else ""
                                count_data['rack_name'] = parts[1].strip() if len(parts) > 1 else ""
                                count_data['bin_name'] = parts[2].strip() if len(parts) > 2 else ""
                            else:
                                count_data['zone_name'] = location.strip()
                                count_data['rack_name'] = ""
                                count_data['bin_name'] = ""
                        
                        # ALWAYS INSERT NEW - NO CHECK FOR EXISTING
                        # This allows multiple counts per batch
                        insert_query = self.queries.INSERT_COUNT_DETAIL
                        insert_params = {
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
                        conn.execute(text(insert_query), insert_params)
                        
                        saved_count += 1
                        
                    except Exception as e:
                        errors.append(f"Row {i+1}: {str(e)}")
                        logger.error(f"Error saving count {i+1}: {e}")
                        continue
                
                # Update transaction counts if any saved
                if saved_count > 0 and transaction_id:
                    update_query = self.queries.UPDATE_TRANSACTION_COUNTS
                    conn.execute(text(update_query), {'transaction_id': transaction_id})
            
            # Log performance
            elapsed = time.time() - start_time
            logger.info(f"Batch save completed: {saved_count} saved, {len(errors)} errors in {elapsed:.2f}s")
            
            return saved_count, errors
            
        except Exception as e:
            logger.error(f"Error in batch save: {e}")
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
    
    def delete_count_detail(self, count_id: int, user_id: int) -> bool:
        """Delete (soft delete) a count detail"""
        try:
            # First check if user owns this count and transaction is still draft
            check_query = self.queries.CHECK_COUNT_OWNERSHIP
            check_params = {'count_id': count_id, 'user_id': user_id}
            
            result = self._execute_query(check_query, check_params, fetch='one')
            
            if not result:
                logger.warning(f"User {user_id} cannot delete count {count_id} - not owner or not draft")
                return False
            
            # Perform soft delete
            delete_query = self.queries.SOFT_DELETE_COUNT_DETAIL
            delete_params = {'count_id': count_id, 'user_id': user_id}
            
            self._execute_query(delete_query, delete_params, fetch='none')
            
            logger.info(f"Count detail {count_id} deleted by user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting count detail: {e}")
            return False
    
    # ============== ENHANCED COUNT TRACKING ==============
    
    def get_product_counts(self, transaction_id: int, product_id: int) -> List[Dict]:
        """Get all counts for a product in transaction"""
        try:
            query = self.queries.GET_PRODUCT_COUNTS
            params = {
                'transaction_id': transaction_id,
                'product_id': product_id
            }
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting product counts: {e}")
            return []
    
    def get_product_count_summary(self, transaction_id: int, product_id: int) -> Dict:
        """Get count summary for a specific product"""
        try:
            query = self.queries.GET_PRODUCT_COUNT_SUMMARY
            params = {
                'transaction_id': transaction_id,
                'product_id': product_id
            }
            
            result = self._execute_query(query, params, fetch='one')
            return result or {
                'product_id': product_id,
                'total_counted': 0,
                'count_times': 0,
                'batches_counted': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting product count summary: {e}")
            return {
                'product_id': product_id,
                'total_counted': 0,
                'count_times': 0,
                'batches_counted': 0
            }
    
    def get_transaction_count_summary(self, transaction_id: int) -> List[Dict]:
        """Get count summary for all products in transaction - optimized"""
        try:
            query = self.queries.GET_TRANSACTION_COUNT_SUMMARY
            params = {'transaction_id': transaction_id}
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting transaction count summary: {e}")
            return []
    
    def get_batch_count_status(self, transaction_id: int, product_id: int) -> List[Dict]:
        """Get count status for each batch"""
        try:
            query = self.queries.GET_BATCH_COUNT_STATUS
            params = {
                'transaction_id': transaction_id,
                'product_id': product_id
            }
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting batch count status: {e}")
            return []
    
    def get_batch_count_history(self, transaction_id: int, product_id: int, batch_no: str) -> List[Dict]:
        """Get count history for a specific batch"""
        try:
            query = self.queries.GET_BATCH_COUNT_HISTORY
            params = {
                'transaction_id': transaction_id,
                'product_id': product_id,
                'batch_no': batch_no
            }
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting batch count history: {e}")
            return []
        
    def get_product_counts_all_transactions(self, session_id: int, product_id: int) -> List[Dict]:
        """Get all counts for a product across all transactions in the session"""
        try:
            query = self.queries.GET_PRODUCT_COUNTS_ALL_TRANSACTIONS
            params = {
                'session_id': session_id,
                'product_id': product_id
            }
            return self._execute_query(query, params)
        except Exception as e:
            logger.error(f"Error getting all transaction counts: {e}")
            return []

    def get_product_total_summary(self, session_id: int, product_id: int) -> Dict:
        """Get total summary for a product across all transactions"""
        try:
            query = self.queries.GET_PRODUCT_TOTAL_SUMMARY
            params = {
                'session_id': session_id,
                'product_id': product_id
            }
            result = self._execute_query(query, params, fetch='one')
            return result or {
                'total_transactions': 0,
                'total_users': 0,
                'total_batches': 0,
                'total_count_records': 0,
                'grand_total_counted': 0
            }
        except Exception as e:
            logger.error(f"Error getting product total summary: {e}")
            return {
                'total_transactions': 0,
                'total_users': 0,
                'total_batches': 0,
                'total_count_records': 0,
                'grand_total_counted': 0
            }



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
            return {}
    
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
    
    def get_product_batch_details(self, warehouse_id: int, product_id: int) -> List[Dict]:
        """Get all batch details for a product in warehouse"""
        try:
            query = self.queries.GET_PRODUCT_BATCH_DETAILS
            params = {
                'warehouse_id': warehouse_id,
                'product_id': product_id
            }
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting product batch details: {e}")
            return []
    
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
    
    def get_variance_analysis(self, session_id: int) -> List[Dict]:
        """Get variance analysis for session"""
        try:
            query = self.queries.GET_VARIANCE_ANALYSIS
            params = {'session_id': session_id}
            
            return self._execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting variance analysis: {e}")
            return []
    
    def export_session_to_excel(self, session_id: int, file_path: str = None) -> str:
        """Export session data to Excel file"""
        try:
            # Get session data
            session_info = self.get_session_info(session_id)
            report_data = self.get_session_report_data(session_id)
            
            if not report_data:
                raise AuditException("No data available for export")
            
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
                
                # Variance analysis sheet
                variance_data = self.get_variance_analysis(session_id)
                if variance_data:
                    variance_df = pd.DataFrame(variance_data)
                    variance_df.to_excel(writer, sheet_name='Variance_Analysis', index=False)
            
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
            variance_data = self.get_variance_analysis(session_id)
            
            # Calculate summary metrics
            total_variance_value = sum(item.get('variance_value', 0) for item in variance_data)
            items_with_variance = len([item for item in variance_data if item.get('variance_quantity', 0) != 0])
            
            return {
                'session_info': session_info,
                'progress': session_progress,
                'variance_summary': {
                    'total_variance_usd': total_variance_value,
                    'items_with_variance': items_with_variance,
                    'variance_items': variance_data[:10]  # Top 10 variance items
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting audit summary: {e}")
            return {}
        
        # Trong audit_service.py

    def save_media_attachment(self, attachment_data: Dict) -> int:
        """Save media attachment record to database"""
        try:
            query = """
            INSERT INTO audit_media_attachments (
                entity_type, entity_id, file_name, file_type,
                mime_type, file_size, s3_key, s3_bucket,
                description, uploaded_by_user_id
            ) VALUES (
                :entity_type, :entity_id, :file_name, :file_type,
                :mime_type, :file_size, :s3_key, :s3_bucket,
                :description, :uploaded_by_user_id
            )
            """
            
            engine = get_db_engine()
            with engine.connect() as conn:
                result = conn.execute(text(query), attachment_data)
                conn.commit()
                return result.lastrowid
                
        except Exception as e:
            logger.error(f"Error saving media attachment: {e}")
            raise e

    def get_entity_attachments(self, entity_type: str, entity_id: int) -> List[Dict]:
        """Get all attachments for an entity"""
        try:
            query = """
            SELECT 
                ama.*,
                u.username as uploaded_by_username,
                CONCAT(e.first_name, ' ', e.last_name) as uploaded_by_name
            FROM audit_media_attachments ama
            LEFT JOIN users u ON ama.uploaded_by_user_id = u.id
            LEFT JOIN employees e ON u.employee_id = e.id
            WHERE ama.entity_type = :entity_type
            AND ama.entity_id = :entity_id
            AND ama.delete_flag = 0
            ORDER BY ama.uploaded_date DESC
            """
            
            return self._execute_query(query, {
                'entity_type': entity_type,
                'entity_id': entity_id
            })
            
        except Exception as e:
            logger.error(f"Error getting attachments: {e}")
            return []

    def delete_attachment(self, attachment_id: int, user_id: int) -> bool:
        """Soft delete attachment"""
        try:
            query = """
            UPDATE audit_media_attachments
            SET 
                delete_flag = 1,
                modified_by_user_id = :user_id,
                modified_date = NOW()
            WHERE id = :attachment_id
            """
            
            self._execute_query(query, {
                'attachment_id': attachment_id,
                'user_id': user_id
            }, fetch='none')
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting attachment: {e}")
            return False
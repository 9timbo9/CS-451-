from lstore.lock_manager import LockType, LockManager
import time
import threading
from lstore.config import MAX_RETRIES, RETRY_DELAY


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manager=None):
        self.queries = []
        self._aborted = False
        self._provided_lock_manager = lock_manager  # Store if explicitly provided
        self.lock_manager = lock_manager  # May be None initially
        self.acquired_locks = set()  # (record_id, lock_type)
        self.modifications = []  
        self.transaction_id = id(self)
        self.tables_modified = set()  # Track which tables were modified
        self.max_retries = MAX_RETRIES  # Maximum retry attempts
        self.retry_delay = RETRY_DELAY  # Initial delay in seconds (10ms)

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        self.tables_modified.add(table)

    def run(self, auto_retry=True):
        """
        Run transaction with 2PL and automatic retry on failure.
        
        :param auto_retry: if True, automatically retry on abort; if False, return False on abort
        :return: True if transaction committed, False if failed (and auto_retry=False)
        """
        # Try to get lock manager from tables if not set
        if self.lock_manager is None and self.tables_modified:
            for table in self.tables_modified:
                if hasattr(table, 'lock_manager') and table.lock_manager is not None:
                    self.lock_manager = table.lock_manager
                    break
        
        if not auto_retry:
            # Legacy behavior: single attempt
            if self.lock_manager is None:
                return self._run_without_locking()
            return self._run_with_locking()
        
        # Retry loop with exponential backoff
        retry_count = 0
        current_delay = self.retry_delay
        
        while retry_count < self.max_retries:
            try:
                if self.lock_manager is None:
                    result = self._run_without_locking()
                else:
                    result = self._run_with_locking()
                
                # If commit succeeded, return True
                if result:
                    return True
                
                # Transaction aborted, prepare for retry
                retry_count += 1
                if retry_count >= self.max_retries:
                    print(f"Transaction {self.transaction_id} failed after {self.max_retries} retries")
                    return False
                
                # Exponential backoff with jitter
                time.sleep(current_delay + (time.time() % 0.001))
                current_delay = min(current_delay * 1.5, 1.0)  # Cap at 1 second
                
                # Reset transaction state for retry
                self._reset_for_retry()
                
            except Exception as e:
                print(f"Transaction error: {e}")
                retry_count += 1
                if retry_count >= self.max_retries:
                    return False
                time.sleep(current_delay)
                current_delay = min(current_delay * 1.5, 1.0)
                self._reset_for_retry()
        
        return False

    def _reset_for_retry(self):
        """Reset transaction state for a retry attempt"""
        self._aborted = False
        self.acquired_locks = set()
        self.modifications = []
        # Reset transaction ID to get new lock identifiers
        self.transaction_id = id(self)
        # Note: queries and tables_modified remain the same

    def _run_without_locking(self):
        """Run transaction without locking (original behavior)"""
        for query, table, args in self.queries:
            try:
                result = query(*args)
                if result == False:
                    return self.abort()
            except Exception as e:
                print(f"Transaction error: {e}")
                return self.abort()
        return self.commit()

    def _run_with_locking(self):
        """Run transaction with 2PL - growing phase"""
        try:
            # Get lock manager from first table if not provided
            if self.lock_manager is None and self.tables_modified:
                for table in self.tables_modified:
                    if hasattr(table, 'lock_manager') and table.lock_manager is not None:
                        self.lock_manager = table.lock_manager
                        break
            
            # If still no lock manager, create one
            if self.lock_manager is None:
                self.lock_manager = LockManager()
            
            # Set transaction context on all tables
            for table in self.tables_modified:
                table.transaction_id = self.transaction_id
                table.lock_manager = self.lock_manager
            
            # Phase 1: Acquire all locks (GROW PHASE)
            for query, table, args in self.queries:
                if not self._acquire_locks_for_query(query, table, args):
                    return self.abort()
            
            # Phase 2: Execute all operations (EXECUTION PHASE)
            for query, table, args in self.queries:
                # Store state before modification for rollback
                if query.__name__ in ['update', 'delete']:
                    self._record_pre_modification_state(query, table, args)
                
                result = query(*args)
                if result == False:
                    return self.abort()
            
            # Phase 3: Commit and release locks (SHRINK PHASE)
            return self.commit()
        
        except Exception as e:
            print(f"Transaction error with locking: {e}")
            return self.abort()

    def _acquire_locks_for_query(self, query, table, args):
        """Acquire necessary locks for a query"""
        records_to_lock = self._get_records_for_query(query, table, args)
        
        for record_id, lock_type in records_to_lock:
            if not self.lock_manager.acquire_lock(self.transaction_id, record_id, lock_type):
                return False
            self.acquired_locks.add((record_id, lock_type))
        
        return True

    def _get_records_for_query(self, query, table, args):
        """Determine which records and lock types are needed for a query"""
        records = []
        
        query_func = query
        if hasattr(query, '__name__'):
            query_name = query.__name__
        else:
            # Handle case where it's a bound method
            query_name = query.__func__.__name__
        
        if query_name == 'select':
            # SELECT needs shared locks
            search_key, search_key_index, projected_columns = args
            rids = table.index.locate(search_key_index, search_key)
            for rid in rids:
                records.append((rid, LockType.SHARED))
        
        elif query_name == 'update':
            # UPDATE needs exclusive locks
            primary_key, *columns = args
            rids = table.index.locate(table.key, primary_key)
            for rid in rids:
                records.append((rid, LockType.EXCLUSIVE))
        
        elif query_name == 'insert':
            # INSERT - use a table-level lock for simplicity
            table_lock_id = f"table_{table.name}"
            records.append((table_lock_id, LockType.EXCLUSIVE))
        
        elif query_name == 'delete':
            # DELETE needs exclusive locks
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            for rid in rids:
                records.append((rid, LockType.EXCLUSIVE))
        
        elif query_name == 'sum':
            # SUM needs shared locks on the range
            start_range, end_range, aggregate_column_index = args
            rids = table.index.locate_range(start_range, end_range, table.key)
            for rid in rids:
                records.append((rid, LockType.SHARED))
        
        return records

    def _record_pre_modification_state(self, query, table, args):
        """Record state before modification for potential rollback"""
        query_name = query.__name__
        
        if query_name == 'update':
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            for rid in rids:
                current_data = table.read_record(rid)
                if current_data:
                    self.modifications.append((table, rid, current_data.copy()))
        
        elif query_name == 'delete':
            primary_key = args[0]
            rids = table.index.locate(table.key, primary_key)
            for rid in rids:
                current_data = table.read_record(rid)
                if current_data:
                    self.modifications.append((table, rid, current_data.copy()))

    def abort(self):
        """Abort transaction and rollback changes"""
        self._aborted = True
        
        # Rollback modifications on all affected tables
        for table in self.tables_modified:
            try:
                table.rollback_modifications()
            except Exception as e:
                print(f"Error rolling back modifications for {table.name}: {e}")
        
        # Release all locks
        if self.lock_manager:
            self.lock_manager.release_locks(self.transaction_id)
        
        # Clear transaction context
        for table in self.tables_modified:
            table.transaction_id = None
            table.lock_manager = None
        
        return False

    def commit(self):
        """Commit transaction"""
        if self._aborted:
            return False
        
        # Release all locks (shrinking phase of 2PL)
        if self.lock_manager:
            self.lock_manager.release_locks(self.transaction_id)
        
        # Clear transaction context
        for table in self.tables_modified:
            table.transaction_id = None
            table.lock_manager = None
        
        # Clear recorded modifications since we're committing
        for table in self.tables_modified:
            with table._transaction_modifications_lock:
                table._transaction_modifications = [m for m in table._transaction_modifications if m['transaction_id'] != self.transaction_id]
        
        return True


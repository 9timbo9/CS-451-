from lock_manager import LockType
class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manager=None):
        self.queries = []
        self._aborted = False
        self.lock_manager = lock_manager
        self.acquired_locks = set()  # (record_id, lock_type)
        self.modifications = []  
        self.transaction_id = id(self)

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))

    def run(self):
        """Run transaction with 2PL"""
        # If no lock manager provided, run without locking (backward compatibility)
        if self.lock_manager is None:
            return self._run_without_locking()
        
        # Run with 2PL locking
        return self._run_with_locking()

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
            # Phase 1: Acquire all locks
            for query, table, args in self.queries:
                if not self._acquire_locks_for_query(query, table, args):
                    return self.abort()
            
            # Phase 2: Execute all operations
            for query, table, args in self.queries:
                # Store state before modification for rollback
                if query.__name__ in ['update', 'delete']:
                    self._record_pre_modification_state(query, table, args)
                
                result = query(*args)
                if result == False:
                    return self.abort()
            
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
        
        # Rollback modifications (simplified - in real implementation, restore old state)
        for table, record_id, old_data in reversed(self.modifications):
            # This would require a restore_record method in Table
            pass
        
        # Release all locks
        if self.lock_manager:
            self.lock_manager.release_locks(self.transaction_id)
        
        return False

    def commit(self):
        """Commit transaction"""
        if self._aborted:
            return False
        
        # Release all locks (shrinking phase of 2PL)
        if self.lock_manager:
            self.lock_manager.release_locks(self.transaction_id)
        
        return True


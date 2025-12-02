from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockType


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table):
        self.table = table
        self.acquired_locks = []  # Track locks acquired in this query
        self.transaction_id = None  # Transaction context for locking

    def _get_page_ranges_for_rids(self, rids):
        """Get unique page ranges for given RIDs"""
        page_ranges = set()
        with self.table.page_directory_lock:
            for rid in rids:
                loc = self.table.page_directory.get(rid)
                if loc is not None:
                    range_idx, is_tail, offset = loc
                    page_ranges.add(range_idx)
        return sorted(page_ranges)

    def _acquire_locks(self, rids, lock_type):
        """
        Acquire page range locks for given RIDs.
        If any lock cannot be acquired, release all locks and return False.
        """
        if self.table.lock_manager is None:
            return True
        
        # Use transaction_id from query (set by transaction) or table
        transaction_id = self.transaction_id or getattr(self.table, 'transaction_id', None)
        if transaction_id is None:
            transaction_id = id(self)  # Fallback to query object ID
        
        # Get unique page ranges
        page_ranges = self._get_page_ranges_for_rids(rids)
        acquired = []
        
        try:
            for range_idx in page_ranges:
                # Use page range index as lock identifier (as a string)
                lock_id = f"page_range_{self.table.name}_{range_idx}"
                if self.table.lock_manager.acquire_lock(
                    transaction_id, lock_id, lock_type
                ):
                    acquired.append((lock_id, lock_type))
                else:
                    # Lock acquisition failed - release and return
                    self._release_locks(acquired)
                    return False
            # Store acquired locks for later release in shrink phase
            self.acquired_locks.extend(acquired)
            return True
        except Exception as e:
            # Release any locks acquired before exception
            self._release_locks(acquired)
            return False

    def _release_locks(self, locks):
        """Release a list of locks"""
        if self.table.lock_manager is None:
            return
        
        for lock_id, lock_type in locks:
            try:
                self.table.lock_manager.release_locks(self.table.transaction_id)
            except Exception:
                pass

    def _begin_2pl(self):
        """Begin 2PL - reset acquired locks for this query"""
        self.acquired_locks = []

    def _end_2pl(self):
        """End 2PL - release all locks acquired during grow phase (shrink phase)"""
        self._release_locks(self.acquired_locks)
        self.acquired_locks = []

    def delete(self, primary_key, transaction=None):
        """
        Delete records matching the primary_key with 2PL (page range level)
        """
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')

        if not in_transaction:
            self._begin_2pl()
        
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            if not in_transaction:
                # GROW PHASE: Acquire exclusive page range locks
                if not self._acquire_locks(rids, LockType.EXCLUSIVE):
                    return False

            # EXECUTION PHASE: Perform operations
            for rid in rids:
                deleted = self.table.delete_record(rid)
                if deleted is False:
                    return False
            
            return True
        except Exception as e:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

    def insert(self, *columns, transaction=None):
        """
        Insert a record with specified columns using 2PL (page range level)
        """
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')
        
        if not in_transaction:
            self._begin_2pl()
        
        try:
            # GROW PHASE: Acquire page range lock for base records (only if standalone)
            if not in_transaction and self.table.lock_manager:
                page_range = self.table._get_or_create_page_range()
                lock_id = f"page_range_{self.table.name}_{page_range.range_idx}"
                if not self.table.lock_manager.acquire_lock(
                    self.table.transaction_id, lock_id, LockType.EXCLUSIVE
                ):
                    return False
                self.acquired_locks.append((lock_id, LockType.EXCLUSIVE))
            
            # EXECUTION PHASE: Perform operation
            rid = self.table.insert(*columns)
            
            if rid is None:
                return False
            
            return rid is not None
        except Exception as e:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        """
        Read matching records with specified search key using 2PL (page range level)
        """
        # Use transaction_id from parameter, not from table (which is shared across threads!)
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')
        
        if not in_transaction:
            self._begin_2pl()
        
        try:
            results = []
            rids = set()

            # Use index if available
            if 0 <= search_key_index < self.table.num_columns:
                col_index_struct = self.table.index.indices[search_key_index]
                if col_index_struct is not None:
                    rids = self.table.index.locate(search_key_index, search_key)

            # If no index or no results, do full scan
            if not rids:
                with self.table.page_directory_lock:
                    for rid, (range_idx, is_tail, offset) in list(self.table.page_directory.items()):
                        if is_tail:
                            continue
                        values, _schema = self.table.get_latest_version(rid)
                        if values is None:
                            continue
                        if search_key_index < len(values) and values[search_key_index] == search_key:
                            rids.add(rid)

            # GROW PHASE: Acquire shared page range locks
            if not self._acquire_locks(rids, LockType.SHARED):
                return False

            # EXECUTION PHASE: Retrieve records
            for rid in rids:
                values, _schema = self.table.get_latest_version(rid)
                
                if values is None:
                    continue

                projected = [
                    v if bit else None
                    for v, bit in zip(values, projected_columns_index)
                ]

                results.append(Record(rid, values[self.table.key], projected))
            
            return results
        except Exception as e:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

    def update(self, primary_key, *columns, transaction=None):
        """
        Update a record with specified key and columns using 2PL (page range level)
        """
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')
        
        if not in_transaction:
            self._begin_2pl()
        
        try:
            # make sure caller passed exactly num_columns positional values
            if len(columns) != self.table.num_columns:
                return False

            # cannot update primary key via update
            if columns[self.table.key] is not None and columns[self.table.key] != primary_key:
                if self.table.index.locate(self.table.key, columns[self.table.key]):
                    return False

            # locate RIDs for a given primary key via its index
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids or any(r is None for r in rids):
                return False
            
            # GROW PHASE: Acquire exclusive page range locks (only if standalone)
            if not in_transaction:
                if not self._acquire_locks(rids, LockType.EXCLUSIVE):
                    return False
            
            # EXECUTION PHASE: Perform updates
            for rid in rids:
                ok = self.table.update_record(rid, *columns)
                if ok is False:
                    return False

            return True
        except Exception:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        """
        Sum aggregation with 2PL (page range level)
        """
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')
        
        if not in_transaction:
            self._begin_2pl()
        
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
            
            # GROW PHASE: Acquire shared page range locks (only if standalone)
            if not in_transaction:
                if not self._acquire_locks(rids, LockType.SHARED):
                    return False
            
            # EXECUTION PHASE: Perform summation
            total = 0
            for rid in rids:
                values, _ = self.table.get_latest_version(rid)
                if values is None:
                    continue
                total += values[aggregate_column_index]
            
            return total
        except Exception:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        """
        Read specific version with 2PL (page range level)
        """
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')
        
        if not in_transaction:
            self._begin_2pl()
        
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            # GROW PHASE: Acquire shared page range locks (only if standalone)
            if not in_transaction:
                if not self._acquire_locks(rids, LockType.SHARED):
                    return False

            # EXECUTION PHASE: Retrieve records
            results = []
            for rid in rids:
                values, _schema = self.table.get_version(rid, relative_version)
                if values is None:
                    continue

                projected = [
                    v if bit else None
                    for v, bit in zip(values, projected_columns_index)
                ]

                results.append(Record(rid, values[self.table.key], projected))

            return results if results else []
        except Exception:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        """
        Sum version aggregation with 2PL (page range level)
        """
        in_transaction = transaction is not None and hasattr(transaction, 'transaction_id')
        
        if not in_transaction:
            self._begin_2pl()
        
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False
            
            # GROW PHASE: Acquire shared page range locks (only if standalone)
            if not in_transaction:
                if not self._acquire_locks(rids, LockType.SHARED):
                    return False
            
            # EXECUTION PHASE: Perform summation
            total = 0
            for rid in rids:
                values, _ = self.table.get_version(rid, relative_version)
                if values is None:
                    continue
                total += values[aggregate_column_index]
            
            return total
        except Exception:
            return False
        finally:
            if not in_transaction:
                self._end_2pl()

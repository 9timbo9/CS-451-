from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockType


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table):
        self.table = table
        self.transaction_id = None  # Set by transaction

    def delete(self, primary_key, transaction=None):
        """Delete records matching the primary_key"""
        in_transaction = transaction is not None
        
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            # EXECUTION PHASE: Perform operations
            for rid in rids:
                deleted = self.table.delete_record(rid)
                if deleted is False:
                    return False
            
            return True
        except Exception as e:
            print(f"Delete error: {e}")
            return False

    def insert(self, *columns, transaction=None):
        """Insert a record with specified columns"""
        try:
            rid = self.table.insert(*columns)
            return rid is not None
        except Exception as e:
            print(f"Insert error: {e}")
            return False

    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        """Read matching records with specified search key"""
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
            print(f"Select error: {e}")
            return False

    def update(self, primary_key, *columns, transaction=None):
        """Update a record with specified key and columns"""
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
            
            # EXECUTION PHASE: Perform updates
            for rid in rids:
                ok = self.table.update_record(rid, *columns)
                if ok is False:
                    return False

            return True
        except Exception:
            return False

    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        """Sum aggregation"""
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
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

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        """Read specific version"""
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

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

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        """Sum version aggregation"""
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:
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

from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    def delete(self, primary_key):
        """
        # internal Method
        # Delete records matching the primary_key
        # Returns True upon successful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids or any(r is None for r in rids):  # if there's a missing rid then its False
                return False

            for rid in rids:
                deleted = self.table.delete_record(rid)  # needs to return true
                if deleted is False:
                    return False
            return True
        except Exception:
            return False

    def insert(self, *columns):
        """
        # Insert a record with specified columns
        # Return True upon successful insertion
        # Returns False if insert fails for whatever reason
        """
        try:
            rid = self.table.insert(*columns)     # fixed insert by not using schema_encoding 
            if not rid:
                return False
            return True
        except Exception:
            return False

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Read matching records with specified search key.

        :param search_key: the value you want to search based on
        :param search_key_index: the column index you want to search based on (user columns)
        :param projected_columns_index: which columns to return (list of 0/1)
        Returns a list of Record objects upon success.
        Returns [] if nothing matches.
        Returns False if something exceptional happens.
        """
        try:
            results = []

            # use index if it exists on the search column
            rids = set()
            if 0 <= search_key_index < self.table.num_columns:
                col_index_struct = self.table.index.indices[search_key_index]
            else:
                col_index_struct = None

            if col_index_struct is not None:
                rids = self.table.index.locate(search_key_index, search_key)

            # if no index or cant find via index, do full table scan
            if not rids:
                for rid, (range_idx, is_tail, offset) in self.table.page_directory.items():
                    if is_tail:
                        continue  # logical records are base RIDs only
                    values, _schema = self.table.get_latest_version(rid)
                    if values is None:
                        continue
                    if values[search_key_index] == search_key:
                        rids.add(rid)

            # retrieve records for all matching RIDs
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
        except Exception:
            return False

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # :param relative_version: the relative version of the record you need to retrieve.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """
        try:
            # RIDs with matching search key
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            results = []
            for rid in rids:
                values, _schema = self.table.get_version(rid, relative_version)  # same thing except we get specific version
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

    def update(self, primary_key, *columns):
        """
        # Update a record with specified key and columns
        # Returns True if update is succesful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        try:
            # make sure caller passed exactly num_columns positional values
            if len(columns) != self.table.num_columns:
                return False

            # cannot update primary key via update
            if columns[self.table.key] is not None and columns[self.table.key] != primary_key:
                return False

            # locate RIDs for a given primary key via its index
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids or any(r is None for r in rids):
                return False
            # apply update for each RID
            for rid in rids:
                ok = self.table.update_record(rid, *columns)
                if ok is False:
                    return False

            return True
        except Exception:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        :param start_range: int             # Start of the key range to aggregate
        :param end_range: int               # End of the key range to aggregate
        :param aggregate_column_index: int  # Index of desired column to aggregate
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """
        try:
            # get all rids whose in [start_range, end_range]
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:  # classic
                return False
            total = 0
            for rid in rids:
                values, _ = self.table.get_latest_version(rid)  # summation
                if values is None:
                    continue
                total += values[aggregate_column_index]
            return total
        except Exception:
            return False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        :param start_range: int             # Start of the key range to aggregate
        :param end_range: int               # End of the key range to aggregate
        :param aggregate_column_index: int  # Index of desired column to aggregate
        :param relative_version: the relative version of the record you need to retrieve.
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """
        try:
            # get all rids whose in [start_range, end_range]
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:  # classic
                return False
            total = 0
            for rid in rids:
                values, _ = self.table.get_version(rid, relative_version)  # using get_getversion instead of latest to get specific version
                if values is None:
                    continue
                total += values[aggregate_column_index]  # summation
            return total
        except Exception:
            return False

    def increment(self, key, column):
        """
        increments one column of the record
        this implementation should work if your select and update queries already work
        :param key: the primary key of the record to increment
        :param column: the column to increment
        # Returns True if increment is successful
        # Returns False if no record matches key or if target record is locked by 2PL.
        """
        try:
            records = self.select(key, self.table.key, [1] * self.table.num_columns)
            if records is False or not records:
                return False

            r = records[0]        # Record object
            current_val = r.columns[column]

            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = current_val + 1

            return self.update(key, *updated_columns)
        except Exception:
            return False

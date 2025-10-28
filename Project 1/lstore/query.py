from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass


    def delete(self, primary_key):
        """
        # internal Method
        # Delete records matching the primary_key
        # Returns True upon succesful deletion
        # Return False if record doesn't exist or is locked due to 2PL

        TODO: Whenever a record is deleted from the table, update the index: self.table.index.delete(self.table.key, value, rid)
        """
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids or any(r is None for r in rids): #if there's a missing rid then its False
                return False

            for rid in rids:
                deleted = self.table.delete_record(rid) #needs to return true
                if deleted is False:
                    return False
                self.table.index.delete(self.table.key, primary_key, rid)
            return True
        except Exception:
            return False

    def insert(self, *columns):
        """
        # Insert a record with specified columns
        # Return True upon succesful insertion
        # Returns False if insert fails for whatever reason

        TODO: Whenever a new record is inserted into the table, update the index: self.table.index.insert(self.table.key, value, rid)
        """
        try:
            rid = self.table.insert(*columns)     # fixed insert by not using schema_encoding 
            if not rid:
                return False
            key_col = self.table.key
            self.table.index.insert(key_col, columns[key_col], rid)
            return True
        except Exception:
            return False


    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            results = []
            for rid in rids:
                values, _schema = self.table.get_latest_version(rid) #schemas not used but if unassigned stuff goes bad 
                #pretty sure its needed for next assignment
                if values is None:
                    continue
                # apply projection mask
                projected = [
                    v if bit else None
                    for v, bit in zip(values, projected_columns_index) #mask determining what to select
                ]
                results.append(Record(rid, values[self.table.key], projected))
            return results
        except Exception:
            return False


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    def update(self, primary_key, *columns):
        """
        # Update a record with specified key and columns
        # Returns True if update is succesful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking

        TODO: If the updated column is indexed, you need to update the index: self.table.index.update(column_number, old_value, new_value, rid)
        """
        try:
        # make sure caller passed exactly num_columns positional values
            if len(columns) != self.table.num_columns:
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

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            # get all rids whose in [start_range, end_range]
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if not rids:#classic
                return False
            total = 0
            for rid in rids:
                values, _ = self.table.get_latest_version(rid)
                if values is None:
                    continue
                total += values[aggregate_column_index]
            return total
        except Exception:
            return False


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

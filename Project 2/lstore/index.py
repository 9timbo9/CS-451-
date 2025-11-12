from lstore.config import INDIRECTION_COLUMN


class Index:
    """
    A data structure holding indices for various columns of a table. Key column should be indexed by default,
    other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be
    used as well.
    """

    def __init__(self, table, create_index):
        self.table = table
        self.indices = [None] * table.num_columns  # One index for each table. All are empty initially.
        # TODO: Hash table that stores pointers to B+ trees for each indexed column
        # An index looks like {value: {rid1, rid2, ...}}
        if create_index:
            self.create_index(table.key)  # Key column should be indexed by default

    def create_index(self, column_number):
        """
        # optional: Create index on specific column
        """
        if self.indices[column_number] is not None:
            return  # index already exists for this column
        idx = {}  # {value: {rid1, rid2, ...}}
        self.indices[column_number] = idx  # add index to indices list
        # First, collect all tail RIDs by scanning base records' indirection columns
        tail_rids = set()
        for rid, (pages, offset) in self.table.page_directory.items():
            record = self.table.read_record(rid)
            if record is None:
                continue  # skip deleted records
            # Check if this is a base record
            indirection_rid = record[INDIRECTION_COLUMN]
            if indirection_rid != 0:  # if indirection is non-zero, it's a tail record
                tail_rids.add(indirection_rid)
        # Populate the index, only process base records
        for rid, (pages, offset) in self.table.page_directory.items():
            if rid in tail_rids:
                continue  # Skip tail records
            record = self.table.read_record(rid)
            if record is None:
                continue  # skip deleted records
            latest_values, _ = self.table.get_latest_version(rid)  # Get the latest record version
            if latest_values is None:
                continue  # skip if we can't get the latest version
            value = latest_values[column_number]
            if value not in idx:
                idx[value] = set()  # if the value isn't in the index yet, create a new set
            idx[value].add(rid)  # add the rid to the set for this value

    def drop_index(self, column_number):
        """
        # optional: Drop indexing of a specific column that is not the primary key
        """
        if column_number == self.table.key:
            return  # cannot drop index on key column
        self.indices[column_number] = None  # reset the index to None

    def locate(self, column, value):
        """
        returns the location of all records with the given value on column "column"
        """
        idx = self.indices[column]
        if idx is None:
            return set()  # if we haven't created an index for this column, return empty set
        return idx.get(value, set())  # return the set of RIDs for this value, or empty set if value not found

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end" (inclusive)
        """
        idx = self.indices[column]
        result = set()
        if idx is None:
            return result  # we haven't created an index for this column
        for value, rids in idx.items():
            if begin <= value <= end:
                result.update(rids)  # add all RIDs for this value to the result set
        return result

    def insert(self, column, value, rid):
        """
        Inserts a value into the index for the specified column.
        Should be called whenever a new record is inserted into the table.
        """
        idx = self.indices[column]
        if idx is None:
            return  # index does not exist for this column
        if value not in idx:
            idx[value] = set()
        idx[value].add(rid)

    def delete(self, column, value, rid):
        """
        Deletes a record's RID from the index for the specified column.
        Should be called whenever a record is deleted from the table.
        """
        idx = self.indices[column]
        if idx is None:
            return  # index does not exist for this column
        if value in idx:
            idx[value].discard(rid)
            if not idx[value]:  # if the set is now empty, remove the entry from the index
                del idx[value]

    def update(self, column, old_value, new_value, rid):
        """
        Updates a record's value in the index for the specified column.
        Should be called whenever a record is updated in the table.
        """
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)

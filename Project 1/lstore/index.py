class Index:
    """
    A data structure holding indices for various columns of a table. Key column should be indexed by default,
    other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be
    used as well.
    """

    def __init__(self, table):
        self.indices = [None] * table.num_columns  # One index for each table. All are empty initially.
        pass

    def locate(self, column, value):
        """
        returns the location of all records with the given value on column "column"
        """
        pass

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        pass

    def create_index(self, column_number):
        """
        # optional: Create index on specific column
        """
        pass

    def drop_index(self, column_number):
        """
        # optional: Drop index of specific column
        """
        pass

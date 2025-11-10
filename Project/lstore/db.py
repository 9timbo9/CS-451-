from lstore.table import Table


class Database:

    def __init__(self):
        self.tables = {}  # Store tables by name

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table

        :param name: string         # Table name
        :param num_columns: int     # Number of Columns: all columns are integer
        :param key_index: int       # Index of table key in columns
        """
        if name in self.tables:
            raise Exception(f"\"{name}\" table already exists in db")
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        """
        Deletes the specified table
        """
        if name not in self.tables:
            raise Exception(f"\"{name}\" table doesnt exist in db")
        del self.tables[name]

    def get_table(self, name):
        """
        Returns table with the passed name
        """
        if name not in self.tables:
            raise Exception(f"\"{name}\" table doesnt exist in db")
        return self.tables[name]

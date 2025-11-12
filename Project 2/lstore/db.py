from lstore.table import Table, PageRange
from lstore.page import Page
from lstore.index import Index
import os
import struct

BOOL = '?'              # 1 byte
UNSIGNED_SHORT = 'H'    # 2 bytes
UNSIGNED_INT = 'I'      # 4 bytes


class Database:

    def __init__(self):
        self.tables = {}  # Store tables by name
        self.path = None

    def __str__(self):
        return f"Database(tables={list(self.tables.keys())})"

    def __repr__(self):
        return self.__str__()

    # Required for milestone2
    def open(self, path, delete_existing=False):
        self.path = path
        if not os.path.exists(path):
            return  # No existing database to load
        # Delete existing files if specified
        if delete_existing:
            for filename in os.listdir(path):
                file_path = os.path.join(path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            print("Existing database files deleted.")
            return
        metadata_file = os.path.join(path, "db_metadata.bin")
        if not os.path.exists(metadata_file):
            return  # No metadata, nothing to load
        with open(metadata_file, 'rb') as f:
            # Read number of tables
            num_tables = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
            for _ in range(num_tables):
                # Read table name and length
                name_len = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
                table_name = f.read(name_len).decode('utf-8')
                # Load each table
                self._load_table(path, table_name)
        print(f"Database opened from disk and {num_tables} tables loaded successfully from \"{self.path}\"")

    def close(self):
        if not hasattr(self, 'path') or self.path is None:
            return
        os.makedirs(self.path, exist_ok=True)
        metadata_file = os.path.join(self.path, "db_metadata.bin")
        with open(metadata_file, 'wb') as f:
            # Write the number of tables
            f.write(struct.pack(UNSIGNED_SHORT, len(self.tables)))
            for table_name in self.tables.keys():
                # Write table name length and name
                name_bytes = table_name.encode('utf-8')
                f.write(struct.pack(UNSIGNED_SHORT, len(name_bytes)))
                f.write(name_bytes)
        for table_name, table in self.tables.items():
            self._save_table(table)
        print(f"Database closed and saved to disk at \"{self.path}\"")

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

    def _write_pages(self, f, page_list):
        """
        Write a list of pages to disk
        """
        f.write(struct.pack(UNSIGNED_SHORT, len(page_list)))
        for page in page_list:
            f.write(struct.pack(UNSIGNED_SHORT, page.num_records))
            f.write(page.data)
    
    def _read_pages(self, f):
        """
        Read a list of pages from disk
        """
        num_pages = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
        page_list = []
        for _ in range(num_pages):
            page = Page()
            page.num_records = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
            page.data = bytearray(f.read(4096))
            page_list.append(page)
        return page_list

    def _save_page_range(self, f, page_range):
        """
        Save a PageRange to disk
        """
        f.write(struct.pack(UNSIGNED_SHORT, page_range.num_base_records))
        f.write(struct.pack(UNSIGNED_SHORT, page_range.num_tail_records))
        # Save base pages
        f.write(struct.pack(UNSIGNED_SHORT, len(page_range.base_pages)))
        for page_list in page_range.base_pages:
            self._write_pages(f, page_list)
        # Save tail pages
        f.write(struct.pack(UNSIGNED_SHORT, len(page_range.tail_pages)))
        for page_list in page_range.tail_pages:
            self._write_pages(f, page_list)

    def _load_page_range(self, f, num_columns):
        """
        Load a PageRange from disk
        """
        page_range = PageRange(num_columns)
        page_range.num_base_records = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
        page_range.num_tail_records = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
        # Load base pages
        num_base_page_lists = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
        page_range.base_pages = []
        for _ in range(num_base_page_lists):
            page_list = self._read_pages(f)
            page_range.base_pages.append(page_list)
        # Load tail pages
        num_tail_page_lists = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
        page_range.tail_pages = []
        for _ in range(num_tail_page_lists):
            page_list = self._read_pages(f)
            page_range.tail_pages.append(page_list)
        return page_range

    def _save_table(self, table):
        """
        Save a single table to disk
        """
        table_file = os.path.join(self.path, f"{table.name}.bin")
        with open(table_file, 'wb') as f:
            # Write table metadata
            f.write(struct.pack(UNSIGNED_SHORT, table.num_columns))
            f.write(struct.pack(UNSIGNED_SHORT, table.key))
            f.write(struct.pack(UNSIGNED_INT, table.next_rid))
            # Write number of page ranges
            f.write(struct.pack(UNSIGNED_INT, len(table.page_ranges)))
            for pr in table.page_ranges:
                self._save_page_range(f, pr)
            # Write page directory
            f.write(struct.pack(UNSIGNED_INT, len(table.page_directory)))
            pages_to_range = {}
            for idx, pr in enumerate(table.page_ranges):
                pages_to_range[id(pr.base_pages)] = (idx, False)
                pages_to_range[id(pr.tail_pages)] = (idx, True)
            for rid, (pages, offset) in table.page_directory.items():
                f.write(struct.pack(UNSIGNED_INT, rid))
                range_idx, is_tail = pages_to_range[id(pages)]
                f.write(struct.pack(UNSIGNED_SHORT, range_idx))
                f.write(struct.pack(BOOL, is_tail))
                f.write(struct.pack(UNSIGNED_SHORT, offset))

    def _load_table(self, path, table_name):
        """
        Load a single table from disk
        """
        table_file = os.path.join(path, f"{table_name}.bin")
        with open(table_file, 'rb') as f:
            # Read table metadata
            num_col = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
            key_idx = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
            next_rid = struct.unpack(UNSIGNED_INT, f.read(4))[0]
            # Create table
            table = Table(table_name, num_col, key_idx, create_index=False)
            table.next_rid = next_rid
            # Read number of page ranges
            num_page_ranges = struct.unpack(UNSIGNED_INT, f.read(4))[0]
            for _ in range(num_page_ranges):
                pr = self._load_page_range(f, table.total_columns)
                table.page_ranges.append(pr)
            # Rebuild page directory
            num_directory_entries = struct.unpack(UNSIGNED_INT, f.read(4))[0]
            for _ in range(num_directory_entries):
                rid = struct.unpack(UNSIGNED_INT, f.read(4))[0]
                range_idx = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
                is_tail = struct.unpack(BOOL, f.read(1))[0]
                offset = struct.unpack(UNSIGNED_SHORT, f.read(2))[0]
                page_range = table.page_ranges[range_idx]
                if is_tail:
                    pages = page_range.tail_pages
                else:
                    pages = page_range.base_pages
                table.page_directory[rid] = (pages, offset)
            # Set current page ranges
            if table.page_ranges:
                for pr in reversed(table.page_ranges):
                    if pr.has_capacity():
                        table.current_page_range = pr
                        break
                else:
                    # All are full, create a new one on next insert
                    table.current_page_range = table.page_ranges[-1]
                # Find the last page range with capacity for tail records
                for pr in reversed(table.page_ranges):
                    if pr.num_tail_records < pr.max_records:
                        table.current_tail_page_range = pr
                        break
                else:
                    # All are full, will create a new one on next update
                    table.current_tail_page_range = table.page_ranges[-1]
            table.index = Index(table, create_index=True)
            self.tables[table_name] = table

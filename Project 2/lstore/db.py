from lstore.table import Table, PageRange
from lstore.page import Page
from lstore.index import Index
from lstore.disk import DiskManager
from lstore.bufferpool import Bufferpool
from lstore.config import BUFFERPOOL_CAPACITY
import os
# import json


class Database:

    def __init__(self):
        self.tables = {}  # Store tables by name
        self.path = None
        self.disk_manager = None
        self.bufferpool = None

    def __str__(self):
        return f"Database(tables={list(self.tables.keys())})"

    def __repr__(self):
        return self.__str__()

    # Required for milestone2
    def open(self, path, delete_existing=False, bufferpool_capacity=BUFFERPOOL_CAPACITY):
        self.path = path
        self.disk_manager = DiskManager(path)
        self.bufferpool = Bufferpool(self.disk_manager, bufferpool_capacity)
        if not os.path.exists(path):
            return  # No existing database to load
        # Delete existing files if specified
        if delete_existing:
            # TODO: drag me out into the street and shoot me
            import shutil
            if os.path.exists(path):
                try:
                    shutil.rmtree(path)
                except (PermissionError, OSError) as e:
                    # On Windows, files might be locked - try to delete individual files
                    print(f"Warning: Could not delete directory, trying individual files: {e}")
                    for root, dirs, files in os.walk(path, topdown=False):
                        for name in files:
                            try:
                                os.remove(os.path.join(root, name))
                            except:
                                pass
                        for name in dirs:
                            try:
                                os.rmdir(os.path.join(root, name))
                            except:
                                pass
                    try:
                        os.rmdir(path)
                    except:
                        pass
            os.makedirs(path, exist_ok=True)
            print("Existing database files deleted.")
            return
        # Load table metadata from JSON files
        tables_dir = os.path.join(path, "tables")
        if not os.path.exists(tables_dir):
            return  # No tables directory, nothing to load
        table_names = []
        for item in os.listdir(tables_dir):
            table_path = os.path.join(tables_dir, item)
            if os.path.isdir(table_path):
                # Check if meta.json exists
                meta = self.disk_manager.read_meta(item)
                if meta:
                    self._load_table_metadata(item, meta)
                    table_names.append(item)
        if table_names:
            print(f"Database opened from disk and {len(table_names)} tables loaded successfully from \"{self.path}\"")

    def close(self):
        if not hasattr(self, 'path') or self.path is None:
            return
        # save all pages from tables to disk
        for table_name, table in self.tables.items():
            self._save_all_pages(table)
        # write dirty pages to disk
        if self.bufferpool is not None:
            self.bufferpool.flush_all()
        # save table metadata
        for table_name, table in self.tables.items():
            self._save_table_metadata(table)
        print(f"Database closed and saved to disk at \"{self.path}\"")
    
    def _save_all_pages(self, table):
        """
        Save all pages from table to individual page files via DiskManager
        This ensures all pages are persisted even if not in bufferpool
        """
        for range_idx, pr in enumerate(table.page_ranges):
            # Save base pages
            for col_idx, page_list in enumerate(pr.base_pages):
                for page_idx, page in enumerate(page_list):
                    if page is not None:
                        self.disk_manager.write_page(table.name, False, col_idx, range_idx, page_idx, page.data)
            # Save tail pages
            for col_idx, page_list in enumerate(pr.tail_pages):
                for page_idx, page in enumerate(page_list):
                    if page is not None:
                        self.disk_manager.write_page(table.name, True, col_idx, range_idx, page_idx, page.data)

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table

        :param name: string         # Table name
        :param num_columns: int     # Number of Columns: all columns are integer
        :param key_index: int       # Index of table key in columns
        """
        if name in self.tables:
            raise Exception(f"\"{name}\" table already exists in db")
        # Initialize bufferpool if open wasn't called
        if self.bufferpool is None:
            if self.path is None:
                # Use a default path if none set
                self.path = "./default_db"
            self.disk_manager = DiskManager(self.path)
            self.bufferpool = Bufferpool(self.disk_manager, BUFFERPOOL_CAPACITY)
        table = Table(name, num_columns, key_index, bufferpool=self.bufferpool)
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

    def _save_table_metadata(self, table):
        """
        Save table metadata to JSON file
        Pages are saved individually via DiskManager when bufferpool flushes
        """
        # build page directory info
        page_directory_info = {}
        for rid, (pages, offset) in table.page_directory.items():
            # find which page range this pages list belongs to
            range_idx = None
            is_tail = None
            for idx, pr in enumerate(table.page_ranges):
                if pages is pr.base_pages:
                    range_idx = idx
                    is_tail = False
                    break
                elif pages is pr.tail_pages:
                    range_idx = idx
                    is_tail = True
                    break
            if range_idx is not None:
                page_directory_info[rid] = {
                    'range_idx': range_idx,
                    'is_tail': is_tail,
                    'offset': offset
                }
        # build page range info
        page_ranges_info = []
        for pr in table.page_ranges:
            page_ranges_info.append({
                'num_base_records': pr.num_base_records,
                'num_tail_records': pr.num_tail_records,
                'num_base_pages_per_col': [len(page_list) for page_list in pr.base_pages],
                'num_tail_pages_per_col': [len(page_list) for page_list in pr.tail_pages]
            })
        # find current page range indices
        current_range_idx = None
        current_tail_range_idx = None
        if table.current_page_range:  # base
            for idx, pr in enumerate(table.page_ranges):
                if pr is table.current_page_range:
                    current_range_idx = idx
                    break
        if table.current_tail_page_range:  # tail
            for idx, pr in enumerate(table.page_ranges):
                if pr is table.current_tail_page_range:
                    current_tail_range_idx = idx
                    break
        metadata = {
            'num_columns': table.num_columns,
            'key_index': table.key,
            'next_rid': table.next_rid,
            'page_ranges': page_ranges_info,
            'page_directory': page_directory_info,
            'current_range_idx': current_range_idx,
            'current_tail_range_idx': current_tail_range_idx
        }
        self.disk_manager.write_meta(table.name, metadata)
    
    def _load_table_metadata(self, table_name, metadata):
        """
        Load table metadata from JSON and reconstruct table structure
        Load pages from individual page files
        """
        num_col, key_idx, next_rid = metadata['num_columns'], metadata['key_index'], metadata['next_rid']
        # create table
        table = Table(table_name, num_col, key_idx, create_index=False, bufferpool=self.bufferpool)
        table.next_rid = next_rid
        # rebuild page ranges and load pages
        for range_idx, pr_info in enumerate(metadata['page_ranges']):
            pr = PageRange(table.total_columns)
            pr.num_base_records = pr_info['num_base_records']
            pr.num_tail_records = pr_info['num_tail_records']
            # load base pages
            for col_idx, num_pages in enumerate(pr_info['num_base_pages_per_col']):
                pr.base_pages[col_idx] = []
                for page_idx in range(num_pages):
                    # load individual page
                    page_data = self.disk_manager.read_page(table_name, False, col_idx, range_idx, page_idx)
                    page = Page()
                    page.data[:] = page_data
                    # Calculate num_records for base pages
                    records_in_previous_pages = page_idx * 512
                    remaining_records = pr.num_base_records - records_in_previous_pages
                    if remaining_records > 512:
                        page.num_records = 512
                    elif remaining_records > 0:
                        page.num_records = remaining_records
                    else:
                        page.num_records = 0
                    pr.base_pages[col_idx].append(page)
            # load tail pages
            for col_idx, num_pages in enumerate(pr_info['num_tail_pages_per_col']):
                pr.tail_pages[col_idx] = []
                for page_idx in range(num_pages):
                    # load individual page
                    page_data = self.disk_manager.read_page(table_name, True, col_idx, range_idx, page_idx)
                    page = Page()
                    page.data[:] = page_data
                    # Calculate num_records for tail pages
                    records_in_previous_pages = page_idx * 512
                    remaining_records = pr.num_tail_records - records_in_previous_pages
                    if remaining_records > 512:
                        page.num_records = 512
                    elif remaining_records > 0:
                        page.num_records = remaining_records
                    else:
                        page.num_records = 0
                    pr.tail_pages[col_idx].append(page)
            table.page_ranges.append(pr)
        # rebuild page directory
        for rid, info in metadata['page_directory'].items():
            rid = int(rid)
            range_idx = info['range_idx']
            is_tail = info['is_tail']
            offset = info['offset']
            page_range = table.page_ranges[range_idx]
            if is_tail:
                pages = page_range.tail_pages
            else:
                pages = page_range.base_pages
            table.page_directory[rid] = (pages, offset)
        # set current page ranges
        if metadata.get('current_range_idx') is not None:
            table.current_page_range = table.page_ranges[metadata['current_range_idx']]
        elif table.page_ranges:
            # Find first range with capacity
            for pr in reversed(table.page_ranges):
                if pr.has_capacity():
                    table.current_page_range = pr
                    break
            else:
                table.current_page_range = table.page_ranges[-1]
        if metadata.get('current_tail_range_idx') is not None:
            table.current_tail_page_range = table.page_ranges[metadata['current_tail_range_idx']]
        elif table.page_ranges:
            # Find first range with tail capacity
            for pr in reversed(table.page_ranges):
                if pr.num_tail_records < pr.max_records:
                    table.current_tail_page_range = pr
                    break
            else:
                table.current_tail_page_range = table.page_ranges[-1]
        # Rebuild index
        table.index = Index(table, create_index=True)
        self.tables[table_name] = table

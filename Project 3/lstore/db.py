from lstore.table import Table, PageRange
from lstore.transaction import Transaction
from lstore.index import Index
from lstore.disk import DiskManager
from lstore.bufferpool import Bufferpool
from lstore.config import BUFFERPOOL_CAPACITY, RECORDS_PER_PAGE
import os
import shutil


class Database:
    def __init__(self):
        self.tables = {} # Store tables by name
        self.path = None
        self.disk_manager = None
        self.bufferpool = None
        self.lock_manager = None
    
    def create_transaction(self, use_locking=True):
        """Create a new transaction"""
        if use_locking and self.lock_manager is None:
            from lstore.lock_manager import LockManager
            self.lock_manager = LockManager()
        
        if use_locking:
            return Transaction(self.lock_manager)
        else:
            return Transaction()  # without locking for backward compatibility
    
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
            if os.path.exists(path):
                try:
                    shutil.rmtree(path)
                except (PermissionError, OSError) as e:
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
        if not self.path:
            return

        # Stop all merge threads before closing
        for _, table in self.tables.items():
            table.stop_merge_thread()

        # write dirty pages to disk
        if self.bufferpool is not None:
            self.bufferpool.flush_all()

        # save table metadata
        for _, table in self.tables.items():
            self._save_table_metadata(table)

        print(f'Database closed and saved to disk at "{self.path}"')

    def create_table(self, name, num_columns, key_index):
        """
        Creates a new table

        :param name: string         # Table name
        :param num_columns: int     # Number of Columns: all columns are integer
        :param key_index: int       # Index of table key in columns
        """
        # If a table with this name already exists, reset it instead of failing.
        if name in self.tables:
            old_table = self.tables[name]
            # Stop merge thread before removing old table
            old_table.stop_merge_thread()
            del self.tables[name]

            # Clear its on-disk directory so old pages don't linger
            if self.disk_manager is not None:
                table_dir = self.disk_manager.table_dir(name)
                if os.path.exists(table_dir):
                    import shutil
                    shutil.rmtree(table_dir, ignore_errors=True)

            # ALSO clear any bufferpool frames that belong to this table
            if self.bufferpool is not None:
                for pid in list(self.bufferpool.frames.keys()):
                    # pid is (table_name, is_tail, col, rng, idx)
                    if pid[0] == name:
                        del self.bufferpool.frames[pid]
                        if pid in self.bufferpool.lru:
                            del self.bufferpool.lru[pid]

        # Initialize bufferpool if open() wasn't called
        if self.bufferpool is None:
            if self.path is None:
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
        table = self.tables[name]
        # Stop merge thread before dropping table
        table.stop_merge_thread()
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
        Save table metadata to JSON file.
        We only store logical info: record counts, page counts,
        and page directory; page bytes are already on disk.
        """
        # page_directory: rid -> (range_idx, is_tail, offset)
        page_directory_info = {}
        for rid, (range_idx, is_tail, offset) in table.page_directory.items():
            page_directory_info[rid] = {
                "range_idx": range_idx,
                "is_tail": is_tail,
                "offset": offset,
            }

        # page_ranges info
        page_ranges_info = []
        for pr in table.page_ranges:
            page_ranges_info.append(
                {
                    "num_base_records": pr.num_base_records,
                    "num_tail_records": pr.num_tail_records,
                    "num_base_pages_per_col": pr.num_base_pages_per_col,
                    "num_tail_pages_per_col": pr.num_tail_pages_per_col,
                }
            )

        # current page range indices
        current_range_idx = None
        current_tail_range_idx = None
        if table.current_page_range is not None:
            current_range_idx = table.current_page_range.range_idx
        if table.current_tail_page_range is not None:
            current_tail_range_idx = table.current_tail_page_range.range_idx

        # Save which columns have indexes
        indexed_columns = []
        for col_num in range(table.num_columns):
            if table.index.indices[col_num] is not None:
                indexed_columns.append(col_num)

        metadata = {
            "num_columns": table.num_columns,
            "key_index": table.key,
            "next_rid": table.next_rid,
            "page_ranges": page_ranges_info,
            "page_directory": page_directory_info,
            "current_range_idx": current_range_idx,
            "current_tail_range_idx": current_tail_range_idx,
            "updates_since_merge": table.updates_since_merge,
            "indexed_columns": indexed_columns,
        }

        self.disk_manager.write_meta(table.name, metadata)
    
    def _load_table_metadata(self, table_name, metadata):
        """
        Load table metadata from JSON and reconstruct logical table structure.
        Actual page bytes are read lazily via the bufferpool.
        """
        num_col = metadata["num_columns"]
        key_idx = metadata["key_index"]
        next_rid = metadata["next_rid"]

        # create table (no index yet)
        table = Table(
            table_name, num_col, key_idx, create_index=False, bufferpool=self.bufferpool
        )
        table.next_rid = next_rid
        
        # Restore update counter if it exists in metadata
        table.updates_since_merge = metadata.get("updates_since_merge", 0)

        # rebuild page ranges
        for range_idx, pr_info in enumerate(metadata["page_ranges"]):
            # NOTE: new PageRange constructor: (table, range_idx)
            pr = PageRange(table, range_idx)
            pr.num_base_records = pr_info["num_base_records"]
            pr.num_tail_records = pr_info["num_tail_records"]
            pr.num_base_pages_per_col = pr_info["num_base_pages_per_col"]
            pr.num_tail_pages_per_col = pr_info["num_tail_pages_per_col"]

            table.page_ranges.append(pr)

            # reconstruct Page.num_records for base pages
            for col_idx, num_pages in enumerate(pr.num_base_pages_per_col):
                for page_idx in range(num_pages):
                    pid = (table_name, False, col_idx, range_idx, page_idx)
                    page = self.bufferpool.fix_page(pid, mode="r")

                    records_before = page_idx * RECORDS_PER_PAGE
                    remaining = pr.num_base_records - records_before
                    if remaining > RECORDS_PER_PAGE:
                        page.num_records = RECORDS_PER_PAGE
                    elif remaining > 0:
                        page.num_records = remaining
                    else:
                        page.num_records = 0

                    self.bufferpool.unfix_page(pid)

            # reconstruct Page.num_records for tail pages
            for col_idx, num_pages in enumerate(pr.num_tail_pages_per_col):
                for page_idx in range(num_pages):
                    pid = (table_name, True, col_idx, range_idx, page_idx)
                    page = self.bufferpool.fix_page(pid, mode="r")

                    records_before = page_idx * RECORDS_PER_PAGE
                    remaining = pr.num_tail_records - records_before
                    if remaining > RECORDS_PER_PAGE:
                        page.num_records = RECORDS_PER_PAGE
                    elif remaining > 0:
                        page.num_records = remaining
                    else:
                        page.num_records = 0

                    self.bufferpool.unfix_page(pid)

        # rebuild page directory: rid -> (range_idx, is_tail, offset)
        for rid, info in metadata["page_directory"].items():
            rid_int = int(rid)
            table.page_directory[rid_int] = (
                info["range_idx"],
                info["is_tail"],
                info["offset"],
            )

        # set current page ranges
        if metadata.get("current_range_idx") is not None:
            table.current_page_range = table.page_ranges[metadata["current_range_idx"]]
        elif table.page_ranges:
            # find last range with capacity as "current"
            for pr in reversed(table.page_ranges):
                if pr.has_capacity():
                    table.current_page_range = pr
                    break
            else:
                table.current_page_range = table.page_ranges[-1]

        if metadata.get("current_tail_range_idx") is not None:
            table.current_tail_page_range = table.page_ranges[
                metadata["current_tail_range_idx"]
            ]
        elif table.page_ranges:
            for pr in reversed(table.page_ranges):
                if pr.num_tail_records < pr.max_records:
                    table.current_tail_page_range = pr
                    break
            else:
                table.current_tail_page_range = table.page_ranges[-1]

        # rebuild indexes (primary key + any others that were saved)
        table.index = Index(table, create_index=True)  # This creates the primary key index
        
        # Restore indexes on other columns that were saved
        indexed_columns = metadata.get("indexed_columns", [])
        for col_num in indexed_columns:
            if col_num != key_idx:  # Primary key is already indexed
                table.index.create_index(col_num)
        
        self.tables[table_name] = table

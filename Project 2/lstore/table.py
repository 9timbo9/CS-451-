from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from time import time
import os
import threading


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return f"Record(RID={self.rid}, Key={self.key}, Columns={self.columns})"

    def __repr__(self):
        return self.__str__()


class PageRange:
    """
    Manages a range of base and tail pages
    """
    def __init__(self, table, range_idx):
        self.table = table
        self.range_idx = range_idx
        self.num_columns = table.total_columns  # includes metadata columns

        # logical record counts in this range
        self.num_base_records = 0
        self.num_tail_records = 0

        # page counts per column
        # start with 1 page per column for base and tail
        self.num_base_pages_per_col = [1] * self.num_columns
        self.num_tail_pages_per_col = [1] * self.num_columns

        # capacity: 16 pages * RECORDS_PER_PAGE records/page
        self.max_records = RECORDS_PER_PAGE * 16

    def has_capacity(self):
        """
        Just checking if there is space
        """
        return self.num_base_records < self.max_records
    
    def _page_id(self, is_tail, col_index, page_index):
        """
        Build the page_id tuple used by the bufferpool.
        """
        return (self.table.name, is_tail, col_index, self.range_idx, page_index)

    def write_base_record(self, record_data):
        """
        Append a base record (metadata+user columns).
        Returns the offset where the record was written (0-based index within this range).
        """
        offset = self.num_base_records
        page_index = offset // RECORDS_PER_PAGE

        for col_index, value in enumerate(record_data):
            # ensure we have enough pages for this column
            if page_index >= self.num_base_pages_per_col[col_index]:
                self.num_base_pages_per_col[col_index] += 1

            pid = self._page_id(False, col_index, page_index)
            page = self.table.bufferpool.fix_page(pid, mode="w")
            # appending: Page.write writes at page.num_records
            page.write(value)
            self.table.bufferpool.unfix_page(pid, dirty=True)

        self.num_base_records += 1
        return offset
    
    def read_base_record(self, offset):
        """
        Read a base record at given logical offset and return full [meta+user] list.
        """
        record_data = []
        page_index = offset // RECORDS_PER_PAGE
        slot_in_page = offset % RECORDS_PER_PAGE

        for col_index in range(self.num_columns):
            pid = self._page_id(False, col_index, page_index)
            page = self.table.bufferpool.fix_page(pid, mode="r")
            value = page.read(slot_in_page)
            self.table.bufferpool.unfix_page(pid)
            record_data.append(value)

        return record_data

    def update_base_column(self, offset, col_index, value):
        """
        Overwrite a specific column in a base record at 'offset'.
        col_index is the *physical* column index (includes metadata).
        """
        page_index = offset // RECORDS_PER_PAGE
        slot_in_page = offset % RECORDS_PER_PAGE

        pid = self._page_id(False, col_index, page_index)
        page = self.table.bufferpool.fix_page(pid, mode="w")
        page.update(slot_in_page, value)
        self.table.bufferpool.unfix_page(pid, dirty=True)
    
    def write_tail_record(self, record_data):
        """
        Append a tail record (metadata+user columns).
        Returns the offset where the tail record was written.
        """
        offset = self.num_tail_records
        page_index = offset // RECORDS_PER_PAGE

        for col_index, value in enumerate(record_data):
            if page_index >= self.num_tail_pages_per_col[col_index]:
                self.num_tail_pages_per_col[col_index] += 1

            pid = self._page_id(True, col_index, page_index)
            page = self.table.bufferpool.fix_page(pid, mode="w")
            page.write(value)
            self.table.bufferpool.unfix_page(pid, dirty=True)

        self.num_tail_records += 1
        return offset

    def read_tail_record(self, offset):
        """
        Read a tail record at given offset and return full [meta+user] list.
        """
        record_data = []
        page_index = offset // RECORDS_PER_PAGE
        slot_in_page = offset % RECORDS_PER_PAGE

        for col_index in range(self.num_columns):
            pid = self._page_id(True, col_index, page_index)
            page = self.table.bufferpool.fix_page(pid, mode="r")
            value = page.read(slot_in_page)
            self.table.bufferpool.unfix_page(pid)
            record_data.append(value)

        return record_data


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, create_index=True, bufferpool=None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # INDIRECTION, RID, TIMESTAMP, SCHEMA_ENCODING
        self.total_columns = 4 + num_columns
        self.page_directory = {}
        self.page_ranges = []
        self.current_page_range = None
        self.current_tail_page_range = None
        self.next_rid = 1
        self.DELETED_RID = 0  # if rid is 0 then it is deleted
        self.bufferpool = bufferpool
        self.index = Index(self, create_index)
        
        # Update-based merge tracking
        self.updates_since_merge = 0
        
        # Background merge thread
        self._merge_lock = threading.Lock()  # Lock for thread-safe merge operations
        self._merge_thread = None
        self._merge_thread_stop = threading.Event()  # signals thread to stop
        self._merge_in_progress = False  # prevent concurrent merges
        self._start_merge_thread()

    def __str__(self):
        return f'Table(name="{self.name}", num_columns={self.num_columns}, key={self.key})'

    def __repr__(self):
        return self.__str__()

    def _get_or_create_page_range(self):
        """Get current page range or create a new one if the current is full."""
        if self.current_page_range is None or not self.current_page_range.has_capacity():
            pr = PageRange(self, len(self.page_ranges))
            self.page_ranges.append(pr)
            self.current_page_range = pr
        return self.current_page_range

    
    def insert(self, *columns):
        """
        Insert a new base record and return the RID of the inserted record.
        """
        if len(columns) != self.num_columns:
            raise ValueError(f"Expected {self.num_columns} columns, got {len(columns)}")

        # enforce unique primary key
        if self.index.locate(self.key, columns[self.key]):
            raise ValueError(
                f"Duplicate entry for primary key column {self.key} with value {columns[self.key]}"
            )

        rid = self.next_rid
        self.next_rid += 1

        indirection = 0  # no tail yet
        timestamp = int(time())
        schema_encoding = 0

        full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)

        page_range = self._get_or_create_page_range()
        offset = page_range.write_base_record(full_record)

        # base record
        self.page_directory[rid] = (page_range.range_idx, False, offset)

        # update indices
        for col_num in range(self.num_columns):
            if self.index.indices[col_num] is not None:
                value = columns[col_num]
                self.index.insert(col_num, value, rid)

        return rid

    def read_record(self, rid):
        """
        Read a record (base or tail) by RID.
        Returns full [meta+user] list, or None if deleted/not present.
        """
        loc = self.page_directory.get(rid)
        if loc is None:
            return None

        range_idx, is_tail, offset = loc
        page_range = self.page_ranges[range_idx]

        if not is_tail:
            record_data = page_range.read_base_record(offset)
        else:
            record_data = page_range.read_tail_record(offset)

        if record_data[RID_COLUMN] == self.DELETED_RID:
            return None

        return record_data
    
    def get_latest_version(self, rid):
        """
        Get the latest version of a record by following the indirection chain.

        Returns (user_columns_list, schema_encoding).
        NOTE: This version ignores TPS for simplicity and correctness.
        """
        base_record = self.read_record(rid)
        if base_record is None:
            return None, None

        indirection_rid = base_record[INDIRECTION_COLUMN]

        # If there's no tail chain, the base record is the latest
        if indirection_rid == 0:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]

        # Otherwise, follow the pointer to the latest tail record
        tail_record = self.read_record(indirection_rid)
        if tail_record is None:
            # Fallback: if something went wrong, return base
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]

        return tail_record[4:], tail_record[SCHEMA_ENCODING_COLUMN]
        
    def update_record(self, rid, *columns):
        """
        Update a record by creating a new tail record.
        'columns' is a list where None means no change for that column.
        """
        base_record = self.read_record(rid)
        if base_record is None:
            return False

        latest_values, current_schema = self.get_latest_version(rid)

        tail_rid = self.next_rid
        self.next_rid += 1

        prev_tail_rid = base_record[INDIRECTION_COLUMN]

        new_schema = current_schema
        updated_columns_info = []  # (col_index, old_val, new_val)

        for i, value in enumerate(columns):
            if value is not None:
                new_schema |= (1 << i)
                updated_columns_info.append((i, latest_values[i], value))

        # build tail record [meta] + [user columns]
        tail_data = [prev_tail_rid, tail_rid, int(time()), new_schema]
        for i in range(self.num_columns):
            if columns[i] is not None:
                tail_data.append(columns[i])
            else:
                tail_data.append(latest_values[i])

        # pick a page range for the tail
        if (
            self.current_tail_page_range is None
            or self.current_tail_page_range.num_tail_records >= RECORDS_PER_PAGE * 16
        ):
            self.current_tail_page_range = self._get_or_create_page_range()

        page_range = self.current_tail_page_range
        offset = page_range.write_tail_record(tail_data)

        # record the tail in page directory
        self.page_directory[tail_rid] = (page_range.range_idx, True, offset)

        # update base record's indirection + schema encoding in base pages
        base_range_idx, is_tail, base_offset = self.page_directory[rid]
        assert not is_tail
        base_pr = self.page_ranges[base_range_idx]

        base_pr.update_base_column(base_offset, INDIRECTION_COLUMN, tail_rid)
        base_pr.update_base_column(base_offset, SCHEMA_ENCODING_COLUMN, new_schema)

        # update all relevant secondary indexes
        for col_num, old_value, new_value in updated_columns_info:
            if self.index.indices[col_num] is not None:
                self.index.update(col_num, old_value, new_value, rid)

        # increment update counter for merge tracking
        with self._merge_lock:
            self.updates_since_merge += 1

        return True

    def delete_record(self, rid):
        """
        Mark a record as deleted by setting RID to DELETED_RID in the base record
        and remove it from all indexes.
        """
        loc = self.page_directory.get(rid)
        if loc is None:
            return False

        base_record = self.read_record(rid)
        if base_record is None:
            return False

        range_idx, is_tail, offset = loc
        if is_tail:
            # we only logically delete via base record in this implementation
            return False

        # get latest version to ensure we have the correct values for index deletion
        latest_values, _ = self.get_latest_version(rid)
        if latest_values is None:
            return False

        pr = self.page_ranges[range_idx]
        pr.update_base_column(offset, RID_COLUMN, self.DELETED_RID)

        # drop from indexes using latest values
        for col_num in range(self.num_columns):
            if self.index.indices[col_num] is not None:
                value = latest_values[col_num]
                self.index.delete(col_num, value, rid)

        return True
    
    def get_version(self, rid, relative_version):
        """
        Get a specific version of a record.
        relative_version is a negative number like -2 for "2 versions ago"
        or 0 for "latest".
        Returns (user_columns_list, schema_encoding).
        """
        base_record = self.read_record(rid)
        if base_record is None:
            return None, None

        if relative_version == 0:
            return self.get_latest_version(rid)

        # start from latest tail record
        tail_rid = base_record[INDIRECTION_COLUMN]
        if tail_rid == 0:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]

        steps = abs(relative_version)
        curr_tail_rid = tail_rid

        for _ in range(steps):
            if curr_tail_rid == 0:
                break
            tail_record = self.read_record(curr_tail_rid)
            if tail_record is None:
                return None, None
            curr_tail_rid = tail_record[INDIRECTION_COLUMN]

        if curr_tail_rid == 0:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]
        else:
            version_record = self.read_record(curr_tail_rid)
            if version_record is None:
                return None, None
            return version_record[4:], version_record[SCHEMA_ENCODING_COLUMN]

    def __merge(self):
        """
        Simple, in-place merge:
        - For each base record in each PageRange:
          * compute the latest values using get_latest_version()
          * overwrite the user columns + schema encoding in the base pages
          * update TPS on the base page to reflect the merged tail RID
        """
        if not self.page_ranges:
            return

        for page_range in self.page_ranges:
            if page_range.num_base_records == 0:
                continue

            last_page_idx = -1
            current_tps = 0
            # For each base-record offset within this page range
            for offset in range(page_range.num_base_records):
                page_index = offset // RECORDS_PER_PAGE
                # only read tps when on a new page
                if page_index != last_page_idx:
                    pid = page_range._page_id(False, RID_COLUMN, page_index)
                    page = self.bufferpool.fix_page(pid, mode="r")
                    current_tps = page.get_tps()
                    self.bufferpool.unfix_page(pid)
                    last_page_idx = page_index

                base_record = page_range.read_base_record(offset)
                if not base_record:
                    continue

                rid = base_record[RID_COLUMN]
                tail_rid = base_record[INDIRECTION_COLUMN]

                # Skip deleted or empty slots
                if rid == self.DELETED_RID or rid == 0:
                    continue

                # Skip if this record's tail chain was already merged
                if tail_rid != 0 and tail_rid <= current_tps:
                    continue  # Already merged up to this tail

                latest_values, schema_encoding = self.get_latest_version(rid)
                if latest_values is None:
                    continue

                # Overwrite user columns in the base pages with the consolidated version
                for user_col_idx, value in enumerate(latest_values):
                    physical_col_idx = 4 + user_col_idx  # skip metadata columns
                    page_range.update_base_column(offset, physical_col_idx, value)

                # Update the schema-encoding metadata column in the base record
                page_range.update_base_column(offset, SCHEMA_ENCODING_COLUMN, schema_encoding)

                # Update TPS on the base page: we have now merged up to at least the current tail RID
                tail_rid = base_record[INDIRECTION_COLUMN]
                if tail_rid != 0:
                    page_index = offset // RECORDS_PER_PAGE
                    # Use any base column (RID column here) to store TPS
                    pid = page_range._page_id(False, RID_COLUMN, page_index)
                    page = self.bufferpool.fix_page(pid, mode="w")
                    current_tps = page.get_tps()
                    # since RIDs are monotonically increasing, TPS = max of merged tail RIDs
                    if tail_rid > current_tps:
                        page.set_tps(tail_rid)
                    self.bufferpool.unfix_page(pid, dirty=True)

        # Free tail pages after merge to reclaim disk/memory
        for page_range in self.page_ranges:
            if page_range.num_tail_records == 0:
                continue  # No tail pages to clean
            
            # Remove tail pages from bufferpool
            for col_idx in range(page_range.num_columns):
                num_tail_pages = page_range.num_tail_pages_per_col[col_idx]
                for page_idx in range(num_tail_pages):
                    pid = page_range._page_id(True, col_idx, page_idx)
                    
                    # Evict from bufferpool if present
                    if pid in self.bufferpool.frames:
                        # Flush if dirty before removing
                        self.bufferpool.flush(pid)
                        del self.bufferpool.frames[pid]
                        if pid in self.bufferpool.lru:
                            del self.bufferpool.lru[pid]
                    
                    # Delete file from disk
                    path = self.bufferpool.disk.page_path(self.name, True, col_idx, page_range.range_idx, page_idx)
                    if os.path.exists(path):
                        try:
                            os.remove(path)
                        except OSError:
                            pass  # Ignore errors if file is locked/missing
            
            # Reset tail record tracking for this page range
            page_range.num_tail_records = 0
            page_range.num_tail_pages_per_col = [1] * page_range.num_columns

    def merge(self):
        """Public method to trigger merge"""
        # Prevent concurrent merges
        with self._merge_lock:
            if self._merge_in_progress:
                return  # Merge already in progress, skip
            self._merge_in_progress = True
        
        try:
            self.__merge()
            # Reset counter after successful merge
            with self._merge_lock:
                self.updates_since_merge = 0
        finally:
            with self._merge_lock:
                self._merge_in_progress = False
    
    def _start_merge_thread(self):
        """Start the background merge thread"""
        if self._merge_thread is None or not self._merge_thread.is_alive():
            self._merge_thread_stop.clear()
            self._merge_thread = threading.Thread(target=self._merge_thread_worker, daemon=True)
            self._merge_thread.start()
    
    def _merge_thread_worker(self):
        """Background thread worker that periodically checks if merge is needed"""
        while not self._merge_thread_stop.is_set():
            # Check if merge is needed
            should_merge = False
            with self._merge_lock:
                if self.updates_since_merge >= MERGE_THRESHOLD_UPDATES and not self._merge_in_progress:
                    should_merge = True
            
            if should_merge:
                self.merge()
            
            # Sleep for the check interval, but wake up early if stop is signaled
            self._merge_thread_stop.wait(timeout=MERGE_CHECK_INTERVAL)
    
    def stop_merge_thread(self):
        """Stop the background merge thread when table/db is closing"""
        if self._merge_thread is not None:
            self._merge_thread_stop.set()
            self._merge_thread.join(timeout=5.0)  # Wait up to 5 seconds for thread to finish
            self._merge_thread = None

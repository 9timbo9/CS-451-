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

        self.lock = threading.RLock() # per page lock

    def has_capacity(self):
        """
        Just checking if there is space
        """
        with self.lock:
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
        with self.lock:
            offset = self.num_base_records

            for col_index, value in enumerate(record_data):
                page_index = offset // RECORDS_PER_PAGE
                
                # ensure we have enough pages for this column
                if page_index >= self.num_base_pages_per_col[col_index]:
                    self.num_base_pages_per_col[col_index] += 1

                pid = self._page_id(False, col_index, page_index)
                page = self.table.bufferpool.fix_page(pid, mode="w")
                if page.num_records == 0:
                    page.num_records = offset % RECORDS_PER_PAGE
                # appending: Page.write writes at page.num_records
                page.write(value)
                self.table.bufferpool.unfix_page(pid, dirty=True)

            self.num_base_records += 1
            return offset
    
    def read_base_record(self, offset):
        """
        Read a base record at given logical offset and return full [meta+user] list.
        """
        with self.lock:
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
        with self.lock:
            page_index = offset // RECORDS_PER_PAGE
            slot_in_page = offset % RECORDS_PER_PAGE

            pid = self._page_id(False, col_index, page_index)
            page = self.table.bufferpool.fix_page(pid, mode="w")
            if page.num_records == 0:
                last_page_idx = (self.num_base_records - 1) // RECORDS_PER_PAGE
                if page_index < last_page_idx:
                    page.num_records = RECORDS_PER_PAGE
                else:
                    count = self.num_base_records % RECORDS_PER_PAGE
                    page.num_records = RECORDS_PER_PAGE if count == 0 else count
            page.update(slot_in_page, value)
            self.table.bufferpool.unfix_page(pid, dirty=True)
    
    def write_tail_record(self, record_data):
        """
        Append a tail record (metadata+user columns).
        Returns the offset where the tail record was written.
        """
        with self.lock:
            offset = self.num_tail_records

            for col_index, value in enumerate(record_data):
                page_index = offset // RECORDS_PER_PAGE
                
                if page_index >= self.num_tail_pages_per_col[col_index]:
                    self.num_tail_pages_per_col[col_index] += 1

                pid = self._page_id(True, col_index, page_index)
                page = self.table.bufferpool.fix_page(pid, mode="w")

                if page.num_records == 0:
                    page.num_records = offset % RECORDS_PER_PAGE

                page.write(value)
                self.table.bufferpool.unfix_page(pid, dirty=True)

            self.num_tail_records += 1
            return offset

    def read_tail_record(self, offset):
        """
        Read a tail record at given offset and return full [meta+user] list.
        """
        with self.lock:
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

        self._rids_to_merge = set()
        self._rids_to_merge_lock = threading.Lock()

        # each shared data structure has a lock
        self.page_directory_lock = threading.RLock()
        self.page_ranges_lock = threading.RLock()
        self.rid_lock = threading.Lock()
        self.updates_counter_lock = threading.Lock()
        
        self.index_lock = threading.RLock()
        
        # Update-based merge tracking
        self.updates_since_merge = 0
        
        # Background merge thread
        self._merge_thread = None
        self._merge_thread_stop = threading.Event()  # signals thread to stop
        self._merge_in_progress = threading.Lock()  # prevents concurrent merges
        self._start_merge_thread()

        # Transaction support
        self.lock_manager = None
        self.transaction_id = None
        self._transaction_modifications = []  # Track changes for rollback
        self._transaction_modifications_lock = threading.Lock()

    def __str__(self):
        return f'Table(name="{self.name}", num_columns={self.num_columns}, key={self.key})'

    def __repr__(self):
        return self.__str__()

    def _get_or_create_page_range(self):
        """Get current page range or create a new one if the current is full."""
        with self.page_ranges_lock:
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

        with self.index_lock:
            # enforce unique primary key
            if self.index.locate(self.key, columns[self.key]):
                raise ValueError(
                    f"Duplicate entry for primary key column {self.key} with value {columns[self.key]}"
                )

            # allocate RID
            with self.rid_lock:
                rid = self.next_rid
                self.next_rid += 1

            indirection = 0  # no tail yet
            timestamp = int(time())
            schema_encoding = 0

            full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)

            # write the base record
            page_range = self._get_or_create_page_range()
            offset = page_range.write_base_record(full_record)

            # base record
            with self.page_directory_lock:
                self.page_directory[rid] = (page_range.range_idx, False, offset)

            # update indices while still holding index_lock to make the whole operation atomic w.r.t other inserts
            for col_num in range(self.num_columns):
                if self.index.indices[col_num] is not None:
                    value = columns[col_num]
                    self.index.insert(col_num, value, rid)

            # Record modification for potential rollback
            self.record_modification(rid, 'insert', new_data=full_record)

            return rid

    def read_record(self, rid):
        """
        Read a record (base or tail) by RID.
        Returns full [meta+user] list, or None if deleted/not present.
        """
        with self.page_directory_lock:
            loc = self.page_directory.get(rid)
        
        if loc is None:
            return None

        range_idx, is_tail, offset = loc
        
        with self.page_ranges_lock:
            page_range = self.page_ranges[range_idx]

        with page_range.lock: # lock for the current page range (not whole table)
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
            # if something went wrong, return base
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

        # Record old state before modification
        self.record_modification(rid, 'update', old_data=base_record.copy())

        latest_values, current_schema = self.get_latest_version(rid)

        with self.rid_lock:
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
        with self.page_ranges_lock:
            if (
                self.current_tail_page_range is None
                or self.current_tail_page_range.num_tail_records >= RECORDS_PER_PAGE * 16
            ):
                self.current_tail_page_range = self._get_or_create_page_range()
            page_range = self.current_tail_page_range

        offset = page_range.write_tail_record(tail_data)

        # record the tail in page directory
        with self.page_directory_lock:
            self.page_directory[tail_rid] = (page_range.range_idx, True, offset)
            base_range_idx, is_tail, base_offset = self.page_directory[rid]
        
        assert not is_tail
        
        with self.page_ranges_lock:
            base_pr = self.page_ranges[base_range_idx]

        with base_pr.lock:
            # update indirection and schema-encoding on base record
            base_pr.update_base_column(base_offset, INDIRECTION_COLUMN, tail_rid)
            base_pr.update_base_column(base_offset, SCHEMA_ENCODING_COLUMN, new_schema)

        # update all relevant secondary indexes
        with self.index_lock:
            for col_num, old_value, new_value in updated_columns_info:
                if self.index.indices[col_num] is not None:
                    self.index.update(col_num, old_value, new_value, rid)

        with self._rids_to_merge_lock:
            self._rids_to_merge.add(rid)

        # increment update counter for merge tracking
        with self.updates_counter_lock:
            self.updates_since_merge += 1

        return True

    def delete_record(self, rid):
        """
        Mark a record as deleted by setting RID to DELETED_RID in the base record
        and remove it from all indexes.
        """
        with self.page_directory_lock:
            loc = self.page_directory.get(rid)
        
        if loc is None:
            return False

        base_record = self.read_record(rid)
        if base_record is None:
            return False

        # Record old state before deletion
        self.record_modification(rid, 'delete', old_data=base_record.copy())

        range_idx, is_tail, offset = loc
        if is_tail:
            # we only logically delete via base record in this implementation
            return False

        # get latest version to ensure we have the correct values for index deletion
        latest_values, _ = self.get_latest_version(rid)
        if latest_values is None:
            return False

        with self.page_ranges_lock:
            pr = self.page_ranges[range_idx]
        
        with pr.lock:
            pr.update_base_column(offset, RID_COLUMN, self.DELETED_RID)

        # drop from indexes using latest values
        with self.index_lock:
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
        with self._rids_to_merge_lock:
            rids_to_merge = self._rids_to_merge.copy()
            self._rids_to_merge.clear()

        if not rids_to_merge:
            return

        with self.page_ranges_lock:
            # Capture current tail range at start to avoid cleaning it
            start_tail_range = self.current_tail_page_range
        # print("MERGE")

        merged_ranges = set()

        # Group RIDs by their page range
        rids_by_range = {}
        with self.page_directory_lock:
            for rid in rids_to_merge:
                loc = self.page_directory.get(rid)
                if loc is None:
                    continue
                range_idx, is_tail, offset = loc
                if is_tail:
                    continue  # skip tail records
                if range_idx not in rids_by_range:
                    rids_by_range[range_idx] = []
                rids_by_range[range_idx].append((rid, offset))

        with self.page_ranges_lock:
            page_ranges_list = list(self.page_ranges)

        for range_idx, rid_offset_list in rids_by_range.items():
            if range_idx >= len(page_ranges_list):
                continue
            page_range = page_ranges_list[range_idx]

            acquired = page_range.lock.acquire(blocking=False)
            if not acquired:
                # Put RIDs back for next merge cycle
                with self._rids_to_merge_lock:
                    for rid, _ in rid_offset_list:
                        self._rids_to_merge.add(rid)
                continue

            merged_ranges.add(page_range)

            try:
                for rid, offset in rid_offset_list:
                    page_index = offset // RECORDS_PER_PAGE

                    # Read base record directly (we already hold the lock)
                    base_record = page_range.read_base_record(offset)
                    if not base_record:
                        continue

                    base_rid = base_record[RID_COLUMN]
                    tail_rid = base_record[INDIRECTION_COLUMN]

                    if base_rid == self.DELETED_RID or base_rid == 0:
                        continue

                    if tail_rid == 0:
                        continue  # no tail to merge

                    # Check TPS
                    pid = page_range._page_id(False, RID_COLUMN, page_index)
                    page = self.bufferpool.fix_page(pid, mode="r")
                    current_tps = page.get_tps()
                    self.bufferpool.unfix_page(pid)

                    if tail_rid <= current_tps:
                        continue  # already merged

                    # Need to release lock before calling get_latest_version to avoid potential deadlock w/ other page ranges
                    page_range.lock.release()

                    latest_values, schema_encoding = self.get_latest_version(rid)

                    # Reacquire lock
                    page_range.lock.acquire()

                    if latest_values is None:
                        continue

                    # NOTE: We intentionally do NOT copy values into base record
                    # to preserve original values for historical version queries.
                    # The base record keeps its original values; latest values
                    # are always retrieved by following the tail chain.

                    # Update TPS
                    pid = page_range._page_id(False, RID_COLUMN, page_index)
                    page = self.bufferpool.fix_page(pid, mode="w")
                    current_tps = page.get_tps()
                    if tail_rid > current_tps:
                        page.set_tps(tail_rid)
                    self.bufferpool.unfix_page(pid, dirty=True)
            finally:
                page_range.lock.release()

        # NOTE: Tail records are intentionally NOT deleted after merge
        # to preserve historical versions for select_version queries.
        # The merge only consolidates values into base records for faster reads.

    def merge(self):
        """Public method to trigger merge"""
        acquired = self._merge_in_progress.acquire(blocking=False)
        if not acquired:
            return
        
        try:
            self.__merge()
            with self.updates_counter_lock:
                self.updates_since_merge = 0
        finally:
            self._merge_in_progress.release()
    
    def _start_merge_thread(self):
        """Start the background merge thread"""
        if self._merge_thread is None or not self._merge_thread.is_alive():
            self._merge_thread_stop.clear()
            self._merge_thread = threading.Thread(target=self._merge_thread_worker, daemon=True)
            self._merge_thread.start()
    
    def _merge_thread_worker(self):
        """Background thread worker that periodically checks if merge is needed"""
        while not self._merge_thread_stop.is_set():
            # check if merge is needed
            should_merge = False
            with self.updates_counter_lock:
                if self.updates_since_merge >= MERGE_THRESHOLD_UPDATES:
                    should_merge = True
            
            if should_merge:
                self.merge()
            
            # sleep for the check interval but wake up early if stop is signaled
            self._merge_thread_stop.wait(timeout=MERGE_CHECK_INTERVAL)
    
    def stop_merge_thread(self):
        """Stop the background merge thread when table/db is closing"""
        if self._merge_thread is not None:
            self._merge_thread_stop.set()
            self._merge_thread.join(timeout=5.0) # wait to finish
            self._merge_thread = None

    def record_modification(self, rid, modification_type, old_data=None, new_data=None):
        """Record a modification for potential rollback"""
        with self._transaction_modifications_lock:
            self._transaction_modifications.append({
                'transaction_id': self.transaction_id,
                'rid': rid,
                'type': modification_type,  # 'insert', 'update', 'delete'
                'old_data': old_data,
                'new_data': new_data
            })
    
    def rollback_modifications(self):
        """Rollback all recorded modifications"""
        current_transaction_id = self.transaction_id

        with self._transaction_modifications_lock:
            # Filter to only add this transaction's modifications
            modifications = [m for m in self._transaction_modifications if m['transaction_id'] == current_transaction_id]
            self._transaction_modifications = [m for m in self._transaction_modifications if m['transaction_id'] != current_transaction_id]
        
        # Rollback in reverse order
        for mod in reversed(modifications):
            try:
                if mod['type'] == 'insert':
                    # Delete the inserted record from indexes and page_directory
                    rid = mod['rid']
                    with self.page_directory_lock:
                        loc = self.page_directory.get(rid)
                    
                    if loc:
                        range_idx, is_tail, offset = loc

                        with self.page_ranges_lock:
                            page_range = self.page_ranges[range_idx]
                        with page_range.lock:
                            page_range.update_base_column(offset, RID_COLUMN, self.DELETED_RID)
                        
                        # Remove from all indexes
                        try:
                            latest_values, _ = self.get_latest_version(rid)
                            if latest_values:
                                for col_num in range(self.num_columns):
                                    if self.index.indices[col_num] is not None:
                                        value = latest_values[col_num]
                                        self.index.delete(col_num, value, rid)
                        except Exception:
                            pass
                        
                        # Remove from page_directory
                        with self.page_directory_lock:
                            if rid in self.page_directory:
                                del self.page_directory[rid]
                
                elif mod['type'] == 'update':
                    # Restore old data
                    rid = mod['rid']
                    old_data = mod['old_data']
                    if old_data:
                        with self.page_directory_lock:
                            loc = self.page_directory.get(rid)
                        if loc:
                            range_idx, is_tail, offset = loc
                            with self.page_ranges_lock:
                                page_range = self.page_ranges[range_idx]
                            
                            # Restore base record columns (skip metadata)
                            with page_range.lock:
                                for col_idx in range(4, len(old_data)):
                                    page_range.update_base_column(offset, col_idx, old_data[col_idx])
                                
                                # Restore indirection and schema encoding
                                page_range.update_base_column(offset, INDIRECTION_COLUMN, old_data[INDIRECTION_COLUMN])
                                page_range.update_base_column(offset, SCHEMA_ENCODING_COLUMN, old_data[SCHEMA_ENCODING_COLUMN])
                        try:
                            current_values, _ = self.get_latest_version(rid)
                            if current_values and old_data and len(old_data) > 4:
                                old_user_columns = old_data[4:]
                                for col_num in range(self.num_columns):
                                    if self.index.indices[col_num] is not None:
                                        old_value = old_user_columns[col_num]
                                        new_value = current_values[col_num]
                                        if old_value != new_value:
                                            self.index.update(col_num, new_value, old_value, rid)
                        except Exception:
                            pass
                
                elif mod['type'] == 'delete':
                    # Restore deleted record by clearing DELETED_RID marker
                    rid = mod['rid']
                    old_data = mod['old_data']
                    
                    with self.page_directory_lock:
                        loc = self.page_directory.get(rid)
                    if loc:
                        range_idx, is_tail, offset = loc
                        if not is_tail:
                            with self.page_ranges_lock:
                                page_range = self.page_ranges[range_idx]
                            with page_range.lock:
                                # Restore RID column to original rid
                                page_range.update_base_column(offset, RID_COLUMN, old_data[RID_COLUMN])
                            
                            # Re-add to indexes with original values
                            try:
                                if old_data and len(old_data) > 4:
                                    user_columns = old_data[4:]
                                    for col_num in range(self.num_columns):
                                        if self.index.indices[col_num] is not None:
                                            value = user_columns[col_num]
                                            self.index.insert(col_num, value, rid)
                            except Exception:
                                pass
            except Exception as e:
                print(f"Error during rollback: {e}")

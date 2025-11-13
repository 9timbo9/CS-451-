from lstore.index import Index
from lstore.page import Page
from lstore.config import *
from time import time


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
    def __init__(self, num_columns):
        self.num_columns = num_columns  # includes metadata columns
        self.base_pages = [[] for _ in range(num_columns)]
        self.tail_pages = [[] for _ in range(num_columns)]
        
        # init
        for i in range(num_columns):
            self.base_pages[i].append(Page())
            self.tail_pages[i].append(Page())
        
        self.num_base_records = 0
        self.num_tail_records = 0
        self.max_records = 512 * 16  # 512 records per page times 16 pages
    
    def has_capacity(self):
        """
        Just checking if there is space
        """
        return self.num_base_records < self.max_records
    
    def write_base_record(self, record_data):
        """
        Write a record to base pages (one value per column)
        Returns the offset where the record was written
        """
        offset = self.num_base_records
        
        for col_index, value in enumerate(record_data):
            # get current page for this column
            page_list = self.base_pages[col_index]
            current_page = page_list[-1]
            
            if not current_page.has_capacity():
                new_page = Page()
                page_list.append(new_page)
                current_page = new_page
            
            current_page.write(value)
        
        self.num_base_records += 1
        return offset
    
    def write_tail_record(self, record_data):
        """
        Write a record to tail pages (for updates)
        Returns the offset where the record was written
        """
        offset = self.num_tail_records
        
        for col_index, value in enumerate(record_data):
            page_list = self.tail_pages[col_index]
            current_page = page_list[-1]
            
            if not current_page.has_capacity():
                new_page = Page()
                page_list.append(new_page)
                current_page = new_page
            
            current_page.write(value)
        
        self.num_tail_records += 1
        return offset
    
    def read_base_record(self, offset):
        """Read a base record at given offset"""
        record_data = []
        for col_index in range(self.num_columns):
            # calculate which page and slot within that page
            # print(offset)
            # print(offset//512)
            # print(offset%512)
            page_index = offset // 512  # do ranges even need to have a cap
            slot_in_page = offset % 512
            
            page = self.base_pages[col_index][page_index]
            value = page.read(slot_in_page)
            record_data.append(value)
        
        return record_data
    
    def read_tail_record(self, offset):
        """Read a tail record at given offset"""
        record_data = []
        for col_index in range(self.num_columns):
            page_index = offset // 512
            slot_in_page = offset % 512
            
            page = self.tail_pages[col_index][page_index]
            value = page.read(slot_in_page)
            record_data.append(value)
        
        return record_data
    
    def update_base_column(self, offset, col_index, value):
        """Update a specific column in a base record"""
        page_index = offset // 512
        slot_in_page = offset % 512
        
        page = self.base_pages[col_index][page_index]
        # overwrite the value at the specific slot
        page_offset = slot_in_page * 8
        page.data[page_offset:page_offset+8] = value.to_bytes(8, byteorder='little', signed=(value < 0))


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
        self.bufferpool = bufferpool  # TODO: bufferpool integration
        self.index = Index(self, create_index)

    def __str__(self):
        return f'Table(name="{self.name}", num_columns={self.num_columns}, key={self.key})'

    def __repr__(self):
        return self.__str__()

    def _get_or_create_page_range(self):
        """Get current page range or create new one if full"""  # CHANGE !!!
        if self.current_page_range is None or not self.current_page_range.has_capacity():
            self.current_page_range = PageRange(self.total_columns)
            self.page_ranges.append(self.current_page_range)
        return self.current_page_range
    
    def insert(self, *columns):
        """
        Insert a new base record and returns the RID of the inserted record
        """
        if len(columns) != self.num_columns:
            raise ValueError(f"Expected {self.num_columns} columns, got {len(columns)}")

        if self.index.locate(self.key, columns[self.key]):
            raise ValueError(f"Duplicate entry for primary key column {self.key} with value {columns[self.key]}")
        
        rid = self.next_rid
        self.next_rid += 1
        
        indirection = 0  # no tail records
        timestamp = int(time())
        schema_encoding = 0
        
        # metadata and appended actual columns
        full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)
        
        page_range = self._get_or_create_page_range()
        offset = page_range.write_base_record(full_record)
        
        self.page_directory[rid] = (page_range.base_pages, offset)

        for col_num in range(self.num_columns):
            if self.index.indices[col_num] is not None:
                value = columns[col_num]
                self.index.insert(col_num, value, rid)
        
        return rid
    
    def read_record(self, rid):
        """
        Read a record by a given RID
        Returns the record data as a list or None if its deleted
        """
        if rid not in self.page_directory:
            return None
        
        pages, offset = self.page_directory[rid]
        
        record_data = []
        for col_index in range(self.total_columns):
            page_index = offset // 512
            slot_in_page = offset % 512
            
            page = pages[col_index][page_index]
            value = page.read(slot_in_page)
            record_data.append(value)
        
        if record_data[RID_COLUMN] == self.DELETED_RID:
            return None
        
        return record_data
    
    def get_latest_version(self, rid):
        """
        Get the latest version of a record by following the indirection chain
        Returns tuple: (record_data, schema_encoding)
        record_data contains only the actual columns (not metadata)
        """
        base_record = self.read_record(rid)  # if it exists
        if base_record is None:
            return None, None
        
        # if there is only a base record (doesnt happen i think)
        if base_record[INDIRECTION_COLUMN] == 0:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]
        
        tail_rid = base_record[INDIRECTION_COLUMN]
        tail_record = self.read_record(tail_rid)
        
        if tail_record is None:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]
        
        return tail_record[4:], tail_record[SCHEMA_ENCODING_COLUMN]
    
    def update_record(self, rid, *columns): 
        # EXAMPLE TO USE UPDATE: update_record(1, None, 200, None)
        # update RID 1, keep col 0, change col 1 to 200, keep col 2
        """
        Update a record by creating a new tail record
        columns: list where None means no change for that column
        """

        base_record = self.read_record(rid)
        if base_record is None:
            return False
        
        latest_values, current_schema = self.get_latest_version(rid)
        
        tail_rid = self.next_rid
        self.next_rid += 1
        
        # get the previous tail
        prev_tail_rid = base_record[INDIRECTION_COLUMN]
        
        new_schema = current_schema
        updated_columns_info = []  # track which columns changed for index updates
        
        for i, value in enumerate(columns):
            if value is not None:
                new_schema |= (1 << i)  # weird bit stuff to set bit i to 1
                # print(new_schema)
                updated_columns_info.append((i, latest_values[i], value))
        
        tail_data = [prev_tail_rid, tail_rid, int(time()), new_schema]  # tail metadata
        
        for i in range(self.num_columns):
            # print(columns[i])
            if columns[i] is not None: # if column is none then keep old version
                tail_data.append(columns[i])
            else:
                tail_data.append(latest_values[i])
        
        if self.current_tail_page_range is None or self.current_tail_page_range.num_tail_records >= 512 * 16:
            self.current_tail_page_range = self._get_or_create_page_range()
        
        page_range = self.current_tail_page_range
        offset = page_range.write_tail_record(tail_data)
        
        self.page_directory[tail_rid] = (page_range.tail_pages, offset)
        
        base_pages, base_offset = self.page_directory[rid]
        page_index = base_offset // 512
        slot_in_page = base_offset % 512
        
        # update in base record
        indirection_page = base_pages[INDIRECTION_COLUMN][page_index]
        page_offset = slot_in_page * 8
        indirection_page.data[page_offset:page_offset+8] = tail_rid.to_bytes(8, byteorder='little')
        
        schema_page = base_pages[SCHEMA_ENCODING_COLUMN][page_index]
        schema_page.data[page_offset:page_offset+8] = new_schema.to_bytes(8, byteorder='little')
        
        for col_num, old_value, new_value in updated_columns_info:
            if self.index.indices[col_num] is not None:
                self.index.update(col_num, old_value, new_value, rid)
        
        return True
    
    def delete_record(self, rid):
        """
        Mark a record as deleted by setting RID to DELETED_RID (not actually removing the data yet)
        Also update indexes to remove the deleted record (only the index)
        """
        if rid not in self.page_directory:
            return False
        base_record = self.read_record(rid)
        if base_record is None:
            return False
        
        user_columns = base_record[4:]
        pages, offset = self.page_directory[rid]
        
        page_index = offset // 512
        slot_in_page = offset % 512
        rid_page = pages[RID_COLUMN][page_index]
        page_offset = slot_in_page * 8
        rid_page.data[page_offset:page_offset+8] = self.DELETED_RID.to_bytes(8, byteorder='little')
        
        for col_num in range(self.num_columns):
            if self.index.indices[col_num] is not None:
                value = user_columns[col_num]
                self.index.delete(col_num, value, rid)
        
        return True
    
    def get_version(self, rid, relative_version):
        """
        Get a specific version of a record (version is a negative number like -2 for 2 versions ago)
        Returns tuple: (record_data, schema_encoding)
        record_data contains only the user columns (not metadata)
        """
        base_record = self.read_record(rid)
        if base_record is None:
            return None, None
        if relative_version == 0:  # 0 steps back
            return self.get_latest_version(rid)
        
        # tails rid should be in the indirection column of the base record
        tail_rid = base_record[INDIRECTION_COLUMN]
        
        if tail_rid == 0:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]
        
        steps = abs(relative_version)  # number of steps backwards (versions)
        curr_tail_rid = tail_rid
        
        for i in range(steps):
            if curr_tail_rid == 0:  # same as above
                break
            tail_record = self.read_record(curr_tail_rid)
            # print(tail_record)
            if tail_record is None:
                return None, None
            curr_tail_rid = tail_record[INDIRECTION_COLUMN]
        
        # if that number of steps brings you back to the base record
        if curr_tail_rid == 0:
            return base_record[4:], base_record[SCHEMA_ENCODING_COLUMN]
        else:
            version_record = self.read_record(curr_tail_rid)
            if version_record is None:
                return None, None
            return version_record[4:], version_record[SCHEMA_ENCODING_COLUMN]

    def __merge(self):
        print("merge is happening")  # ASSIGNMENT 2!!!
        pass

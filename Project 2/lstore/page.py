from lstore.config import RECORDS_PER_PAGE

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.dirty = False
        self.pin_count = 0
        # TPS is stored in the first 8 bytes of data, load it when page is created
        self._load_tps()

    def _load_tps(self):
        """Load TPS value from the first 8 bytes of data"""
        # TPS is stored in bytes 0-7
        pass  # TPS will be read on-demand from data
    
    def has_capacity(self):
        # Page structure: 8 bytes for TPS + (511 records * 8 bytes each) = 4096 bytes
        # print("checking capacity")
        if RECORDS_PER_PAGE*8 + 8 > 4096:
            raise RuntimeError("RECORDS_PER_PAGE is too large for the set page size.")
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        # print("writing value:", value)
        if self.has_capacity():
            # Records start at byte 8 (first 8 bytes reserved for TPS)
            offset = 8 + (self.num_records * 8)
            self.data[offset:offset+8] = value.to_bytes(8, byteorder='little')
            self.num_records += 1
            self.set_dirty()
        else:
            # print("Page is full, cannot write more records.")
            return
    
    def read(self, slot_number):
        # print("reading slot number:", slot_number)
        # Records start at byte 8 (first 8 bytes reserved for TPS)
        offset = 8 + (slot_number * 8)
        value = int.from_bytes(self.data[offset:offset+8], byteorder='little')
        return value
    
    def set_dirty(self):
        """Set page as dirty/modified (meaning it needs to be written to disk)"""
        self.dirty = True
    
    def set_not_dirty(self):
        """Clear dirty flag after writing to disk"""
        self.dirty = False
    
    def is_dirty(self):
        """Check if page has been modified"""
        return self.dirty
    
    def pin(self):
        """Increment pin count when transaction accesses page"""
        self.pin_count += 1
    
    def unpin(self):
        """Decrement pin count when transaction finishes with page"""
        if self.pin_count > 0:
            self.pin_count -= 1
    
    def is_pinned(self):
        """Check if page can be evicted (pin_count must be 0)"""
        return self.pin_count > 0
    
    def get_tps(self):
        """Get TPS Number for merge tracking - stored in first 8 bytes of data"""
        return int.from_bytes(self.data[0:8], byteorder='little')
    
    def set_tps(self, tps_value):
        """Update TPS after merge completion - stored in first 8 bytes of data"""
        self.data[0:8] = tps_value.to_bytes(8, byteorder='little')
        self.set_dirty()
    
    def update(self, slot_number, value):
        """Update existing record at slot_number"""
        if slot_number < self.num_records:
            # Records start at byte 8 (first 8 bytes reserved for TPS)
            offset = 8 + (slot_number * 8)
            self.data[offset:offset+8] = value.to_bytes(8, byteorder='little')
            self.set_dirty()
            return True
        return False
    

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.dirty = False
        self.pin_count = 0
        self.tps = 2**64 - 1

    def has_capacity(self):
        # each record is 64 bits = 8 bytes (one column of one record = one value)
        # 4096 bytes / 8 bytes per record = 512 records
        # print("checking capacity")
        return self.num_records < 512

    def write(self, value):
        # print("writing value:", value)
        if self.has_capacity():
            # get the location to write the new record
            offset = (self.num_records) * 8
            self.data[offset:offset+8] = value.to_bytes(8, byteorder='little')
            self.num_records += 1
            self.mark_dirty()
        else:
            # print("Page is full, cannot write more records.")
            return
    
    def read(self, slot_number):
        # print("reading slot number:", slot_number)
        offset = slot_number * 8
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
        """Get TPS Number for merge tracking"""
        return self.tps
    
    def set_tps(self, tps_value):
        """Update TPS after merge completion"""
        self.tps = tps_value
        self.mark_dirty()
    
    def update(self, slot_number, value):
        """Update existing record at slot_number"""
        if slot_number < self.num_records:
            offset = slot_number * 8
            self.data[offset:offset+8] = value.to_bytes(8, byteorder='little')
            self.mark_dirty()
            return True
        return False
    

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # each record is 64 bits = 8 bytes
        # 4096 bytes / 8 bytes per record = 512 records
        print("checking capacity")
        return self.num_records < 512

    def write(self, value):
        print("writing value:", value)
        if self.has_capacity():
            # get the location to write the new record
            offset = (self.num_records) * 8
            # then write the value as 8 bytes and add to page
            self.data[offset:offset+8] = value.to_bytes(8, byteorder='little')
            self.num_records += 1
        else:
            print("Page is full, cannot write more records.")
    
    def read(self, slot_number):
        print("reading slot number:", slot_number)
        offset = slot_number * 8
        # read 8 bytes and convert back to integer
        value = int.from_bytes(self.data[offset:offset+8], byteorder='little')
        return value
        


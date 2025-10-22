
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        print("checking capacity")
        pass

    def write(self, value):
        print("writing value:", value)
        self.num_records += 1
        pass
    
    def read(self, slot_number):
        print("reading slot number:", slot_number)
        pass


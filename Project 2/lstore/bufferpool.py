from collections import OrderedDict
from lstore.page import Page


class Bufferpool:

    def __init__(self, disk_manager, capacity):
        self.disk = disk_manager
        self.capacity = capacity

        self.frames = {}  # page_id = {"page": Page, "pin": int, "dirty": bool}

        # Imma go with a LRU, 
        self.lru = OrderedDict()  # ordereddict LRU solution

    def fix_page(self, page_id, mode="r"):
        """
        Get a Page for a given page_id, load if not in memory,
        pin it, and return the Page object.
        mode is "r" or "w" (for now just semantic).
        """
        # already in buffer
        if page_id in self.frames:
            frame = self.frames[page_id]
            frame["pin"] += 1
            # refresh LRU
            self.lru.move_to_end(page_id)
            return frame["page"]

        # need to load; maybe evict if full
        if len(self.frames) >= self.capacity:
            self.evict()

        # load from disk
        table, is_tail, col, rng, idx = page_id
        raw = self.disk.read_page(table, is_tail, col, rng, idx)

        page = Page()            # create empty page
        page.data[:] = raw     

        self.frames[page_id] = {"page": page, "pin": 1, "dirty": False}
        self.lru[page_id] = True
        return page

    def unfix_page(self, page_id, dirty=False):
        """
        Unpin the page (transaction done using it).
        Mark dirty if the caller wrote to it.
        """
        frame = self.frames.get(page_id)
        if frame is None:
            return

        if frame["pin"] > 0:
            frame["pin"] -= 1

        if dirty:
            frame["dirty"] = True

    def flush(self, page_id):
        frame = self.frames.get(page_id)
        if not frame:
            return
        if not frame["dirty"]:
            return

        table, is_tail, col, rng, idx = page_id
        self.disk.write_page(table, is_tail, col, rng, idx, frame["page"].data)  # update new stuff writes on disk
        frame["dirty"] = False  # clean now

    def flush_all(self):  # call this at close() in db to
        for pid in list(self.frames.keys()):
            self.flush(pid)

    def evict(self):
        for victim_id in list(self.lru.keys()):
            frame = self.frames[victim_id]
            if frame["pin"] == 0:
                # flush if dirty, then evict
                if frame["dirty"]:
                    table, is_tail, col, rng, idx = victim_id # update in the disk
                    self.disk.write_page(table, is_tail, col, rng, idx, frame["page"].data)
                # remove from structures
                del self.frames[victim_id]
                del self.lru[victim_id]
                return

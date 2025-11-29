from lstore.config import PAGE_SIZE
import json
import os


class DiskManager:
    def __init__(self, root):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)  # root
        os.makedirs(os.path.join(self.root, "tables"), exist_ok=True)  # directory per table

    def table_dir(self, table_name):
        return os.path.join(self.root, "tables", table_name)  # path to a table

    def page_path(self, table, is_tail, col, rng, idx):
        kind = "tail" if is_tail else "base"
        d = self.table_dir(table)

        # Make sure the directory exists
        if not os.path.isdir(d):
            try:
                os.makedirs(d, exist_ok=True)
            except FileExistsError:
                if not os.path.isdir(d):
                    raise RuntimeError(
                        f"Path {d} exists and is not a directory. "
                        f"Delete it (and its parent 'default_db') and rerun."
                    )

        return os.path.join(d, f"{kind}_{col}_{rng}_{idx}.bin")

    def read_page(self, table, is_tail, col, rng, idx):
        path = self.page_path(table, is_tail, col, rng, idx)  # reads from a disk
        if not os.path.exists(path):  # for bufferpool
            return bytearray(PAGE_SIZE)
        with open(path, "rb") as f:
            data = f.read()
        buf = bytearray(PAGE_SIZE)
        buf[:len(data)] = data[:PAGE_SIZE]
        return buf

    def write_page(self, table, is_tail, col, rng, idx, buf):  # writes onto disk
        path = self.page_path(table, is_tail, col, rng, idx)  # used for dirty pages, for example
        with open(path, "wb") as f:
            f.write(bytes(buf[:PAGE_SIZE]))

    def meta_path(self, table_name):
        return os.path.join(self.table_dir(table_name), "meta.json")  # return file that holds meta data

    def write_meta(self, table_name, meta: dict):
        os.makedirs(self.table_dir(table_name), exist_ok=True)
        with open(self.meta_path(table_name), "w") as f:
            json.dump(meta, f)  # writing to meta if it exists

    def read_meta(self, table_name):  # reading from correct meta page
        path = self.meta_path(table_name)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f)

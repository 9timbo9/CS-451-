class IndexNode:
    """
    A node in a doubly linked list used by the index. Holds a value, a set of RIDs for that
    value, and pointers to the next and previous nodes in sorted order.
    """
    def __init__(self, value):
        self.value = value
        self.rids = set()
        self.next = None
        self.prev = None

    def __str__(self):
        return f"IndexNode(value={self.value}, rids={self.rids})"

    def __repr__(self):
        return self.__str__()


class Index:
    """
    A data structure holding indices for various columns of a table. Key column should be indexed by default,
    other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be
    used as well.
    """

    def __init__(self, table, create_index):
        self.table = table
        self.indices = [None] * table.num_columns  # One index for each table. All are empty initially.
        # Each column has an index structure: tuple(idx_map: dict, head: IndexNode, tail: indexNode) or None if no index
        if create_index:
            self.create_index(table.key)  # Key column should be indexed by default

    def create_index(self, column_number):
        """
        Create index on specific column.
        Rebuild the index using only latest version of base records
        """
        # If index already exists, do nothing
        if self.indices[column_number] is not None:
            return

        idx_map = {}
        head = None
        tail = None

        # Scan page_directory: rid -> (range_idx, is_tail, offset)
        for rid, (range_idx, is_tail, offset) in self.table.page_directory.items():
            if is_tail:  # skip tail records
                continue

            latest_values, _ = self.table.get_latest_version(rid)
            if latest_values is None:
                continue  # deleted or inaccessible

            value = latest_values[column_number]
            if value not in idx_map:
                idx_map[value] = set()
            idx_map[value].add(rid)

        # Build doubly-linked list sorted by value
        last_node = None
        for key in sorted(idx_map.keys()):
            node = IndexNode(key)
            node.rids.update(idx_map[key])
            idx_map[key] = node

            if last_node is None:
                head = node
            else:
                last_node.next = node
                node.prev = last_node
            last_node = node

        tail = last_node
        self.indices[column_number] = (idx_map, head, tail)

    def drop_index(self, column_number):
        """
        # optional: Drop indexing of a specific column that is not the primary key
        """
        if column_number == self.table.key:
            return  # cannot drop index on key column
        self.indices[column_number] = None  # reset the index to None

    def locate(self, column, value):
        """
        returns the location of all records with the given value on column "column"
        """
        column_index = self.indices[column]
        # If index exists, use it
        if column_index is not None:
            idx_map, head, tail = column_index
            node = idx_map.get(value)
            if node:
                return node.rids.copy()
            return set()

        # scan page_directory if no index
        result = set()
        for rid, (range_idx, is_tail, offset) in self.table.page_directory.items():
            # only consider base logical records
            if is_tail:
                continue
            latest_values, _ = self.table.get_latest_version(rid)
            if latest_values is None:
                continue
            if latest_values[column] == value:
                result.add(rid)
        return result

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end" (inclusive)
        """
        column_index = self.indices[column]
        result = set()
        # If no index, fall back to scanning page_directory
        if column_index is None:
            for rid, (range_idx, is_tail, offset) in self.table.page_directory.items():
                if is_tail:
                    continue
                latest_values, _ = self.table.get_latest_version(rid)
                if latest_values is None:
                    continue
                val = latest_values[column]
                if begin <= val <= end:
                    result.add(rid)
            return result

        idx_map, head, tail = column_index
        cur_node = head
        while cur_node and cur_node.value < begin:
            cur_node = cur_node.next
        while cur_node and cur_node.value <= end:
            result.update(cur_node.rids.copy())
            cur_node = cur_node.next
        return result

    def insert(self, column, value, rid):
        """
        Inserts a value into the index for the specified column.
        Should be called whenever a new record is inserted into the table.
        """
        column_index = self.indices[column]
        if column_index is None:
            return  # index does not exist for this column
        idx_map, head, tail = column_index
        if value in idx_map:  # Value already exists in index
            node = idx_map[value]
            node.rids.add(rid)
            return
        # New value needs to be inserted
        node = IndexNode(value)
        node.rids.add(rid)
        idx_map[value] = node
        # Insert into LL in sorted order
        if head is None:  # empty list
            head = tail = node
        elif value < head.value:  # insert at head
            node.next = head
            head.prev = node
            head = node
        elif value > tail.value:  # insert at tail
            tail.next = node
            node.prev = tail
            tail = node
        else:  # insert in middle
            # TODO: optimize by binary search
            cur_node = head
            while cur_node.value < value:
                cur_node = cur_node.next
            # Insert before cur_node
            prev_node = cur_node.prev
            prev_node.next = node
            node.prev = prev_node
            node.next = cur_node
            cur_node.prev = node
        self.indices[column] = (idx_map, head, tail)  # update index

    def delete(self, column, value, rid):
        """
        Deletes a record's RID from the index for the specified column.
        Should be called whenever a record is deleted from the table.
        """
        column_index = self.indices[column]
        if column_index is None:
            return  # index does not exist for this column
        idx_map, head, tail = column_index
        node_to_update = idx_map.get(value)
        if not node_to_update:
            return  # value not found in index
        node_to_update.rids.discard(rid)
        if not node_to_update.rids:  # If node is not empty, remove it
            if node_to_update.prev:  # unlink from list
                node_to_update.prev.next = node_to_update.next
            else:  # it was the head
                head = node_to_update.next
            if node_to_update.next:
                node_to_update.next.prev = node_to_update.prev
            else:  # it was the tail
                tail = node_to_update.prev
            del idx_map[value]  # remove from map
            self.indices[column] = (idx_map, head, tail)  # update index

    def update(self, column, old_value, new_value, rid):
        """
        Updates a record's value in the index for the specified column.
        Should be called whenever a record is updated in the table.
        """
        self.delete(column, old_value, rid)
        self.insert(column, new_value, rid)

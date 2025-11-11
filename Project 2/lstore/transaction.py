from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self._aborted = False

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            try:
                result = query(*args)
            # If the query has failed the transaction should abort
            except Exception:
                return self.abort()
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        """
        Abort transaction, for milestone 1 just signaling failure
        """
        self._aborted = True
        return False

    
    def commit(self):
        """
        Commit transaction, nothing to flush for m1 so return true unless already aborted
        """
        if self._aborted:
            return False
        return True


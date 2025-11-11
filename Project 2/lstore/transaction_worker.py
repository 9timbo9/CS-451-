from lstore.table import Table, Record
from lstore.index import Index
from threading import Thread

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        # avoid shared list across instances
        self.transactions = list(transactions) if transactions else []
        self.result = 0
        self._thread = None

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = Thread(target=self.__run, daemon=True)
            self._thread.start()
        # here you need to create a thread and call __run
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self._thread is not None:
            self._thread.join()


    def __run(self):
        for transaction in self.transactions:
            try:
                ok = transaction.run()
            # each transaction returns True if committed or False if aborted
            except Exception:
                ok = False
            self.stats.append(ok)
        # stores the number of transactions that committed
        self.result = sum(1 for ok in self.stats if ok)


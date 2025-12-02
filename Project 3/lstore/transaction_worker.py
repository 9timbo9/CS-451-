from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import MAX_RETRIES
from threading import Thread


class TransactionWorker:

    """
    Creates a transaction worker object.
    """
    def __init__(self, transactions=None, db=None):
        self.stats = []
        self.transactions = list(transactions) if transactions else []
        self.result = 0
        self._thread = None
        self.db = db

    def add_transaction(self, t):
        """
        Appends t to transactions
        """
        self.transactions.append(t)

    def run(self):
        """
        Runs all transaction as a thread
        """
        if self._thread is None or not self._thread.is_alive():
            self._thread = Thread(target=self.__run, daemon=True)
            self._thread.start()
        # here you need to create a thread and call __run

    def join(self):
        """
        Waits for the worker to finish
        """
        if self._thread is not None:
            self._thread.join()

    def __run(self):
            for transaction in self.transactions:
                committed = False
                attempts = 0
                max_attempts = 10  # Prevent infinite loops
                
                # Keep retrying until transaction commits or max attempts reached
                while not committed and attempts < max_attempts:
                    try:
                        committed = transaction.run()
                        attempts += 1
                    except Exception as e:
                        committed = False
                        print(f"Transaction error: {e}")
                        attempts += 1
                
                self.stats.append(committed)
            
            self.result = sum(1 for ok in self.stats if ok)


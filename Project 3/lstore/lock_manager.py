import threading
from enum import Enum


class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2


class LockManager:
    def __init__(self):
        self.locks = {} 
        self.lock_table = {}  
        self.lock = threading.RLock()
    
    def acquire_lock(self, transaction_id, record_id, lock_type):
        """
        Acquire lock for a transaction.
        Returns True if successful, False if would cause conflict (abort transaction)
        """
        with self.lock:
            if record_id not in self.lock_table:
                self.lock_table[record_id] = Lock(record_id)
            
            lock = self.lock_table[record_id]
            return lock.acquire(transaction_id, lock_type)
    
    def release_locks(self, transaction_id):
        with self.lock:
            for lock in self.lock_table.values():
                lock.release(transaction_id)


class Lock:
    def __init__(self, record_id):
        self.record_id = record_id
        self.lock_holders = set()  # transactions holding shared locks
        self.exclusive_holder = None  # transaction holding exclusive lock
        self.waiting_queue = []  # transactions waiting for lock
        self.lock = threading.RLock()
    
    def acquire(self, transaction_id, lock_type):
        with self.lock:
            # If transaction already holds a lock, upgrade if needed
            if transaction_id in self.lock_holders:
                if lock_type == LockType.EXCLUSIVE:
                    # Upgrade from shared to exclusive
                    if len(self.lock_holders) == 1 and self.exclusive_holder is None:
                        self.lock_holders.remove(transaction_id)
                        self.exclusive_holder = transaction_id
                        return True
                    else:
                        return False  # Cannot upgrade, conflict
                return True  # Already has appropriate lock
            
            if transaction_id == self.exclusive_holder:
                return True  # Already holds exclusive lock
            
            # Check for conflicts
            if lock_type == LockType.SHARED:
                if self.exclusive_holder is None:
                    self.lock_holders.add(transaction_id)
                    return True
                else:
                    return False  # Conflict with exclusive lock
            
            elif lock_type == LockType.EXCLUSIVE:
                if self.exclusive_holder is None and len(self.lock_holders) == 0:
                    self.exclusive_holder = transaction_id
                    return True
                else:
                    return False  # Conflict with existing locks
            
            return False
    
    def release(self, transaction_id):
        with self.lock:
            if transaction_id in self.lock_holders:
                self.lock_holders.remove(transaction_id)
            elif transaction_id == self.exclusive_holder:
                self.exclusive_holder = None

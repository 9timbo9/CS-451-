INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

DELETED_RID = 0  # if rid is 0 then it is deleted

PAGE_SIZE = 4096  # Fixed page size
RECORDS_PER_PAGE = 511  # 8 bytes for TPS + (511 * 8 bytes) = 4096 bytes total
PAGES_PER_RANGE = 16  # Number of pages in a range
BUFFERPOOL_CAPACITY = 8192

MERGE_THRESHOLD_UPDATES = 100  # trigger merge after this many updates
MERGE_CHECK_INTERVAL = 0.05

MAX_RETRIES = 100  # Maximum retry attempts
RETRY_DELAY = 0.01  # Initial delay in seconds (10ms)
RETRY_BACKOFF_MULTIPLIER = 1.5
MAX_RETRY_DELAY = 1.0

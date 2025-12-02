INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

PAGE_SIZE = 4096  # Fixed page size
RECORDS_PER_PAGE = 511  # 8 bytes for TPS + (511 * 8 bytes) = 4096 bytes total
BUFFERPOOL_CAPACITY = 8192

MERGE_THRESHOLD_UPDATES = 100  # trigger merge after this many updates
MERGE_CHECK_INTERVAL = 0.05

MAX_RETRIES = 100  # Maximum retry attempts
RETRY_DELAY = 0.01  # Initial delay in seconds (10ms)

# db_bunker
Bunker Dropbox data locally via python script

This script utilizes the dropbox python api to query and download (via chunking if needed) files to a local drive.

- maintains versioning
- does not replicate deletes
- email alerting

Wishlist
- containerize
- scheduler (instead of cron)
- better error handling with try/except

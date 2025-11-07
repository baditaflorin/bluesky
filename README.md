# Bluesky Follower Scraper

A robust Go application for fetching and storing follower data from the Bluesky social network API. This tool provides two modes of operation: clean start and resume capability.

## Features

- 🔄 **Automatic Retry Logic**: Built-in retry mechanism with exponential backoff for resilient API calls
- 💾 **SQLite Storage**: Persistent local storage of follower data
- 📊 **Pagination Support**: Handles cursor-based pagination to fetch all followers
- 🔁 **Resume Capability**: Can resume from a specific cursor if interrupted
- ⚡ **Batch Processing**: Efficient transaction-based database operations
- 🛡️ **Error Handling**: Comprehensive error handling and logging
- 📝 **Detailed Logging**: Extensive logging for monitoring and debugging

## Project Structure

```
bluesky/
├── clean_start/     # Fresh start mode - fetches all followers from beginning
│   └── main.go
├── resume_start/    # Resume mode - can start from a specific cursor
│   └── main.go
├── go.mod           # Go module definition
├── go.sum           # Go module checksums
└── README.md        # This file
```

## Requirements

- Go 1.23.2 or later
- SQLite3 (via go-sqlite3 driver)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/baditaflorin/bluesky.git
cd bluesky
```

2. Install dependencies:
```bash
go mod download
```

## Usage

### Clean Start Mode

Fetches all followers from the beginning:

```bash
cd clean_start
go run main.go
```

Or build and run:
```bash
cd clean_start
go build main.go
./main
```

### Resume Start Mode

Fetch followers starting from a specific cursor (useful for resuming interrupted runs):

```bash
cd resume_start
go run main.go -cursor "your_cursor_here"
```

Or start from the beginning:
```bash
cd resume_start
go run main.go
```

## Configuration

Both programs use the following constants that can be modified in the source code:

- `baseURL`: The Bluesky API endpoint
- `dbFile`: SQLite database filename (default: `followers.db`)
- `tableName`: Database table name (default: `followers`)
- `maxRetries`: Maximum retry attempts for failed API calls (default: 5)

## Database Schema

### Clean Start

```sql
CREATE TABLE followers (
    did TEXT PRIMARY KEY,
    handle TEXT,
    displayName TEXT,
    avatar TEXT,
    createdAt DATETIME,
    indexedAt DATETIME
);
```

### Resume Start

```sql
CREATE TABLE followers (
    did TEXT PRIMARY KEY,
    handle TEXT,
    displayName TEXT,
    avatar TEXT,
    viewer_muted BOOLEAN,
    viewer_blockedBy BOOLEAN,
    viewer_following TEXT,
    labels TEXT,
    createdAt DATETIME,
    description TEXT,
    indexedAt DATETIME
);
```

## Error Handling

The application includes comprehensive error handling for:

- Network failures (with automatic retry)
- API errors (HTTP status code checking)
- Invalid JSON responses
- HTML error pages
- Database transaction failures (with automatic rollback)

## Resilience Features

1. **Exponential Backoff**: Retry delays increase with each attempt
2. **HTTP Status Validation**: Checks response codes before processing
3. **Content Type Detection**: Identifies and handles HTML error pages
4. **Transaction Rollback**: Ensures database consistency on errors
5. **Resource Management**: Proper closing of HTTP response bodies to prevent leaks

## Logging

The application provides detailed logging including:
- API request attempts and timing
- Response parsing status
- Database operations
- Error messages with context
- Cursor progression

## Examples

### Example Output

```
2024/11/07 16:38:35 Initializing the database...
2024/11/07 16:38:35 Database initialized successfully.
2024/11/07 16:38:35 Fetching followers with cursor: 
2024/11/07 16:38:35 Attempt 1: Making API request to URL: https://...
2024/11/07 16:38:36 API request successful after 1.2s
2024/11/07 16:38:36 Reading response body...
2024/11/07 16:38:36 Response body read in 50ms
2024/11/07 16:38:36 Parsing JSON response.
2024/11/07 16:38:36 JSON parsed in 10ms, new cursor: abc123
2024/11/07 16:38:36 Returning 30 followers and cursor abc123
2024/11/07 16:38:36 Fetched 30 followers.
2024/11/07 16:38:36 Saving followers to the database...
2024/11/07 16:38:36 Followers saved successfully.
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the MIT License.

## Notes

- The database file (`followers.db`) is automatically created if it doesn't exist
- Each follower is uniquely identified by their DID (Decentralized Identifier)
- The `INSERT OR REPLACE` strategy ensures idempotent operations
- API rate limiting is handled through retry logic and backoff

## Troubleshooting

### Issue: "exceeded max retries"
**Solution**: Check your internet connection and verify the API endpoint is accessible.

### Issue: "failed to open database"
**Solution**: Ensure you have write permissions in the current directory.

### Issue: Incomplete data fetching
**Solution**: Use the resume_start mode with the last successful cursor to continue from where it stopped.
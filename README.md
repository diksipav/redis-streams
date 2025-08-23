# Redis Streams

A single-process, in-memory, Redis-like Streams system implemented in Rust with support for concurrent clients.

## Features

- **XADD**: Add entries with auto-generated (*) or explicit IDs with monotonic guarantee
- **XRANGE**: Query entries by range with optional COUNT limit
- **XREAD**: Read entries with support for $ (latest), BLOCK (timeout) and COUNT limit
- **XLEN**: Get stream length

## Usage Options

### Option 1: TCP Server + Multiple Clients

**For testing concurrency, producer/consumer patterns, and realistic Redis behavior:**

Available client commands:
- **XADD** `<stream_name>` `<id|*>` `<field>` `<value>` `[field value ...]`<br>
    Add entry (auto id with *).
- **XRANGE** `<stream_name>` `<start_id|->` `<end_id|+>` `[COUNT <n>]`<br>
    Retrieve entries within the specified range.
    - `-` as start_id → from the beginning
    - `+` as end_id → until the end
    - Otherwise, the ID must be in the format `<milliseconds>-<sequence>`.
- **XREAD** `[COUNT <n>]` `[BLOCK <ms>]` STREAMS `<stream_name>` `<id|$>`<br>
  Read entries with IDs greater than `<id>` or latest (`$`).
  - `COUNT <n>`: Limit to n entries per stream.
  - `BLOCK <ms>`: Wait up to ms for new entries, else return nil.
  - `$`: Only new entries added after the command is issued.
  - `0` or `0-0`: To read from the beginning of the stream.
- **XLEN** `<stream_name>`<br>
    Get stream length.
- **HELP**<br>
    Show this help.
- **QUIT/EXIT**<br>
    Exit.

```bash
# Terminal 1: Start the server
cargo run --bin server
# Server listening on 127.0.0.1:6379

# Terminal 2: Client 1 - Start blocking read
cargo run --bin client
> XREAD BLOCK 10000 STREAMS mystream $
# This client blocks waiting for data

# Terminal 3: Client 2 - Add data
cargo run --bin client  
> XADD mystream * user bob action login
1692547200000-0

# Terminal 2: Client 1 immediately unblocks with the new data
  1) "1692547200000-0"
  2) 1) "user"
     2) "bob"
     3) "action"
     4) "login"
```

### Option 2: As a Library

**For integration into other Rust applications:**

#### Database API

```rust
use redis_streams::Database;

let db = Database::new();
```

**XADD** - Add entries to streams
```rust
pub async fn xadd(
    &self,
    key: &str,                           // Stream name
    entry_id: Option<&str>,              // Entry ID ("<milliseconds>-<sequence>") or None for auto-generation
    fields: Vec<(String, Vec<u8>)>       // Field-value pairs
) -> Result<EntryId>

// Examples:
let fields = vec![("user".to_string(), b"alice".to_vec())];           // Byte literal
let fields = vec![("user".to_string(), "alice".as_bytes().to_vec())]; // From string
let id = db.xadd("mystream", None, fields).await?;                    // Auto-generated ID
let id = db.xadd("mystream", Some("1234-0"), fields).await?;          // Explicit ID
```

**XREAD** - Read entries from streams (with blocking support)
```rust
pub async fn xread(
    &self,
    key: &str,                           // Stream name
    from: &str,                          // Start position: "0", "$", or "<ms>-<seq>"
    count: Option<usize>,                // Max entries to return
    block: Option<Duration>              // Blocking timeout
) -> Result<Vec<Entry>>

// Examples:
let entries = db.xread("mystream", "0", None, None).await?;           // All entries from start
let entries = db.xread("mystream", "$", None, None).await?;           // From latest (non-blocking)
let entries = db.xread("mystream", "1234-0", Some(10), None).await?;  // 10 entries after ID
let entries = db.xread("mystream", "$", None, Some(Duration::from_secs(5))).await?; // Block 5s
```

**XLEN** - Get stream length
```rust
pub async fn xlen(&self, key: &str) -> Result<usize>

// Example:
let length = db.xlen("mystream").await?;
```

**XRANGE** - Query entries by range
```rust
pub async fn xrange(
    &self,
    key: &str,                           // Stream name
    start: RangeStart,                   // Start position
    end: RangeEnd,                       // End position  
    count: Option<usize>                 // Max entries to return
) -> Result<Vec<Entry>>

// Examples:
use redis_streams::{RangeStart, RangeEnd};
let entries = db.xrange("mystream", 
                       RangeStart::Start,     // From start of the stream
                       RangeEnd::End,       // To end of the stream
                       Some(100)).await?;           // Max 100 entries

let range = stream.xrange("mystream",
        RangeStart::Id(EntryId::new(1001, 0)),
        RangeEnd::Id(EntryId::new(1003, 0)),
        None,
    );
```

#### Complete Example
```rust
use redis_streams::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new();
    
    // Add some entries
    let fields = vec![("user".to_string(), b"alice".to_vec())];
    let id1 = db.xadd("mystream", None, fields).await?;
    println!("id1 {}", id1);

    let fields = vec![("user".to_string(), "bob".as_bytes().to_vec())];
    let id2 = db.xadd("mystream", None, fields).await?;
    println!("id2 {}", id2);

    
    // Read all entries
    let entries = db.xread("mystream", "0", None, None).await?;
    println!("Found {} entries", entries.len());
    
    // Get stream length
    let length = db.xlen("mystream").await?;
    println!("Stream length: {}", length);
    
    Ok(())
}
```

## Testing

```bash
# Unit tests
cargo test

# Integration tests (not implemented)
cargo test --test integration_tests

# Benchmarks (not implemented)
cargo bench
```

## Architecture

### Core Components

- **Database**: Multi-stream database with concurrent access
- **Stream**: Single stream with Vec storage for O(1) appends
- **Entry**: Stream entry with ID and field-value pairs
- **EntryId**: Millisecond timestamp + sequence number formatted as `<ms>-<seq>`
(e.g., 1692123456789-0)
  - **Milliseconds**: Derived from the system clock, representing the entry’s creation time.
  - **Sequence**: Incremented for entries created in the same millisecond to ensure uniqueness.
  - **Handling Clock Backwards**: If the system clock moves backwards, new entries use the last recorded 
  timestamp with an incremented sequence number to maintain ordering and avoid duplicates.
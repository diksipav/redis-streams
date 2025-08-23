use std::sync::Arc;
use std::time::Duration;
use std::str::FromStr;
use tokio::net::{TcpListener, TcpStream};
use redis_streams::{Database, Entry, EntryId, RangeEnd, RangeStart};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Redis Streams Server starting...");
    let db = Arc::new(Database::new());
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Server listening on 127.0.0.1:6379");
    println!("Connect with: cargo run --bin client");
    println!();

    loop {
        let (socket, addr) = listener.accept().await?;
        let db = Arc::clone(&db);
        println!("New client connected: {}", addr);
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, db, addr.to_string()).await {
                eprintln!("Client {} error: {}", addr, e);
            }
            println!("Client {} disconnected", addr);
        });
    }
}

async fn handle_client(
    socket: TcpStream,
    db: Arc<Database>,
    client_id: String,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::sync::mpsc;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let (tx, mut rx) = mpsc::channel::<String>(100);

    let client_id_writer = client_id.clone();
    let write_handle = tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            if let Err(e) = writer.write_all(response.as_bytes()).await {
                eprintln!("Client {} write error: {}", client_id_writer, e);
                break;
            }
            if let Err(e) = writer.flush().await {
                eprintln!("Client {} flush error: {}", client_id_writer, e);
                break;
            }
        }
    });

    let mut line = String::new();
    
    tx.send("Redis Streams Server: \n".to_string()).await?;
    tx.send("Commands: XADD, XRANGE, XREAD, XLEN, HELP, QUIT.".to_string()).await?;
    tx.send("\n> ".to_string()).await?;

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break; // Client disconnected
        }

        let input = line.trim();
        if input.is_empty() {
            tx.send("> ".to_string()).await?;
            continue;
        }

        let parts: Vec<String> = input.split_whitespace().map(String::from).collect();

        let db = Arc::clone(&db);
        let tx = tx.clone();

        match parts[0].to_uppercase().as_str() {
            "QUIT" | "EXIT" => {
                tx.send("Goodbye!\n".to_string()).await?;
                break;
            }
            "HELP" => {
                let response = handle_help().await;
                tx.send(format!("{}\n> ", response)).await?;
            }
            "XADD" => {
                let response = handle_xadd(&db, &parts, &client_id).await;
                tx.send(format!("{}\n> ", response)).await?;
            }
            "XRANGE" => {
                let response = handle_xrange(&db, &parts, &client_id).await;
                tx.send(format!("{}\n> ", response)).await?;
            }
            "XREAD" => {
                let response = handle_xread(&db, &parts, &client_id).await;
                tx.send(format!("{}\n> ", response)).await?;
            }
            "XLEN" => {
                let response = handle_xlen(&db, &parts, &client_id).await;
                tx.send(format!("{}\n> ", response)).await?;
            }
            _ => {
                tx.send("Unknown command. Type HELP for available commands.\n> ".to_string()).await?;
            }
        }
    }

    drop(tx);
    write_handle.await?;

    Ok(())
}

async fn handle_help() -> String {
  let b = "\x1b[1m";      // bold
  let dim = "\x1b[90m";   // dim
  let r = "\x1b[0m";      // reset
  format!(
  "Available commands:
{b}XADD  {r}{dim}<stream_name> <id|*> <field> <value> [field value ...]{r}
      Add entry (auto id with *).
{b}XRANGE{r} {dim}<stream_name> <start_id|-> <end_id|+> [COUNT <n>]{r}
      Retrieve entries within the specified range.
      • - as start_id → from the beginning
      • + as end_id → until the end
      • Otherwise, the ID must be in the format <milliseconds>-<sequence>.
{b}XREAD {r}{dim}[COUNT <n>] [BLOCK <ms>] STREAMS <stream_name> <id|$>{r}
      Read entries with IDs greater than <id> or latest ($).
      • COUNT <n>: Limit to n entries per stream.
      • BLOCK <ms>: Wait up to ms for new entries, else return nil.
      • $: Only new entries added after the command is issued.
      • 0 or 0-0: To read from the beginning of the stream.
{b}XLEN  {r}{dim}<stream_name>{r}
      Get stream length.
{b}HELP  {r}
      Show this help.
{b}QUIT/EXIT  {r}
      Exit.\n"
  )
}

async fn handle_xadd(db: &Database, parts: &[String], client_id: &str) -> String {
    if parts.len() < 4 {
        return "Error: Usage: XADD <stream_name> <id|*> <field> <value> [field value ...]".to_string();
    }

    let stream_name = parts[1].as_str();
    let id_str = parts[2].as_str();
    let field_parts = &parts[3..];

    if field_parts.len() % 2 != 0 {
        return "Error: field count must be even".to_string();
    }

    let entry_id: Option<&str> = if id_str == "*" {
        None
    } else {
        Some(id_str)
    };

    let fields: Vec<(String, Vec<u8>)> = field_parts
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].as_bytes().to_vec()))
        .collect();

    match db.xadd(stream_name, entry_id, fields).await {
        Ok(result_id) => {
            println!("[{}] XADD {} -> {}", client_id, stream_name, result_id);
            result_id.to_string()
        }
        Err(e) => format!("Error: {}", e),
    }
}

async fn handle_xrange(db: &Database, parts: &[String], client_id: &str) -> String {
    if parts.len() < 4 {
        return "Error: Usage: XRANGE <stream_name> <start_id|-> <end_id|+> [COUNT <c>]".to_string();
    }

    let stream_name = parts[1].as_str();
    let start_str = parts[2].as_str();
    let end_str = parts[3].as_str();

    let start = if start_str == "-" {
        RangeStart::Start
    } else {
        match EntryId::from_str(start_str) {
            Ok(id) => RangeStart::Id(id),
            Err(_) => return "Error: invalid start ID format".to_string(),
        }
    };

    let end = if end_str == "+" {
        RangeEnd::End
    } else {
        match EntryId::from_str(end_str) {
            Ok(id) => RangeEnd::Id(id),
            Err(_) => return "Error: invalid end ID format".to_string(),
        }
    };

    let mut count = None;
    if parts.len() >= 6 && parts[4].to_uppercase() == "COUNT" {
        match parts[5].parse::<usize>() {
            Ok(n) => count = Some(n),
            Err(_) => return "Error: invalid count".to_string(),
        }
    }

    match db.xrange(stream_name, start, end, count).await {
        Ok(entries) => {
            println!("[{}] XRANGE {} -> {} entries", client_id, stream_name, entries.len());
            format_entries(&entries)
        }
        Err(e) => format!("Error: {}", e),
    }
}

async fn handle_xread(db: &Database, parts: &[String], client_id: &str) -> String {
    if parts.len() < 4 {
        return "Error: Usage: XREAD [COUNT <c>] [BLOCK <ms>] STREAMS <stream_name> <id|$>".to_string();
    }

    let mut i = 1;
    let mut block_ms = None;
    let mut count = None;

    while i < parts.len() {
        match parts[i].to_uppercase().as_str() {
            "COUNT" => {
                if i + 1 >= parts.len() {
                    return "Error: COUNT requires value".to_string();
                }
                match parts[i + 1].parse::<usize>() {
                    Ok(n) => count = Some(n),
                    Err(_) => return "Error: invalid COUNT value".to_string(),
                }
                i += 2;
            }
            "BLOCK" => {
                if i + 1 >= parts.len() {
                    return "Error: BLOCK requires timeout value in milliseconds".to_string();
                }
                match parts[i + 1].parse::<u64>() {
                    Ok(0) => block_ms = None,
                    Ok(ms) => block_ms = Some(Duration::from_millis(ms)),
                    Err(_) => return "Error: invalid BLOCK timeout".to_string(),
                }
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return format!("Error: unexpected argument: {}", parts[i]),
        }
    }

    if i + 1 >= parts.len() {
        return "Error: STREAMS requires stream name and start ID".to_string();
    }

    let stream_name = parts[i].as_str();
    let start_id = parts[i + 1].as_str();

    if let Some(timeout) = block_ms {
        println!("[{}] XREAD BLOCK {} on {} (timeout: {}ms)", client_id, stream_name, start_id, timeout.as_millis());
    } else {
        println!("[{}] XREAD {} from {}", client_id, stream_name, start_id);
    }

    let start_time = std::time::Instant::now();
    match db.xread(stream_name, start_id, count, block_ms).await {
        Ok(entries) => {
            let elapsed = start_time.elapsed();
            if entries.is_empty() {
                if elapsed.as_millis() > 100 {
                    println!("[{}] XREAD {} completed after {}ms (timeout/no data)", client_id, stream_name, elapsed.as_millis());
                }
                "(empty array)".to_string()
            } else {
                if elapsed.as_millis() > 100 {
                    println!("[{}] XREAD {} unblocked after {}ms with {} entries", client_id, stream_name, elapsed.as_millis(), entries.len());
                } else {
                    println!("[{}] XREAD {} returned {} entries", client_id, stream_name, entries.len());
                }
                format_entries(&entries)
            }
        }
        Err(e) => format!("Error: {}", e),
    }
}

async fn handle_xlen(db: &Database, parts: &[String], client_id: &str) -> String {
    if parts.len() != 2 {
        return "Error: Usage: XLEN <stream_name>".to_string();
    }

    let stream_name = parts[1].as_str();
    match db.xlen(stream_name).await {
        Ok(length) => {
            println!("[{}] XLEN {} -> {}", client_id, stream_name, length);
            length.to_string()
        }
        Err(e) => format!("Error: {}", e),
    }
}

fn format_entries(entries: &[Entry]) -> String {
  let mut output = String::new();
  for (i, entry) in entries.iter().enumerate() {
      output.push_str(&format!("{})", i + 1));
      output.push_str(&format!(" 1) \"{}\"\n", entry.id));
      output.push_str("   2)");
      for (j, (field, value)) in entry.fields.iter().enumerate() {
          if j== 0 {
            output.push_str(&format!(" {}) \"{}\"\n",1, field));
          } else {
            output.push_str(&format!("      {}) \"{}\"\n", j * 2 + 1, field));
          }
          output.push_str(&format!("      {}) \"{}\"\n", j * 2 + 2, String::from_utf8_lossy(&value)));
      }
  }
  output
}
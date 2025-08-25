use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to Redis Streams Server...");
    let stream = TcpStream::connect("127.0.0.1:6379").await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    println!("Connected to the server!");
    println!();

    // Read welcome message until we see the prompt
    read_response(&mut reader).await?;

    // Handle user input
    let stdin = tokio::io::stdin();
    let mut stdin_reader = BufReader::new(stdin);
    let mut input = String::new();

    loop {
        input.clear();
        match stdin_reader.read_line(&mut input).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                let command = input.trim();
                if command.is_empty() {
                    continue;
                }
                if command.to_uppercase() == "QUIT" || command.to_uppercase() == "EXIT" {
                    writer.write_all(b"QUIT\n").await?;
                    writer.flush().await?;

                    let mut final_response = String::new();
                    let _ = reader.read_line(&mut final_response).await;
                    print!("{}", final_response);
                    break;
                }

                writer.write_all(command.as_bytes()).await?;
                // Server expects new lines at the end of commands
                writer.write_all(b"\n").await?;
                writer.flush().await?;

                // Read response until we get a prompt
                read_response(&mut reader).await?;
            }
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                break;
            }
        }
    }

    println!("Client shutting down...");
    Ok(())
}

// Server sends "> ", using read_line will result in indefinite waiting for the "\n".
async fn read_response(reader: &mut BufReader<OwnedReadHalf>) -> Result<(), std::io::Error> {
    let mut buffer = Vec::new();

    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte).await {
            Ok(_) => {
                buffer.push(byte[0]);
                let text = String::from_utf8_lossy(&buffer);
                print!("{}", char::from(byte[0]));
                io::stdout().flush()?;

                if text.ends_with("\n> ") {
                    return Ok(());
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

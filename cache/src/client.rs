use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::Bytes;
use std::io::Cursor;
use std::time::Duration;

use crate::protocol::{serialize_command, parse_resp, parse_response, Command, Response};

pub struct FerricClient {
    stream: TcpStream,
    /// Accumulates bytes across reads until a full RESP frame is available.
    /// May retain trailing bytes belonging to the next reply (RESP frames are
    /// self-delimiting, so we consume exactly one frame per command).
    buffer: Vec<u8>,
}

impl FerricClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            buffer: Vec::with_capacity(4096),
        })
    }

    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, Box<dyn std::error::Error>> {
        let cmd = Command::Get {
            key: Bytes::from(key.to_string()),
        };

        let response = self.send_command(cmd).await?;

        match response {
            Response::Value(value) => Ok(Some(value)),
            Response::NotFound | Response::Null => Ok(None),
            Response::Error(e) => Err(e.into()),
            _ => Err("Unexpected response for GET".into()),
        }
    }

    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.set_with_ttl(key, value, None).await
    }

    pub async fn set_with_ttl(
        &mut self,
        key: &str,
        value: &str,
        ttl: Option<Duration>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ttl_secs = ttl.map(|d| d.as_secs() as u32);
        let cmd = Command::Set {
            key: Bytes::from(key.to_string()),
            value: Bytes::from(value.to_string()),
            ttl_secs,
        };

        let response = self.send_command(cmd).await?;

        match response {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(e.into()),
            _ => Err("Unexpected response for SET".into()),
        }
    }

    pub async fn delete(&mut self, key: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let cmd = Command::Delete {
            key: Bytes::from(key.to_string()),
        };

        let response = self.send_command(cmd).await?;

        match response {
            // DEL replies with the number of keys removed (RESP integer).
            Response::Integer(n) => Ok(n > 0),
            Response::Ok => Ok(true),
            Response::NotFound | Response::Null => Ok(false),
            Response::Error(e) => Err(e.into()),
            _ => Err("Unexpected response for DELETE".into()),
        }
    }

    async fn send_command(&mut self, cmd: Command) -> Result<Response, Box<dyn std::error::Error>> {
        let cmd_bytes = serialize_command(cmd);
        self.stream.write_all(&cmd_bytes).await?;
        self.stream.flush().await?;

        self.read_response().await
    }

    /// Read exactly one RESP frame from the connection, buffering across reads
    /// until a complete frame is available. The server speaks real RESP text
    /// frames (`+OK\r\n`, `$3\r\nabc\r\n`, …), so we parse incrementally rather
    /// than assuming any fixed-size header.
    async fn read_response(&mut self) -> Result<Response, Box<dyn std::error::Error>> {
        let mut scratch = [0u8; 4096];

        loop {
            // Attempt to parse a full frame from whatever we have buffered.
            if !self.buffer.is_empty() {
                let mut cursor = Cursor::new(self.buffer.as_slice());
                if parse_resp(&mut cursor).is_ok() {
                    let consumed = cursor.position() as usize;
                    let frame = Bytes::copy_from_slice(&self.buffer[..consumed]);
                    self.buffer.drain(..consumed);
                    return parse_response(frame).map_err(Into::into);
                }
                // Otherwise the frame is incomplete — fall through and read more.
            }

            let n = self.stream.read(&mut scratch).await?;
            if n == 0 {
                return Err("connection closed by server before a full response".into());
            }
            self.buffer.extend_from_slice(&scratch[..n]);
        }
    }
}

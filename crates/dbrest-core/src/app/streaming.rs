//! Streaming response support
//!
//! This module provides functionality to stream large JSON responses
//! instead of loading them entirely into memory.

use axum::body::Body;
use bytes::Bytes;
use futures::stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Stream a JSON array by parsing the body string and streaming it chunk by chunk.
///
/// This is useful for large result sets where we want to avoid loading
/// the entire JSON array into memory at once.
pub fn stream_json_array(body: String) -> Body {
    const CHUNK_SIZE: usize = 8192;
    let bytes: Bytes = Bytes::from(body.into_bytes());
    let len = bytes.len();
    let offsets: Vec<usize> = (0..len).step_by(CHUNK_SIZE).collect();
    let stream = stream::iter(offsets.into_iter().map(move |start| {
        let end = (start + CHUNK_SIZE).min(len);
        Ok::<_, std::io::Error>(bytes.slice(start..end))
    }));
    Body::from_stream(stream)
}

/// Check if a response body should be streamed based on size and configuration.
pub fn should_stream(body_size: usize, streaming_enabled: bool, threshold: u64) -> bool {
    streaming_enabled && (body_size as u64) > threshold
}

/// Stream a JSON array response with proper formatting.
///
/// Takes a pre-formatted JSON array string and streams it in chunks.
pub fn stream_json_response(json_body: String) -> Body {
    const CHUNK_SIZE: usize = 8192;
    let bytes: Bytes = Bytes::from(json_body.into_bytes());
    let len = bytes.len();
    let offsets: Vec<usize> = (0..len).step_by(CHUNK_SIZE).collect();
    let stream = stream::iter(offsets.into_iter().map(move |start| {
        let end = (start + CHUNK_SIZE).min(len);
        Ok::<_, std::io::Error>(bytes.slice(start..end))
    }));
    Body::from_stream(stream)
}

/// A stream that yields JSON array elements one at a time.
///
/// This allows streaming very large arrays without loading them all into memory.
pub struct JsonArrayStream {
    items: Vec<serde_json::Value>,
    current_index: usize,
    buffer: String,
    opened: bool,
    done: bool,
}

impl JsonArrayStream {
    /// Create a new JSON array stream from a vector of JSON values.
    pub fn new(items: Vec<serde_json::Value>) -> Self {
        Self {
            items,
            current_index: 0,
            buffer: String::new(),
            opened: false,
            done: false,
        }
    }

    /// Get the next chunk of JSON to send.
    fn next_chunk(&mut self) -> Option<Bytes> {
        if self.done {
            return None;
        }

        if !self.opened {
            self.buffer.push('[');
            self.opened = true;
            // Return opening bracket immediately
            let chunk = Bytes::from(self.buffer.clone());
            self.buffer.clear();
            return Some(chunk);
        }

        // Stream items one at a time
        while self.current_index < self.items.len() {
            if self.current_index > 0 {
                self.buffer.push(',');
            }

            // Serialize the current item
            if let Ok(item_json) = serde_json::to_string(&self.items[self.current_index]) {
                self.buffer.push_str(&item_json);
            }

            self.current_index += 1;

            // Return chunk if buffer is large enough, or if it's the last item
            if self.buffer.len() >= 8192 || self.current_index >= self.items.len() {
                let chunk = Bytes::from(self.buffer.clone());
                self.buffer.clear();

                // If this was the last item, we need to close the array next
                if self.current_index >= self.items.len() {
                    // Don't mark as done yet - we still need to send the closing bracket
                }

                return Some(chunk);
            }
        }

        // Close the array
        if self.current_index >= self.items.len() && !self.done {
            self.buffer.push(']');
            self.done = true;
            let chunk = Bytes::from(self.buffer.clone());
            self.buffer.clear();
            return Some(chunk);
        }

        None
    }
}

impl futures::Stream for JsonArrayStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.next_chunk() {
            Some(chunk) => Poll::Ready(Some(Ok(chunk))),
            None => Poll::Ready(None),
        }
    }
}

/// Stream a vector of JSON values as a JSON array.
pub fn stream_json_array_from_values(items: Vec<serde_json::Value>) -> Body {
    let stream = JsonArrayStream::new(items);
    Body::from_stream(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_json_array_stream() {
        use axum::body::Body;

        let items = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
            json!({"id": 3, "name": "Charlie"}),
        ];

        let stream = JsonArrayStream::new(items);
        let body = Body::from_stream(stream);

        // Collect all chunks using axum's body utilities
        let bytes = axum::body::to_bytes(body, 10 * 1024 * 1024).await.unwrap();
        let json_str = String::from_utf8(bytes.to_vec()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_stream_json_response_preserves_content() {
        let original = r#"[{"id":1},{"id":2},{"id":3}]"#.to_string();
        let body = stream_json_response(original.clone());
        let bytes = axum::body::to_bytes(body, 10 * 1024 * 1024).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), original);
    }

    #[tokio::test]
    async fn test_stream_json_response_large_body() {
        // Body larger than CHUNK_SIZE (8192) to test multi-chunk streaming
        let large = "x".repeat(20_000);
        let body = stream_json_response(large.clone());
        let bytes = axum::body::to_bytes(body, 10 * 1024 * 1024).await.unwrap();
        assert_eq!(bytes.len(), 20_000);
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), large);
    }

    #[tokio::test]
    async fn test_stream_json_array_preserves_content() {
        let original = r#"[1,2,3]"#.to_string();
        let body = stream_json_array(original.clone());
        let bytes = axum::body::to_bytes(body, 10 * 1024 * 1024).await.unwrap();
        assert_eq!(String::from_utf8(bytes.to_vec()).unwrap(), original);
    }

    #[test]
    fn test_should_stream() {
        // Should stream if enabled and size exceeds threshold
        assert!(should_stream(11 * 1024 * 1024, true, 10 * 1024 * 1024));

        // Should not stream if disabled
        assert!(!should_stream(11 * 1024 * 1024, false, 10 * 1024 * 1024));

        // Should not stream if size is below threshold
        assert!(!should_stream(5 * 1024 * 1024, true, 10 * 1024 * 1024));
    }
}

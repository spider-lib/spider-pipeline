//! Streaming Item Pipeline for exporting scraped items to a JSON file.
//!
//! This module provides the `StreamingJsonWriterPipeline`, an item pipeline designed
//! to stream `ScrapedItem`s directly to a JSON file without collecting them in memory.
//! This approach significantly reduces memory usage when processing large numbers of items.
//!
//! Key features include:
//! - Streaming processing: Items are written to the output file as they arrive
//! - Low memory footprint: No accumulation of items in memory
//! - Chunked writing: Items are written in batches to improve I/O performance
//! - Proper JSON formatting: Maintains valid JSON array structure

use crate::pipeline::Pipeline;
use async_trait::async_trait;
use kanal::unbounded_async;
use serde_json::Value;
use spider_util::error::PipelineError;
use spider_util::item::ScrapedItem;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;
use tracing::{debug, error, info};

const DEFAULT_BATCH_SIZE: usize = 100;

enum StreamingJsonCommand {
    Write(Value),
    Shutdown(kanal::AsyncSender<Result<(), PipelineError>>),
}

/// A pipeline that streams items directly to a JSON file without accumulating them in memory.
pub struct StreamingJsonWriterPipeline<I: ScrapedItem> {
    command_sender: kanal::AsyncSender<StreamingJsonCommand>,
    _phantom: PhantomData<I>,
}

impl<I: ScrapedItem> StreamingJsonWriterPipeline<I> {
    /// Creates a new `StreamingJsonWriterPipeline` with default batch size.
    pub fn new(file_path: impl AsRef<Path>) -> Result<Self, PipelineError> {
        Self::with_batch_size(file_path, DEFAULT_BATCH_SIZE)
    }

    /// Creates a new `StreamingJsonWriterPipeline` with a specified batch size.
    pub fn with_batch_size(
        file_path: impl AsRef<Path>,
        batch_size: usize,
    ) -> Result<Self, PipelineError> {
        spider_util::utils::validate_output_dir(&file_path)
            .map_err(|e| PipelineError::Other(e.to_string()))?;
        let path_buf = file_path.as_ref().to_path_buf();
        info!(
            "Initializing StreamingJsonWriterPipeline for file: {:?}",
            path_buf
        );

        let (command_sender, command_receiver) = unbounded_async::<StreamingJsonCommand>();

        tokio::task::spawn(async move {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path_buf)
                .map_err(|e| {
                    error!("Failed to create/open file {:?}: {}", path_buf, e);
                })
                .ok();

            if let Some(file) = file {
                let mut writer = BufWriter::new(file);
                let mut items_buffer = Vec::with_capacity(batch_size);
                let mut first_item = true;

                if writer.write_all(b"[\n").is_err() {
                    error!("Failed to write opening bracket to file: {:?}", path_buf);
                }

                info!(
                    "StreamingJsonWriterPipeline async task started for file: {:?}",
                    path_buf
                );

                while let Ok(command) = command_receiver.recv().await {
                    match command {
                        StreamingJsonCommand::Write(value) => {
                            items_buffer.push(value);

                            if items_buffer.len() >= batch_size {
                                flush_items(&mut writer, &mut items_buffer, &mut first_item).ok();
                            }
                        }
                        StreamingJsonCommand::Shutdown(responder) => {
                            if !items_buffer.is_empty() {
                                flush_items(&mut writer, &mut items_buffer, &mut first_item).ok();
                            }

                            let result = writer
                                .flush()
                                .and_then(|_| {
                                    let file_ref = writer.get_mut();
                                    file_ref.write_all(b"\n]")
                                })
                                .map_err(|e| PipelineError::IoError(e.to_string()));

                            if responder.send(result).await.is_err() {
                                error!("Failed to send shutdown response.");
                            }
                            break;
                        }
                    }
                }

                info!(
                    "StreamingJsonWriterPipeline async task for file: {:?} finished.",
                    path_buf
                );
            }
        });

        Ok(StreamingJsonWriterPipeline {
            command_sender,
            _phantom: PhantomData,
        })
    }
}

fn flush_items(
    writer: &mut BufWriter<std::fs::File>,
    items_buffer: &mut Vec<Value>,
    first_item: &mut bool,
) -> Result<(), PipelineError> {
    for (i, item) in items_buffer.drain(..).enumerate() {
        let prefix = if *first_item && i == 0 {
            *first_item = false;
            ""
        } else {
            ","
        };

        let item_str = serde_json::to_string(&item)
            .map_err(|e| PipelineError::SerializationError(e.to_string()))?;

        writer
            .write_all(format!("{}  {}\n", prefix, item_str).as_bytes())
            .map_err(|e| PipelineError::IoError(e.to_string()))?;
    }

    writer
        .flush()
        .map_err(|e| PipelineError::IoError(e.to_string()))
}

#[async_trait]
impl<I: ScrapedItem> Pipeline<I> for StreamingJsonWriterPipeline<I> {
    fn name(&self) -> &str {
        "StreamingJsonWriterPipeline"
    }

    async fn process_item(&self, item: I) -> Result<Option<I>, PipelineError> {
        debug!("StreamingJsonWriterPipeline processing item.");
        let json_value = item.to_json_value();

        self.command_sender
            .send(StreamingJsonCommand::Write(json_value))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Write command: {}", e)))?;

        Ok(Some(item))
    }

    async fn close(&self) -> Result<(), PipelineError> {
        info!("Closing StreamingJsonWriterPipeline.");
        let (tx, rx) = kanal::unbounded_async();
        self.command_sender
            .send(StreamingJsonCommand::Shutdown(tx))
            .await
            .map_err(|e| PipelineError::Other(format!("Failed to send Shutdown command: {}", e)))?;

        rx.recv().await.map_err(|e| {
            PipelineError::Other(format!("Failed to receive shutdown response: {}", e))
        })?
    }
}


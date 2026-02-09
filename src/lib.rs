//! # spider-pipeline
//!
//! Built-in pipeline implementations for the `spider-lib` framework.
//!
//! Processes, filters, transforms, and stores scraped data.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_pipeline::json_writer::JsonWriterPipeline;
//! use spider_pipeline::console_writer::ConsoleWriterPipeline;
//!
//! let crawler = CrawlerBuilder::new(MySpider)
//!     .add_pipeline(JsonWriterPipeline::new("output.json")?)
//!     .add_pipeline(ConsoleWriterPipeline::new())
//!     .build()
//!     .await?;
//! ```

// Core pipelines (always available)
pub mod console_writer;
pub mod deduplication;
pub mod pipeline;

// Optional pipelines (feature-gated)
#[cfg(feature = "pipeline-csv")]
pub mod csv_exporter;

#[cfg(feature = "pipeline-json")]
pub mod json_writer;

#[cfg(feature = "pipeline-jsonl")]
pub mod jsonl_writer;

#[cfg(feature = "pipeline-sqlite")]
pub mod sqlite_writer;

#[cfg(feature = "pipeline-stream-json")]
pub mod stream_json_writer;

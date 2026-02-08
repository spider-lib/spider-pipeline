//! # spider-pipeline
//!
//! Provides built-in pipeline implementations for the `spider-lib` framework.
//!
//! ## Overview
//!
//! The `spider-pipeline` crate contains a collection of pipeline implementations
//! that process, filter, transform, and store scraped data. Pipelines are the
//! final stage in the crawling process, taking the items extracted by spiders
//! and performing operations like validation, storage, or transformation.
//!
//! ## Available Pipelines
//!
//! - **Console Writer**: Simple pipeline for printing items to the console (debugging)
//! - **Deduplication**: Filters out duplicate items based on configurable keys
//! - **JSON Writer**: Collects all items and writes them to a JSON file at the end
//! - **JSONL Writer**: Streams items as individual JSON objects to a file
//! - **CSV Exporter**: Exports items to CSV format with automatic schema inference
//! - **SQLite Writer**: Stores items in a SQLite database with automatic schema creation
//! - **Streaming JSON Writer**: Efficiently streams items to JSON without accumulating in memory
//!
//! ## Architecture
//!
//! Each pipeline implements the `Pipeline` trait, allowing for flexible composition
//! and chaining of processing steps. Multiple pipelines can be attached to a
//! single crawler to process items in different ways simultaneously.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_pipeline::json_writer::JsonWriterPipeline;
//! use spider_pipeline::console_writer::ConsoleWriterPipeline;
//!
//! // Add pipelines to your crawler
//! let crawler = CrawlerBuilder::new(MySpider)
//!     .add_pipeline(JsonWriterPipeline::new("output.json")?)
//!     .add_pipeline(ConsoleWriterPipeline::new())
//!     .build()
//!     .await?;
//! ```

pub mod console_writer;
pub mod deduplication;

pub mod csv_exporter;
pub mod json_writer;
pub mod jsonl_writer;
pub mod pipeline;
pub mod sqlite_writer;
pub mod streaming_json_writer;

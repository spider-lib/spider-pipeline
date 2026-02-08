# spider-pipeline

Provides built-in pipeline implementations for the `spider-lib` framework.

## Overview

The `spider-pipeline` crate contains a collection of pipeline implementations that process, filter, transform, and store scraped data. Pipelines are the final stage in the crawling process, taking the items extracted by spiders and performing operations like validation, storage, or transformation.

Pipelines are organized using feature flags to prevent bloat. Core pipelines are always available, while advanced features can be enabled as needed.

## Available Pipelines

### Core Pipelines (Always Available)
- **Console Writer**: Simple pipeline for printing items to the console (debugging)
- **Deduplication**: Filters out duplicate items based on configurable keys

### Optional Pipelines (Feature-Gated)
- **JSON Writer**: Collects all items and writes them to a JSON file at the end (feature: `pipeline-json`)
- **JSONL Writer**: Streams items as individual JSON objects to a file (feature: `pipeline-jsonl`)
- **CSV Exporter**: Exports items to CSV format with automatic schema inference (feature: `pipeline-csv`)
- **SQLite Writer**: Stores items in a SQLite database with automatic schema creation (feature: `pipeline-sqlite`)
- **Streaming JSON Writer**: Efficiently streams items to JSON without accumulating in memory (feature: `pipeline-streaming-json`)

## Features

This crate uses feature flags to allow selective inclusion of pipeline components:

- `core` (default): Includes core pipeline functionality
- `pipeline-csv`: Enables CSV export capabilities
- `pipeline-json`: Enables JSON writing functionality
- `pipeline-jsonl`: Enables JSONL writing functionality
- `pipeline-sqlite`: Enables SQLite database functionality
- `pipeline-streaming-json`: Enables streaming JSON functionality

### Important Feature Relationships
There are no interdependent features within spider-pipeline. All pipeline features operate independently.

To use only core functionality:
```toml
[dependencies]
spider-pipeline = { version = "...", default-features = false, features = ["core"] }
```

To include specific pipelines:
```toml
[dependencies]
spider-pipeline = { version = "...", features = ["pipeline-csv", "pipeline-json"] }
```

## Architecture

Each pipeline implements the `Pipeline` trait, allowing for flexible composition and chaining of processing steps. Multiple pipelines can be attached to a single crawler to process items in different ways simultaneously.

## Usage

```rust
use spider_pipeline::json_writer::JsonWriterPipeline;
use spider_pipeline::console_writer::ConsoleWriterPipeline;

// Add pipelines to your crawler
let crawler = CrawlerBuilder::new(MySpider)
    .add_pipeline(JsonWriterPipeline::new("output.json")?)
    .add_pipeline(ConsoleWriterPipeline::new())
    .build()
    .await?;
```

## Pipeline Types

### Console Writer

Prints items to the console for debugging purposes.

**Configuration:**
```rust
use spider_pipeline::console_writer::ConsoleWriterPipeline;

let console_writer = ConsoleWriterPipeline::new();
```

### Deduplication

Filters out duplicate items based on configurable keys to ensure data quality.

**Configuration:**
```rust
use spider_pipeline::deduplication::DeduplicationPipeline;

// Deduplicate based on a single field
let dedup_pipeline = DeduplicationPipeline::new(&["url"]);

// Deduplicate based on multiple fields
let dedup_pipeline = DeduplicationPipeline::new(&["title", "author"]);
```

### JSON Writer

Collects all items and writes them to a single JSON file at the end of the crawl.

**Configuration:**
```rust
use spider_pipeline::json_writer::JsonWriterPipeline;

let json_writer = JsonWriterPipeline::new("output.json")?;
```

### JSONL Writer

Streams items as individual JSON objects to a file, one per line, for efficient processing.

**Configuration:**
```rust
use spider_pipeline::jsonl_writer::JsonlWriterPipeline;

let jsonl_writer = JsonlWriterPipeline::new("output.jsonl")?;
```

### CSV Exporter

Exports items to CSV format with automatic schema inference from the data structure.

**Configuration:**
```rust
use spider_pipeline::csv_exporter::CsvExporterPipeline;

let csv_exporter = CsvExporterPipeline::new("output.csv")?;
```

### SQLite Writer

Stores items in a SQLite database with automatic schema creation based on the item structure.

**Configuration:**
```rust
use spider_pipeline::sqlite_writer::SqliteWriterPipeline;

let sqlite_writer = SqliteWriterPipeline::new("database.db", "items")?;
```

### Streaming JSON Writer

Efficiently streams items to JSON format without accumulating them in memory.

**Configuration:**
```rust
use spider_pipeline::streaming_json_writer::StreamingJsonWriterPipeline;

// With default batch size (100 items)
let streaming_json_writer = StreamingJsonWriterPipeline::new("output.json")?;

// With custom batch size
let streaming_json_writer = StreamingJsonWriterPipeline::with_batch_size("output.json", 50)?;
```

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

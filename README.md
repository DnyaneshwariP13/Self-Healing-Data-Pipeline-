# Self-Healing Data Pipeline

![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0%2B-017CEE?logo=apache-airflow)
![Ollama](https://img.shields.io/badge/Ollama-llama3.2-orange)
![Docker](https://img.shields.io/badge/Docker%20Compose-v2-2496ED?logo=docker)

An Apache Airflow-based data pipeline that automatically detects and heals data quality issues in Yelp review datasets, then performs sentiment analysis using a local Ollama LLM. The pipeline is designed to be **resilient, observable, and scalable**, capable of processing millions of records via parallel batch execution.

## Table of Contents

- [Overview](#overview)
- [Complete Architecture](#complete-architecture)
- [Features](#features)
- [Versions Used](#versions-used)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline Components](#pipeline-components)
- [Health Report](#health-report)
- [Docker Setup](#docker-setup)
- [Dependencies](#dependencies)
- [Testing](#testing)
- [Contributing](#contributing)
- [Acknowledgements](#acknowledgements)

## Overview

This project demonstrates a production-style self-healing data pipeline. It ingests raw JSON review data, detects common data quality issues, applies automated healing strategies, and then analyzes sentiment using a locally hosted large language model. A health report is generated for every run, and a batch runner script lets you process millions of records by triggering multiple DAG runs in parallel.

## Complete Architecture

```text
                Yelp Dataset
                      │
                      ▼
             ┌─────────────────┐
             │ batch_runner.py │
             └────────┬────────┘
                      │
             Generates offsets
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
    offset 0       offset 1000   offset 2000
        │             │             │
        └─────────────┼─────────────┘
                      ▼
             ┌─────────────────┐
             │     Airflow     │
             │  DAG Triggering │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ CeleryExecutor  │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Airflow Worker  │
             └────────┬────────┘
                      ▼
              Load Reviews
                      │
                      ▼
              Diagnose Issues
                      │
                      ▼
              Self-Healing
                      │
                      ▼
                Ollama
              llama3.2
                      │
                      ▼
             Sentiment Analysis
                      │
                      ▼
               Aggregation
                      │
                      ▼
              Health Report
```

### Flow Explanation

1. **Yelp Dataset**: Raw JSON Lines file containing academic review data.
2. **`batch_runner.py`**: Splits the dataset into offsets based on `--batch-size` and triggers multiple DAG runs (sequentially or in parallel).
3. **Airflow DAG Triggering**: Each offset becomes an independent DAG run with its own `offset` and `batch_size` params.
4. **CeleryExecutor**: Distributes the triggered DAG runs across available workers via the Redis broker.
5. **Airflow Worker**: Picks up tasks and executes them in the containerized environment.
6. **Load Reviews**: Reads the batch slice using `itertools.islice` from the offset.
7. **Diagnose Issues**: Checks each review for `missing_text`, `empty_text`, `wrong_type`, `special_characters_only`, and `too_long`.
8. **Self-Healing**: Applies the corresponding healing strategy and logs the action token.
9. **Ollama (llama3.2)**: Receives healed text and returns sentiment with a confidence score.
10. **Sentiment Analysis**: Parses the LLM response into `POSITIVE` / `NEGATIVE` / `NEUTRAL` with retry and fallback.
11. **Aggregation**: Computes success/healing/degradation rates, sentiment distribution, and average confidence.
12. **Health Report**: Writes a timestamped JSON report and classifies run health as `HEALTHY`, `WARNING`, `DEGRADED`, or `CRITICAL`.

## Features

- **Automatic Data Healing**: Detects and fixes 5 types of data quality issues:
  - `missing_text`: Fills with a placeholder.
  - `empty_text`: Fills with a placeholder.
  - `wrong_type`: Converts non-string values to strings.
  - `special_characters_only`: Replaces with a `[Non-text content]` marker.
  - `too_long`: Truncates text to the maximum allowed length.
- **Sentiment Analysis**: Uses an LLM to classify each review as `POSITIVE`, `NEGATIVE`, or `NEUTRAL` with a confidence score.
- **Retry & Fallback Logic**: The LLM call is retried up to 3 times. If all attempts fail, the record is marked as degraded with a `NEUTRAL` sentiment.
- **Health Reporting**: Generates a JSON report with metrics such as `success_rate`, `healing_rate`, `degradation_rate`, and sentiment distribution.
- **Batch Processing**: A dedicated `batch_runner.py` script processes large datasets by triggering DAG runs with configurable batch sizes, offsets, and parallelism.
- **Configurable**: All key parameters (input file, batch size, Ollama host, model name, etc.) can be set via Airflow params or environment variables.

## Versions Used

| Component | Version | Notes |
|-----------|---------|-------|
| Apache Airflow | >=3.0.6 | Uses the new Airflow 3 API server + DAG processor architecture. |
| Airflow FAB Provider | >=3.0.0 | Flask-AppBuilder auth backend for Airflow 3. |
| Python | 3.11+ | Required by Airflow 3. |
| PostgreSQL | 13 | Airflow metadata database. |
| Redis | 7.2 | Celery broker for task distribution. |
| CeleryExecutor | Bundled with Airflow 3 | Parallel task execution. |
| Ollama | >=0.6.0 | Local LLM runtime. |
| Ollama Model | llama3.2 | Default sentiment analysis model (configurable). |
| psycopg2-binary | >=2.9.0 | PostgreSQL driver. |
| transformers | latest | Included for future NLP extensions. |
| torch | latest | Included for future ML extensions. |
| pytest | latest | Test framework. |
| Docker Compose | v2 | Container orchestration for local development. |

## Prerequisites

- **Docker & Docker Compose (v2)**: For running Airflow and its dependencies.
- **Ollama**: Installed and running on your host machine (default host: `http://host.docker.internal:11434`). Pull the required model:

  ```bash
  ollama pull llama3.2
  ```

- **Python 3.11+**: For running the batch runner script locally.
- **Input Data**: A JSON Lines file (e.g., `yelp_academic_dataset_review.json`) placed in the `input/` directory.

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/DnyaneshwariP13/Self-Healing-Data-Pipeline-.git
   cd Self-Healing-Data-Pipeline-
   ```

2. **Set up the directory structure**

   Ensure the following directories exist at the project root (the Docker Compose file mounts them):

   ```text
   Self-Healing-Data-Pipeline-/
   ├── airflow/
   │   ├── logs/
   │   └── plugins/
   ├── dags/
   ├── input/
   │   └── yelp_academic_dataset_review.json
   └── output/
   ```

3. **Start the Airflow stack**

   ```bash
   docker compose -f airflow/docker-compose.yaml up -d
   ```

   This starts PostgreSQL, Redis, the Airflow API server, DAG processor, scheduler, and worker.

4. **Access the Airflow UI**

   Open [http://localhost:8080](http://localhost:8080) and log in with:

   - **Username:** `airflow`
   - **Password:** `airflow`

## Configuration

The pipeline can be configured via Airflow params or environment variables. Key settings are defined in the DAG's `Config` class and in `docker-compose.yaml`.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PIPELINE_BASE_DIR` | `/opt/airflow` | Base directory inside the container. |
| `PIPELINE_INPUT_FILE` | `/opt/airflow/input/yelp_academic_dataset_review.json` | Path to the input JSON file. |
| `PIPELINE_OUTPUT_DIR` | `/opt/airflow/output` | Directory where health reports are written. |
| `PIPELINE_MAX_TEXT_LENGTH` | `2000` | Maximum allowed text length before truncation. |
| `OLLAMA_HOST` | `http://host.docker.internal:11434` | Ollama server URL. |
| `OLLAMA_MODEL` | `llama3.2` | Model to use for sentiment analysis. |
| `OLLAMA_TIMEOUT` | `120` | Timeout in seconds for Ollama requests. |
| `OLLAMA_RETRIES` | `3` | Number of retry attempts for failed LLM calls. |

## Usage

### Triggering the Pipeline

You can trigger the DAG manually from the Airflow UI or via the CLI. The DAG accepts the following parameters (configured as Airflow Params):

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `input_file` | string | see config | Path to the input JSON file. |
| `batch_size` | integer | `100` | Number of reviews to process in the run. |
| `offset` | integer | `0` | Offset to start reading from. |
| `ollama_model` | string | `llama3.2` | Ollama model name. |

**Example CLI trigger:**

```bash
docker compose -f airflow/docker-compose.yaml exec -T airflow-api-server \
  airflow dags trigger self_healing_pipeline \
  --conf '{"batch_size": 500, "offset": 0, "input_file": "/opt/airflow/input/yelp_academic_dataset_review.json", "ollama_model": "llama3.2"}'
```

### Batch Processing

For large datasets, use the `batch_runner.py` script. It triggers multiple DAG runs with different offsets.

```bash
# Process 5 million records with a batch size of 1000 (sequential)
python scripts/batch_runner.py --total 5000000 --batch-size 1000

# Process with 5 parallel DAG runs
python scripts/batch_runner.py --total 5000000 --batch-size 5000 --parallel 5

# Resume from offset 100000
python scripts/batch_runner.py --total 5000000 --batch-size 1000 --start 100000

# Dry run to see what would be triggered
python scripts/batch_runner.py --total 5000000 --batch-size 10000 --dry-run
```

**Batch Runner Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--total` | *(required)* | Total number of records to process. |
| `--batch-size` | `1000` | Records per DAG run. |
| `--parallel` | `1` | Number of parallel DAG runs to trigger. |
| `--start` | `0` | Starting offset for resume. |
| `--delay` | `1.0` | Delay between triggers in seconds. |
| `--dry-run` | `False` | Print what would be triggered without executing. |

## Pipeline Components

### DAG Tasks

The DAG (`dags/agentic_pipeline_dag.py`) consists of the following tasks:

| Task | Description |
|------|-------------|
| `load_model` | Validates that the specified Ollama model is available. If not found locally, it attempts to pull it from the remote repository. Returns model metadata. |
| `load_reviews` | Reads a batch of reviews from the input JSON file using `itertools.islice`, starting at the given offset. Invalid JSON lines are skipped with a warning. |
| `diagnose_and_heal_batch` | Iterates over each review and applies healing logic based on detected issues. Returns healed reviews with metadata about the healing actions taken. |
| `batch_analyze_sentiment` | Sends each healed review to Ollama for sentiment analysis. Constructs a prompt, parses the JSON response, and includes retry logic. If the LLM is unreachable, returns degraded results. |
| `aggregate_results` | Computes summary statistics: total processed, success/healed/degraded counts, sentiment distribution, healing action statistics, star-sentiment correlation, and average confidence. Writes the full results to a timestamped JSON file in the output directory. |
| `generate_health_report` | Evaluates the health of the run based on degradation and healing rates, and produces a structured report with a `health_status` field. |

### Healing Logic

The `_heal_review` function handles the following cases:

| Error Type | Action Token | Healed Text |
|------------|--------------|-------------|
| `missing_text` | `filled_with_placeholder` | `'No review text provided'` |
| `wrong_type` | `type_conversion` | Converted to string (or placeholder if empty) |
| `empty_text` | `filled_with_placeholder` | `'No review text provided.'` |
| `special_characters_only` | `replaced_special_characters` | `'[Non-text content]'` |
| `too_long` | `truncated_text` | Truncated to `MAX_TEXT_LENGTH` with `...` appended |

## Health Report

Each run produces a health report containing:

- `pipeline`: Name of the pipeline.
- `timestamp`: ISO timestamp.
- `health_status`: One of `HEALTHY`, `WARNING`, `DEGRADED`, `CRITICAL`.
- `run_info`: Input file, batch size, offset.
- `metrics`: `total_processed`, `success_rate`, `healing_rate`, `degradation_rate`.
- `sentiment_distribution`: Counts of `POSITIVE`, `NEGATIVE`, `NEUTRAL`.
- `healing_summary`: Counts of each healing action.
- `average_confidence`: Average confidence score across all predictions.

### Health Status Logic

| Status | Condition |
|--------|-----------|
| `CRITICAL` | Degraded records are more than 10% of the total. |
| `DEGRADED` | Any degraded records. |
| `WARNING` | Healed records are more than 50% of the total. |
| `HEALTHY` | Otherwise. |

## Docker Setup

The `airflow/docker-compose.yaml` defines the following services:

| Service | Image / Role | Port |
|---------|--------------|------|
| `postgres` | PostgreSQL 13, Airflow metadata DB | 5432 |
| `redis` | Redis 7.2, Celery broker | 6379 |
| `airflow-init` | Initializes DB and creates admin user | n/a |
| `airflow-api-server` | Airflow API server (UI) | 8080 |
| `airflow-dag-processor` | Parses DAG files | n/a |
| `airflow-scheduler` | Schedules DAG runs | n/a |
| `airflow-worker` | Executes tasks via CeleryExecutor | n/a |

The compose file mounts the local `dags`, `input`, `output`, `airflow/logs`, and `airflow/plugins` directories into the containers. It also sets `OLLAMA_HOST` to `http://host.docker.internal:11434` so containers can reach Ollama running on the host.

## Dependencies

The project's Python dependencies are listed in `requirements.txt`:

```text
apache-airflow>=3.0.6
apache-airflow-providers-fab>=3.0.0

# ML NLP
transformers
torch
ollama>=0.6.0

psycopg2-binary>=2.9.0

pytest
```

The `pyproject.toml` specifies the project metadata and Python version requirement.

## Testing

The project includes `pytest` as a dependency. To run tests (if present), use:

```bash
pytest
```

> **Note:** A test suite is not currently included in the repository. Adding unit tests for the healing functions and integration tests for the DAG is a recommended next step.



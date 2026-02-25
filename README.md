# Scraper

A multi-purpose data extraction toolkit for web scraping, PDF parsing, and Excel processing — with built-in caching, rate limiting, and GSTIN enrichment.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager

## Quick Start

```bash
# Clone & install
git clone <repo-url>
cd scraper
uv sync

# Copy environment config
cp .env.example .env

# Run the CLI
uv run python -m src.main --source dggca --input data/sample.pdf --output output.csv
uv run python -m src.main --source gst --input data/gstins.csv --output output.csv

# Run fill scripts
uv run python -m src.scripts.fill_rapl
uv run python -m src.scripts.fill_sw
```

## Project Structure

```
scraper/
├── src/
│   ├── config.py               # Centralized settings (env-overridable)
│   ├── main.py                 # CLI entry point
│   ├── core/
│   │   ├── interfaces.py       # IDataExtractor ABC
│   │   ├── data_saver.py       # DataSaverMixin (CSV/JSON)
│   │   ├── base_scraper.py     # Web scraper base class
│   │   ├── base_pdf.py         # PDF extractor base class
│   │   ├── db.py               # GenericDatabase (SQLAlchemy)
│   │   └── services/
│   │       ├── pdf_service.py  # PDF text extraction (PyMuPDF)
│   │       ├── mcq_service.py  # MCQ extraction base
│   │       └── kiran/          # Kiran-style MCQ extractor
│   ├── services/
│   │   ├── gst_data_service.py # GST API client with caching
│   │   └── rate_limiter.py     # Adaptive rate limiter
│   ├── recipes/                # Domain-specific extractors
│   │   ├── dggca_recipe.py     # DGGCA PDF parser
│   │   ├── gst_recipe.py       # GST web scraper
│   │   ├── agriculture/        # Krushna PYQ parser
│   │   ├── arihant/            # Arihant index parser
│   │   ├── exambot/            # ExamBot quiz scraper
│   │   ├── insights/           # InsightsOnIndia scrapers
│   │   ├── iasscore/           # IAS Score micro-topics
│   │   └── vision/
│   ├── scripts/
│   │   ├── gst_fill_pipeline.py    # Shared GST fill logic
│   │   ├── fill_rapl.py            # Rapl Excel filler
│   │   ├── fill_sw.py              # S_w multi-sheet filler
│   │   └── deduplicate_excel.py    # Excel dedup utility
│   └── utils/
├── archive/                    # Old diagnostic scripts
├── data/                       # Data files (gitignored)
├── .env.example                # Environment variable reference
├── pyproject.toml
└── uv.lock
```

## Architecture

### Core Design

The project follows **SOLID principles** with a clean class hierarchy:

```
IDataExtractor (ABC)
├── BaseScraper + DataSaverMixin    → web scraping
│   ├── GstExtractor
│   ├── MCQInsights
│   ├── ExamBot
│   └── ...
└── BasePDFExtractor + DataSaverMixin → PDF parsing
    └── DggcaExtractor
```

### Key Patterns

| Pattern | Where | Purpose |
|---------|-------|---------|
| **Template Method** | `BaseScraper.extract()` | fetch → parse pipeline |
| **Mixin** | `DataSaverMixin` | shared save logic |
| **Strategy** | Recipe subclasses | domain-specific parsing |
| **Service** | `GstDataService` | thread-safe caching + API calls |
| **Pipeline** | `GstFillPipeline` | orchestrates scraping + filling |

## Environment Variables

All settings in `src/config.py` can be overridden via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SCRAPER_DATA_DIR` | `data` | Data directory path |
| `GST_BASE_URL` | `https://gst.jamku.app/gstin` | GST API endpoint |
| `MAX_WORKERS` | `3` | Parallel scraping threads |
| `BATCH_SIZE` | `10` | Checkpoint save frequency |
| `RETRY_ATTEMPTS` | `5` | HTTP retry attempts |
| `RETRY_MIN_WAIT` | `5` | Min retry wait (seconds) |
| `RETRY_MAX_WAIT` | `30` | Max retry wait (seconds) |
| `REQUEST_DELAY_MIN` | `2.0` | Min request delay |
| `REQUEST_DELAY_MAX` | `5.0` | Max request delay |
| `RATE_LIMIT_BASE_DELAY` | `1.0` | Rate limiter base delay |
| `RATE_LIMIT_MAX_DELAY` | `10.0` | Rate limiter max delay |

## Usage Examples

### CLI — Extract MCQs from DGGCA PDF

```bash
uv run python -m src.main --source dggca --input data/dggca.pdf --output output.csv --pages 1-50
```

### CLI — Batch scrape GST data

```bash
uv run python -m src.main --source gst --input data/gstins.csv --output gst_data.csv
```

### Fill Scripts — Enrich Excel with GST data

```bash
# Fill the Rapl spreadsheet
uv run python -m src.scripts.fill_rapl

# Fill the multi-sheet S_w spreadsheet
uv run python -m src.scripts.fill_sw

# Retry previously failed GSTINs
uv run python -m src.scripts.fill_rapl --retry-failed
```

### Deduplicate Excel

```bash
uv run python -m src.scripts.deduplicate_excel data/input/file.xlsx
```

## Development

```bash
# Install dependencies
uv sync

# Format & lint (if ruff is installed)
uv run ruff check src/
uv run ruff format src/
```

## Caching Strategy

The fill scripts use a **multi-layer caching** approach to minimize API calls:

1. **Local Excel cache** — reads already-filled Excel files for known GSTINs
2. **JSON checkpoint cache** — persists scrape results across interrupted runs
3. **In-memory service cache** — deduplicates within a single run
4. **Adaptive rate limiting** — dynamically adjusts delays on 429 errors

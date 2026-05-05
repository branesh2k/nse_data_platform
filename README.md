# NSE Equity & Index Data Platform

A data platform that ingests daily equity and index files from NSE (National Stock Exchange of India), processes them through bronze/silver/gold layers on S3, and produces analyst-ready risk signals (volatility, drawdowns, beta, index attribution).

The architecture mirrors a vendor-drop file ingestion pattern (FactSet-style) with a shared `ExecutionContext` and `BaseJob` framework, separate ingest and transform pipelines, and Airflow orchestration.

## Status

Work in progress — see commit history for build order.

## Setup

```bash
python -m venv venv
.\venv\Scripts\activate
pip install -e ".[dev]"
```

## Project structure

See `src/nse_platform/` for the package layout.

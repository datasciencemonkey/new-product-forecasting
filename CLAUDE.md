# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

NPF Retail is a Python project that uses Databricks Connect for data processing. The project appears to be focused on retail data analysis with synthetic apparel and footwear product datasets and forecasting capabilities.

## Development Setup

This project uses `uv` for Python dependency management.

### Common Commands

```bash
# Install dependencies
uv sync

# Run Python scripts
uv run python <script_name>.py

# Run the hello script
uv run python hello.py
```

## Project Structure

- **npf-data/**: Contains CSV data files including:
  - Product data for apparel and footwear
  - 4-4-5 period forecasts
  - Monthly forecasts
- **01-load-data-to-bricks.py**: Script for loading data to Databricks (currently empty)
- **pyproject.toml**: Project configuration with Databricks Connect 16.1.0 dependency

## Technology Stack

- Python 3.12
- Databricks Connect 16.1.0 for Spark/Databricks integration
- uv for dependency management
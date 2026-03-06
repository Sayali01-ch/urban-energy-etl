# Urban Energy ETL Pipeline

An Extract, Transform, Load (ETL) pipeline for processing urban energy consumption data from various sources.

## Project Structure

```
urban-energy-etl/
├── data/                      # Data storage
│   ├── raw/                   # Raw data from sources
│   ├── processed/             # Cleaned and transformed data
│   └── output/                # Final output data
├── src/
│   ├── extractors/            # Data extraction modules
│   ├── transformers/          # Data transformation modules
│   ├── loaders/               # Data loading modules
│   ├── config.py              # Configuration management
│   └── logger.py              # Logging setup
├── tests/                     # Unit and integration tests
├── requirements.txt           # Python dependencies
├── setup.py                   # Project setup file
├── .env.example               # Environment variables template
└── main.py                    # Main ETL orchestrator
```

## Features

- Extract energy consumption data from multiple sources
- Transform and clean energy data
- Load processed data to various storage backends
- Logging and error handling
- Configuration management

## Installation

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
python main.py
```

## Configuration

Copy `.env.example` to `.env` and update configuration values:

```bash
cp .env.example .env
```

## License

MIT
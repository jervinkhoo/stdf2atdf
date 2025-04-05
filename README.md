# STDF to ATDF Converter

A high-performance tool for converting semiconductor test data from Standard Test Data Format (STDF) to ASCII Test Data Format (ATDF) with database storage capabilities.

## Features

- Convert STDF files to ATDF format
- Store test data in SQLite databases for easy querying
- Process multiple files in parallel with automatic resource optimization
- Support for different equipment manufacturers (Advantest, Teradyne, Eagle)
- Filter processing by specific record types
- Comprehensive logging

## Installation

### Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

```bash
# Create and activate a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
# Convert a single STDF file to ATDF
python -m src input.stdf --output

# Convert a single STDF file to both ATDF and SQLite database
python -m src input.stdf --output --database

# Process all STDF files in a directory
python -m src /path/to/stdf/files --output --database

# Alternatively, you can use the runner script in the project root:
python run_conversion.py input.stdf --output --database
```

### Advanced Options

```bash
# Specify number of parallel workers
python -m src input.stdf --output --workers 4

# Process only specific record types
python -m src input.stdf --output --records PIR PRR

# Use a specific equipment manufacturer preprocessor
python -m src input.stdf --output --preprocessor advantest
```

### Command Line Arguments

| Argument | Short | Description |
|----------|-------|-------------|
| `input` | | Input STDF file or directory containing STDF files |
| `--output` | `-o` | Generate ATDF output files (using input filename with .atdf extension) |
| `--database` | `-d` | Generate SQLite database files (using input filename with .db extension) |
| `--records` | `-r` | Specific record types to process |
| `--workers` | `-w` | Number of parallel workers (defaults to optimal based on system resources) |
| `--preprocessor` | `-p` | Specify the preprocessor to use (advantest, teradyne, eagle) |

## Project Structure

```
stdf2atdf/
├── src/                    # Main source code
│   ├── __main__.py         # Entry point
│   ├── cli.py              # Command-line interface
│   ├── converter.py        # Core conversion logic
│   └── core/               # Core functionality
│       ├── stdf/           # STDF format handling
│       │   ├── handler.py  # STDF record handling
│       │   ├── unpackers.py # STDF binary unpacking
│       │   └── templates.py # STDF record templates
│       ├── atdf/           # ATDF format handling
│       │   ├── handler.py  # ATDF record handling
│       │   ├── parsers.py  # ATDF parsing
│       │   ├── templates.py # ATDF record templates
│       │   └── preprocessors/ # Manufacturer-specific preprocessors
│       └── utils/          # Utility functions
│           ├── services.py # Parallel processing
│           ├── files.py    # File handling
│           ├── database.py # Database operations
│           └── setup.py    # Setup functions
├── requirements.txt        # Python dependencies
└── LICENSE                 # License information
```

## Performance

The tool automatically optimizes the number of parallel workers based on:
- Available CPU cores
- System memory
- Number of files to process

This ensures efficient processing even for large datasets while preventing system overload.

## Database Schema

When using the `--database` option, the tool creates a SQLite database with tables corresponding to STDF record types. This allows for easy querying and analysis of test data using SQL.

## Manufacturer-Specific Preprocessing

Use the `--preprocessor` option to apply manufacturer-specific preprocessing:

- `advantest`: For Advantest testers
- `teradyne`: For Teradyne testers
- `eagle`: For Eagle testers

## License

This project is licensed under the terms included in the LICENSE file.

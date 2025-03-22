# MediFinderBot Data Ingestor

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A robust ETL (Extract, Transform, Load) service for the MediFinderBot ecosystem that processes medicine inventory data from source files and loads it into the PostgreSQL database. This component is responsible for keeping the medicine availability data up-to-date.

## Features

- **Efficient Data Processing**: Handles large data files with optimized batch processing
- **Incremental Updates**: Updates existing entities rather than recreating them each run
- **Robust Error Handling**: Comprehensive validation and error recovery
- **Complete Logging**: Detailed activity logs for monitoring and troubleshooting
- **Automatic Cleanup**: Marks medicines not in the current import as out of stock
- **SSH Tunnel Support**: Connects securely to PythonAnywhere PostgreSQL databases

## Prerequisites

- Python 3.8+
- PostgreSQL database (accessible via SSH tunnel for PythonAnywhere)
- Database schema must be created using the schema in [medifinder-db](https://github.com/MediFinderBot/medifinder-db)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/MediFinderBot/medifinder-ingestor.git
cd medifinder-ingestor
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your database credentials and settings
```

## Usage

### Basic Usage

Run the ingestor with a data file:

```bash
python medifinder-ingestor.py /path/to/your/data.txt
```

### Resuming Interrupted Processing

If processing was interrupted, you can resume from a specific record:

```bash
python medifinder-ingestor.py /path/to/your/data.txt 3000
```

This will skip the first 3000 records and continue processing from there.

### Options

```
usage: medifinder-ingestor.py [-h] [--log-level {DEBUG,INFO,WARNING,ERROR}] source_file [start_index]

MediFinderBot Data Ingestion Service

positional arguments:
  source_file           Path to the source data text file
  start_index           Starting record index (for resuming interrupted processing)

optional arguments:
  -h, --help            show this help message and exit
  --log-level {DEBUG,INFO,WARNING,ERROR}
                        Set logging level (default: INFO)
```

### Scheduling

For regular updates, set up a scheduled task:

#### Using PythonAnywhere Scheduled Tasks:

1. Go to the "Tasks" tab in your PythonAnywhere dashboard
2. Add a new scheduled task with the following command:
```bash
cd /home/yourusername/medifinder-ingestor && /home/yourusername/.virtualenvs/your-env/bin/python medifinder-ingestor.py /path/to/data.txt >> /home/yourusername/medifinder-ingestor/logs/scheduled.log 2>&1
```
3. Set the schedule to your desired frequency

## Configuration

### Environment Variables

Configure your application by setting these variables in the `.env` file:

| Variable | Description | Example |
|----------|-------------|---------|
| SSH_HOST | SSH host for tunnel | ssh.pythonanywhere.com |
| SSH_USERNAME | SSH username | your_username |
| SSH_PASSWORD | SSH password | your_password |
| POSTGRES_HOSTNAME | PostgreSQL hostname | yourdb.postgres.pythonanywhere-services.com |
| POSTGRES_PORT | PostgreSQL port | 12345 |
| DB_NAME | Database name | medifinder |
| DB_USER | Database username | admin |
| DB_PASSWORD | Database password | secure_password |
| LOG_LEVEL | Logging level | INFO |

### Advanced Configuration

Additional settings can be modified in `config/settings.py`:

- `FILE_DELIMITER`: Character used to separate fields in the data file (default: `|`)
- `ENCODING`: File encoding (default: `utf-8`)
- `BATCH_SIZE`: Number of records to process in a single transaction (default: `1000`)

## Project Structure

```
medifinder-ingestor/
├── config/                 # Configuration settings
│   ├── __init__.py
│   └── settings.py
├── models/                 # Database models
│   ├── __init__.py
│   └── database.py
├── services/               # Core business logic
│   ├── __init__.py
│   ├── parser.py           # File parsing logic
│   └── etl.py              # ETL processing logic
├── utils/                  # Utility functions
│   ├── __init__.py
│   └── logger.py           # Logging configuration
├── logs/                   # Log files (created at runtime)
├── .env.example            # Example environment variables
├── medifinder-ingestor.py  # Main application
└── requirements.txt        # Python dependencies
```

## Troubleshooting

### Common Issues

#### Connection Errors

If you encounter database connection errors:

1. Verify your SSH credentials in the `.env` file
2. Check if your PythonAnywhere account has database access
3. Confirm the database hostname and port are correct
4. Ensure your IP is whitelisted if required

#### File Parsing Errors

If file parsing fails:

1. Check file encoding (should be UTF-8)
2. Verify the file delimiter matches the configuration
3. Run with `--log-level DEBUG` for detailed error information

#### "psycopg2 module not found" Error

If you get a missing module error:

```bash
pip uninstall psycopg2
pip install psycopg2-binary
```

### Logs

Logs are stored in the `logs/` directory. Check these files for detailed error information when troubleshooting:

- Each run creates a timestamped log file (`ingestor_YYYYMMDD_HHMMSS.log`)
- Log level can be set via command line or the `LOG_LEVEL` environment variable

## Development

### Running Tests

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest
```

### Code Style

This project follows PEP 8 style guidelines. To check your code:

```bash
flake8 .
```

### Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Part of the [MediFinderBot](https://github.com/MediFinderBot) ecosystem
- Developed to improve medicine accessibility in Peru

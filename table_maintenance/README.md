# Delta Table Maintenance

## Project Overview
This project is designed to provide a generic class and implementaiotn of Delta table maintenance

The project is based on the TableMaintenance Class, please see that for source code and then see maintenance_job.py for a implementation of it

workflows_job.json is a Databricks workflows job that creates a nightly maintenance job to delete old records based on a retention, pruge those deletion vectors, optimize, and finally vacuum.

## Installation
To install the necessary dependencies, run:
```bash
python -m venv .venv
pip install -r requirements.txt
```

## Testing
To run the unit / integration tests, run:
```bash
pytest test/
```

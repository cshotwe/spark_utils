import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from maintenance.delta_table_maintenance import TableMaintenance

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

@pytest.fixture
def table_maintenance(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=True), \
         patch('maintenance.delta_table_maintenance.is_table_exists', return_value=True):
        tm = TableMaintenance()
        tm.spark = spark
        tm.set_table("catalog.schema.table")
        return tm

def test_set_table_valid(table_maintenance):
    assert table_maintenance.table_name == "catalog.schema.table"

def test_set_table_invalid_name(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=False):
        tm = TableMaintenance()
        tm.spark = spark
        with pytest.raises(ValueError, match="Invalid table name: invalid.table. Expected format: catalog.schema.table"):
            tm.set_table("invalid.table")

def test_set_table_not_exists(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=True), \
         patch('maintenance.delta_table_maintenance.is_table_exists', return_value=False):
        tm = TableMaintenance()
        tm.spark = spark
        with pytest.raises(ValueError, match="Table catalog.schema.table does not exist in spark catalog"):
            tm.set_table("catalog.schema.table")

def test_optimize(table_maintenance):
    with patch('maintenance.delta_table_maintenance.create_optimize_statement', return_value="OPTIMIZE catalog.schema.table"):
        table_maintenance.spark.sql = MagicMock()
        table_maintenance.optimize()
        table_maintenance.spark.sql.assert_called_once_with("OPTIMIZE catalog.schema.table")

def test_optimize_with_where(table_maintenance):
    with patch('maintenance.delta_table_maintenance.create_optimize_statement_with_where', return_value="OPTIMIZE catalog.schema.table WHERE date > '2023-01-01'"):
        table_maintenance.spark.sql = MagicMock()
        table_maintenance.optimize(where="date > '2023-01-01'")
        table_maintenance.spark.sql.assert_called_once_with("OPTIMIZE catalog.schema.table WHERE date > '2023-01-01'")

def test_vacuum(table_maintenance):
    with patch('maintenance.delta_table_maintenance.create_vacuum_statement', return_value="VACUUM catalog.schema.table"):
        table_maintenance.spark.sql = MagicMock()
        table_maintenance.vacuum()
        table_maintenance.spark.sql.assert_called_once_with("VACUUM catalog.schema.table")

def test_vacuum_with_retention(table_maintenance):
    with patch('maintenance.delta_table_maintenance.create_vacuum_statement_with_retention', return_value="VACUUM catalog.schema.table RETAIN 168 HOURS"):
        table_maintenance.spark.sql = MagicMock()
        table_maintenance.vacuum(retention=168)
        table_maintenance.spark.sql.assert_called_once_with("VACUUM catalog.schema.table RETAIN 168 HOURS")

def test_purge(table_maintenance):
    with patch('maintenance.delta_table_maintenance.create_purge_statement', return_value="PURGE catalog.schema.table"):
        table_maintenance.spark.sql = MagicMock()
        table_maintenance.purge()
        table_maintenance.spark.sql.assert_called_once_with("PURGE catalog.schema.table")

def test_delete_purge(table_maintenance):
    table_maintenance.spark.sql = MagicMock()
    table_maintenance.purge = MagicMock()
    table_maintenance.delete_purge(where_clause="date < '2023-01-01'")
    table_maintenance.spark.sql.assert_any_call("DELETE FROM catalog.schema.table WHERE date < '2023-01-01';")

def test_delete_purge_without_where_clause(table_maintenance):
    with pytest.raises(ValueError, match="where_clause is required for delete_purge"):
        table_maintenance.delete_purge()

def test_optimize_integration(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=True), \
            patch('maintenance.delta_table_maintenance.is_table_exists', return_value=True), \
            patch('maintenance.delta_table_maintenance.create_optimize_statement', return_value="OPTIMIZE catalog.schema.table"):
        tm = TableMaintenance()
        tm.spark = spark
        tm.set_table("catalog.schema.table")
        tm.spark.sql = MagicMock()
        tm.optimize()
        tm.spark.sql.assert_called_once_with("OPTIMIZE catalog.schema.table")

def test_vacuum_integration(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=True), \
            patch('maintenance.delta_table_maintenance.is_table_exists', return_value=True), \
            patch('maintenance.delta_table_maintenance.create_vacuum_statement', return_value="VACUUM catalog.schema.table"):
        tm = TableMaintenance()
        tm.spark = spark
        tm.set_table("catalog.schema.table")
        tm.spark.sql = MagicMock()
        tm.vacuum()
        tm.spark.sql.assert_called_once_with("VACUUM catalog.schema.table")

def test_purge_integration(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=True), \
            patch('maintenance.delta_table_maintenance.is_table_exists', return_value=True), \
            patch('maintenance.delta_table_maintenance.create_purge_statement', return_value="PURGE catalog.schema.table"):
        tm = TableMaintenance()
        tm.spark = spark
        tm.set_table("catalog.schema.table")
        tm.spark.sql = MagicMock()
        tm.purge()
        tm.spark.sql.assert_called_once_with("PURGE catalog.schema.table")

def test_delete_purge_integration(spark):
    with patch('maintenance.delta_table_maintenance.is_valid_table_name', return_value=True), \
            patch('maintenance.delta_table_maintenance.is_table_exists', return_value=True):
        tm = TableMaintenance()
        tm.spark = spark
        tm.set_table("catalog.schema.table")
        tm.spark.sql = MagicMock()
        tm.purge = MagicMock()
        tm.delete_purge(where_clause="date < '2023-01-01'")
        tm.spark.sql.assert_any_call("DELETE FROM catalog.schema.table WHERE date < '2023-01-01';")

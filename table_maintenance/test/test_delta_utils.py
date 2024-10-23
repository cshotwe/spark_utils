import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from maintenance.delta_utils import (
    create_optimize_statement,
    create_optimize_statement_with_where,
    create_vacuum_statement,
    create_vacuum_statement_with_retention,
    create_purge_statement,
    create_delete_statement,
    is_valid_table_name,
    is_table_exists,
    is_table_none
)

def test_create_optimize_statement():
    assert create_optimize_statement("my_table") == "OPTIMIZE my_table;"

def test_create_optimize_statement_with_where():
    assert create_optimize_statement_with_where("my_table", "id > 100") == "OPTIMIZE my_table WHERE id > 100;"

def test_create_vacuum_statement():
    assert create_vacuum_statement("my_table") == "VACUUM my_table;"
    assert create_vacuum_statement("my_table", dry_run=True) == "VACUUM my_table DRY RUN;"

def test_create_vacuum_statement_with_retention():
    assert create_vacuum_statement_with_retention("my_table", 24) == "VACUUM my_table RETAIN 24 HOURS;"
    assert create_vacuum_statement_with_retention("my_table", 24, dry_run=True) == "VACUUM my_table RETAIN 24 HOURS DRY RUN;"

def test_create_purge_statement():
    assert create_purge_statement("my_table") == "REORG TABLE my_table APPLY (PURGE);"

def test_create_delete_statement():
    assert create_delete_statement("my_table", "mytable = 1") == "DELETE FROM my_table WHERE mytable = 1;"

def test_is_valid_table_name():
    assert is_valid_table_name("db.schema.table") is True
    assert is_valid_table_name("db.table") is False
    assert is_valid_table_name("table") is False

def test_is_table_exists(monkeypatch):
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    def mock_getTable(table_name):
        if table_name == "existing_table":
            return True
        else:
            raise AnalysisException("Table not found")

    monkeypatch.setattr(spark.catalog, "getTable", mock_getTable)

    assert is_table_exists(spark, "existing_table") is True
    assert is_table_exists(spark, "non_existing_table") is False

def test_is_table_none():
    with pytest.raises(ValueError) as err:
        is_table_none(None)
    assert "Table name is not set. Please set the table name with .set_table()" in str(err.value)

def test_integration_create_and_check_table(monkeypatch):
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    def mock_getTable(table_name):
        if table_name == "test_catalog.test_schema.integration_table":
            return True
        else:
            raise AnalysisException("Table not found")

    monkeypatch.setattr(spark.catalog, "getTable", mock_getTable)

    table_name = "test_catalog.test_schema.integration_table"
    optimize_statement = create_optimize_statement(table_name)
    vacuum_statement = create_vacuum_statement(table_name)
    purge_statement = create_purge_statement(table_name)

    assert optimize_statement == "OPTIMIZE test_catalog.test_schema.integration_table;"
    assert vacuum_statement == "VACUUM test_catalog.test_schema.integration_table;"
    assert purge_statement == "REORG TABLE test_catalog.test_schema.integration_table APPLY (PURGE);"

    assert is_table_exists(spark, table_name) is True
    assert is_valid_table_name(table_name) is True
    assert is_table_none(table_name) is None

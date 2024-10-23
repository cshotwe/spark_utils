from pyspark.sql import SparkSession

from .delta_utils import *

class TableMaintenance():
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.table_name = None
        self.options_dict = {}

    def set_table(self, table_name):
        if not is_valid_table_name(table_name):
            raise ValueError(f"Invalid table name: {table_name}. Expected format: catalog.schema.table")
        if not is_table_exists(self.spark, table_name):
            raise ValueError(f"Table {table_name} does not exist in spark catalog")
        self.table_name = table_name
     
    def optimize(self, where=None):
        is_table_none(self.table_name)
        # If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `where`
        if where is not None:
            self.spark.sql(create_optimize_statement_with_where(self.table_name, where_clause=where))
        else:
            self.spark.sql(create_optimize_statement(self.table_name))
    
    def vacuum(self, retention=None ,dry_run=False):
        is_table_none(self.table_name)
        if retention is not None:
            self.spark.sql(create_vacuum_statement_with_retention(self.table_name, retention, dry_run))
        else:
            self.spark.sql(create_vacuum_statement(self.table_name, dry_run))
    
    def purge(self):
        is_table_none(self.table_name)
        self.spark.sql(create_purge_statement(self.table_name))

    def delete_purge(self, where_clause=None):
        is_table_none(self.table_name)
        if where_clause is None:
            raise ValueError("where_clause is required for delete_purge")
        else:
            self.spark.sql(create_delete_statement(self.table_name, where_clause))
            self.purge()
            
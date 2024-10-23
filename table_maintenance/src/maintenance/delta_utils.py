from pyspark.sql.utils import AnalysisException

def create_optimize_statement(table_name):
     return f"OPTIMIZE {table_name};"

def create_optimize_statement_with_where(table_name, where_clause):
     return f"OPTIMIZE {table_name} WHERE {where_clause};"

def create_vacuum_statement(table_name, dry_run=False):
     if dry_run:
          return f"VACUUM {table_name} DRY RUN;"
     else:
          return f"VACUUM {table_name};"

def create_vacuum_statement_with_retention(table_name, retention_hours ,dry_run=False):
     if dry_run:
          return f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN;"
     else:
          return f"VACUUM {table_name} RETAIN {retention_hours} HOURS;"

def create_purge_statement(table_name):
     return f"REORG TABLE {table_name} APPLY (PURGE);"

def create_delete_statement(table_name, where_clause):
     return f"DELETE FROM {table_name} WHERE {where_clause};"

def is_valid_table_name(table_name):
     parts = table_name.split('.')
     if len(parts) != 3:
          return False
     return True

def is_table_exists(spark, table_name):
     try:
          spark.catalog.getTable(table_name)
          return True
     except AnalysisException:
          return False
     
def is_table_none(table_name):
     if table_name is None:
          raise ValueError("Table name is not set. Please set the table name with .set_table() before executing.")

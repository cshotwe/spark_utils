import json

from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import * 

STATS_SCHEMA = StructType([
    StructField("numRecords", LongType(), True),
    StructField("minValues", MapType(StringType(), StringType()), True),
    StructField("maxValues", MapType(StringType(), StringType()), True),
    StructField("nullCount", MapType(StringType(), LongType()), True)
])

class DeltaLog:
  def __init__(self):
    self.location = None
    self.latest_version = None
    self.logs_df = None
    self.stats_df = None

  def forName(self, spark, table_name):
    self.latest_version = DeltaTable.forName(spark, table_name).history().selectExpr("MAX(version) AS version").collect()[0].version
    self.location = DeltaTable.forName(spark, table_name).detail().select("location").collect()[0].location
    checkpoint_path = f"{self.location}/_delta_log/_last_checkpoint"
    # try and get the last checkpoint
    try:
      last_checkpoint = json.loads(dbutils.fs.head(checkpoint_path))
      if last_checkpoint['version'] == self.latest_version:
        self.logs_df = spark.read.json(checkpoint_path)
      # If new checkpoint is not the latest version, read latest checkpoint and all newer versions
      else:
        paths = [checkpoint_path]
        for version in range(last_checkpoint['version']+1, self.latest_version+1):
          # Delta logs are padded with 0s, so we need to pad the version number to 20
          paths.append(f"{self.location}/_delta_log/*{str(version).zfill(20)}.json")
        self.logs_df = spark.read.json(paths)  
    except:
      print("No checkpoint found, loading all logs")
    self.logs_df = spark.read.json(f"{location}/_delta_log/*.json")
    return self.logs_df
    
  def collect_stats(self):
    add_actions_df = self.logs_df.filter(col("add").isNotNull()).select("add")
    stats_df = add_actions_df.select(
    col("add.path").alias("file_path"),
    col("add.stats").alias("stats_json")
    ).filter(col("stats_json").isNotNull())
    self.stats_df = stats_df.withColumn("parsed_stats", from_json(col("stats_json"), STATS_SCHEMA))
    return self.stats_df
  
  def get_max_column_value(self, column_name):
    max_values_df = self.stats_df.select("file_path",
    explode(col("parsed_stats.maxValues")).alias("column_name", "max_value"))
    mapping = dl.get_col_metadata()
    target_column = mapping[column_name]
    max_value = max_values_df.where(max_values_df.column_name == target_column).selectExpr("MAX(max_value) as max_value")
    return max_value.collect()[0].max_value
  
  def _get_col_metadata(self):
    metadata_df = self.logs_df.filter(col("metaData").isNotNull()).select("metaData")
    latest_metadata = metadata_df.orderBy(desc("metaData.createdTime")).first()
    schema_dict = json.loads(latest_metadata['metaData']['schemaString'])
    return self._extract_column_mapping(schema_dict['fields'])
  
  def _extract_column_mapping(self, schema_fields):
    column_id_name_map = {}
    for field in schema_fields:
        name = field['name']
        if 'metadata' in field and 'delta.columnMapping.id' in field['metadata']:
            col_id = field['metadata']['delta.columnMapping.physicalName']
            column_id_name_map[name] = col_id
        # Handle nested fields (structs, arrays, etc.)
        if 'type' in field and isinstance(field['type'], dict):
            if field['type']['type'] == 'struct':
                nested_map = extract_column_mapping(field['type']['fields'])
                column_id_name_map.update(nested_map)
    return column_id_name_map
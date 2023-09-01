
from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils


class GDLDLATIngest:
    def __init__(self):
        self.spark = DatabricksSession.builder.getOrCreate()
        self.dbutils = DBUtils(self.spark)
        

    def ingest_tables(self, run_id, target_catalog, landing_path, partition_columns=[]):
        databases_paths = self._get_list_of_databases(run_id)
        for database_path in databases_paths:
            table_paths = self._get_list_of_tables(database_path)
            for table_path in table_paths:
                print(table_path)
                table_name = table_path.split("/")[-2]
                database_name = table_path.split("/")[-3]
                
                print("Ingesting table: " + table_name, "from database: " + database_name)
                self._ingest_table(table_path, table_name, database_name)
                
    def _ingest_table(self, table_path, table_name, database_name):
        overwrite_table_df = self.spark.read.parquet(table_path)
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        overwrite_table_df.write.partitionBy().mode("overwrite").saveAsTable(f"{self._target_catalog}.{database_name}.{table_name}")
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        
                    
                
    def _get_list_of_tables(self, database_path):
        # Get list of tables to ingest
        path_column = "path"
        tables_df = self.spark.sql(f"LIST '{database_path}'")
        
        return  list(tables_df.select(path_column).toPandas()[path_column])
    
    def _get_list_of_databases(self, run_id):
        # Get list of databases to ingest
        path_column = "path"
        databases_df = self.spark.sql(f"LIST '{self._landing_gdl_path}/run_id={run_id}'")
        
        return  list(databases_df.select(path_column).toPandas()[path_column])
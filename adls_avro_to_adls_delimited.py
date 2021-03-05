# expected configuration from as arguments
container_name = dbutils.widgets.get('container_name')
datalake_name = dbutils.widgets.get("datalake_name")
path = dbutils.widgets.get("path")
access_key = dbutils.widgets.get("access_key")
delimiter = dbutils.widgets.get("delimiter")
is_drop_create = eval(dbutils.widgets.get("is_drop_create"))
select_sql = dbutils.widgets.get("select_sql")
source_name = dbutils.widgets.get("source_name")
source_path = dbutils.widgets.get("source_path")

destination_path = "abfss://{}@{}.dfs.core.windows.net/{}".format(container_name, datalake_name, path)
temp_path = "abfss://{}@{}.dfs.core.windows.net/temp_{}".format(container_name, datalake_name, path)

# configuring destination adls
config_key = "fs.azure.account.key.{}.dfs.core.windows.net".format(datalake_name)
spark.conf.set(config_key, access_key)

# removing existing data
if is_drop_create:
    dbutils.fs.rm(destination_path, True)

# reading data from avro file
df = spark.read.format("avro").load(source_path)
df.createOrReplaceTempView(source_name)
output_data = spark.sql(select_sql)
# writing to temporary adls path
output_data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("sep", delimiter).save(
    temp_path)

file_list = dbutils.fs.ls(temp_path)
temp_file_path = None
for file in file_list:
    if file.name.startswith('part-'):
        temp_file_path = file.path

# moving data to destination_path
if temp_file_path is not None:
    dbutils.fs.mv(temp_file_path, destination_path)
    dbutils.fs.rm(temp_path, True)

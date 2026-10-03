# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Ingesta MoreInfo.csv → bronze.more_info
# MAGIC Duración, presupuesto e ingresos en formato texto (por ejemplo "$25,000,000" y "2h 22 min").
# MAGIC
# MAGIC Lee el CSV desde la capa **raw** (ADLS, vía external location con Managed Identity), aplica un esquema explícito y agrega columnas de auditoría.

# COMMAND ----------

dbutils.widgets.text("entorno", "dev", "Entorno (dev/prod)")
dbutils.widgets.text("storage_account", "adlspeliculas2003", "Storage account")

entorno = dbutils.widgets.get("entorno").strip().lower()
storage_account = dbutils.widgets.get("storage_account").strip()

if entorno not in ("dev", "prod"):
    raise ValueError(f"Entorno no válido: {entorno}")

catalogo = f"peliculas_{entorno}"
ruta_raw = f"abfss://raw@{storage_account}.dfs.core.windows.net/peliculas"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType

esquema = StructType([
    StructField("id", IntegerType(), True),
    StructField("runtime", StringType(), True),
    StructField("budget", StringType(), True),
    StructField("revenue", StringType(), True),
    StructField("film_id", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract

# COMMAND ----------

df_raw = (
    spark.read.format("csv")
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')  # el archivo escapa comillas internas como ""
    .option("mode", "PERMISSIVE")
    .schema(esquema)
    .load(f"{ruta_raw}/MoreInfo.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Columnas de auditoría y validación

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("fecha_ingesta", F.current_timestamp())
    .withColumn("archivo_origen", F.col("_metadata.file_path"))
    .select("id", "runtime", "budget", "revenue", "film_id", "fecha_ingesta", "archivo_origen")
)

total_filas = df_bronze.count()
ids_nulos = df_bronze.filter(F.col("id").isNull()).count()

print(f"Filas leídas: {total_filas} | ids nulos: {ids_nulos}")
if total_filas == 0:
    raise ValueError("El archivo MoreInfo.csv no tiene filas")
if ids_nulos > 0:
    raise ValueError(f"Hay {ids_nulos} filas con id nulo en MoreInfo.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load en bronze

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalogo}.bronze.more_info")
)

dbutils.jobs.taskValues.set(key="filas_more_info", value=total_filas)
display(spark.table(f"{catalogo}.bronze.more_info").limit(10))

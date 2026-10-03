# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Load: silver → golden
# MAGIC Tablas agregadas listas para el dashboard:
# MAGIC
# MAGIC | Tabla | Contenido |
# MAGIC |---|---|
# MAGIC | `golden.kpi_genero` | Películas, puntuación, votos, presupuesto, ingresos y ganancia por género, con ranking de ingresos |
# MAGIC | `golden.kpi_anual` | Evolución por año de estreno |
# MAGIC | `golden.top_directores` | Ranking de directores con 3 o más películas |
# MAGIC | `golden.top_peliculas_genero` | Top 10 de películas por género (mínimo 1.000 votos) |

# COMMAND ----------

dbutils.widgets.text("entorno", "dev", "Entorno (dev/prod)")
dbutils.widgets.text("storage_account", "adlspeliculas2003", "Storage account")

entorno = dbutils.widgets.get("entorno").strip().lower()
storage_account = dbutils.widgets.get("storage_account").strip()

if entorno not in ("dev", "prod"):
    raise ValueError(f"Entorno no válido: {entorno}")

catalogo = f"peliculas_{entorno}"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

peliculas = spark.table(f"{catalogo}.silver.peliculas")
peliculas_genero = spark.table(f"{catalogo}.silver.peliculas_genero")
fecha_proceso = F.current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. KPIs por género

# COMMAND ----------

kpi_genero = (
    peliculas_genero
    .groupBy("genero")
    .agg(
        F.count("*").alias("total_peliculas"),
        F.round(F.avg("puntuacion"), 2).alias("puntuacion_promedio"),
        F.sum("votos").alias("total_votos"),
        F.round(F.avg("presupuesto_usd"), 0).alias("presupuesto_promedio_usd"),
        F.sum("ingresos_usd").alias("ingresos_totales_usd"),
        F.sum("ganancia_usd").alias("ganancia_total_usd"),
    )
    .withColumn(
        "ranking_ingresos",
        F.dense_rank().over(Window.orderBy(F.desc_nulls_last("ingresos_totales_usd"))),
    )
    .select(
        F.col("genero").cast("string"),
        F.col("total_peliculas").cast("bigint"),
        F.col("puntuacion_promedio").cast("double"),
        F.col("total_votos").cast("bigint"),
        F.col("presupuesto_promedio_usd").cast("double"),
        F.col("ingresos_totales_usd").cast("bigint"),
        F.col("ganancia_total_usd").cast("bigint"),
        F.col("ranking_ingresos").cast("int"),
        fecha_proceso.alias("fecha_proceso"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. KPIs por año de estreno

# COMMAND ----------

kpi_anual = (
    peliculas
    .filter(F.col("anio_estreno").isNotNull())
    .groupBy("anio_estreno", "decada")
    .agg(
        F.count("*").alias("total_peliculas"),
        F.round(F.avg("puntuacion"), 2).alias("puntuacion_promedio"),
        F.round(F.avg("duracion_min"), 1).alias("duracion_promedio_min"),
        F.sum("ingresos_usd").alias("ingresos_totales_usd"),
        F.sum("ganancia_usd").alias("ganancia_total_usd"),
    )
    .select(
        F.col("anio_estreno").cast("int"),
        F.col("decada").cast("int"),
        F.col("total_peliculas").cast("bigint"),
        F.col("puntuacion_promedio").cast("double"),
        F.col("duracion_promedio_min").cast("double"),
        F.col("ingresos_totales_usd").cast("bigint"),
        F.col("ganancia_total_usd").cast("bigint"),
        fecha_proceso.alias("fecha_proceso"),
    )
    .orderBy("anio_estreno")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ranking de directores

# COMMAND ----------

ventana_directores = Window.orderBy(F.desc("puntuacion_promedio"), F.desc("total_votos"))

top_directores = (
    peliculas
    .filter(F.col("director").isNotNull())
    .groupBy("director")
    .agg(
        F.count("*").alias("total_peliculas"),
        F.round(F.avg("puntuacion"), 2).alias("puntuacion_promedio"),
        F.sum("votos").alias("total_votos"),
        F.sum("ingresos_usd").alias("ingresos_totales_usd"),
    )
    .filter(F.col("total_peliculas") >= 3)
    .withColumn("ranking", F.row_number().over(ventana_directores))
    .select(
        F.col("ranking").cast("int"),
        F.col("director").cast("string"),
        F.col("total_peliculas").cast("bigint"),
        F.col("puntuacion_promedio").cast("double"),
        F.col("total_votos").cast("bigint"),
        F.col("ingresos_totales_usd").cast("bigint"),
        fecha_proceso.alias("fecha_proceso"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Top 10 de películas por género

# COMMAND ----------

ventana_genero = Window.partitionBy("genero").orderBy(F.desc("puntuacion"), F.desc("votos"))

top_peliculas_genero = (
    peliculas_genero
    .filter(F.col("votos") >= 1000)
    .withColumn("ranking", F.row_number().over(ventana_genero))
    .filter(F.col("ranking") <= 10)
    .join(peliculas.select("id", "poster_url"), on="id", how="left")
    .select(
        F.col("genero").cast("string"),
        F.col("ranking").cast("int"),
        F.col("id").cast("int"),
        F.col("titulo").cast("string"),
        F.col("puntuacion").cast("double"),
        F.col("votos").cast("int"),
        F.col("anio_estreno").cast("int"),
        F.col("poster_url").cast("string"),
        fecha_proceso.alias("fecha_proceso"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load en golden

# COMMAND ----------

tablas_golden = {
    "kpi_genero": kpi_genero,
    "kpi_anual": kpi_anual,
    "top_directores": top_directores,
    "top_peliculas_genero": top_peliculas_genero,
}

for nombre, df in tablas_golden.items():
    df.write.format("delta").mode("overwrite").saveAsTable(f"{catalogo}.golden.{nombre}")
    filas = spark.table(f"{catalogo}.golden.{nombre}").count()
    if filas == 0:
        raise ValueError(f"golden.{nombre} quedó vacía")
    print(f"golden.{nombre}: {filas} filas")

# COMMAND ----------

display(spark.table(f"{catalogo}.golden.kpi_genero").orderBy("ranking_ingresos"))

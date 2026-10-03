# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Transform: bronze → silver
# MAGIC Limpieza, tipado, deduplicación, enriquecimiento y unión de las cuatro tablas bronze.
# MAGIC
# MAGIC | Problema detectado en los datos | Tratamiento |
# MAGIC |---|---|
# MAGIC | 163 películas repetidas con distinto `id` | Deduplicación con `Window` + `row_number` |
# MAGIC | ~30 % de presupuesto e ingresos vacíos | `coalesce` entre FilmDetails y MoreInfo; 0 se trata como desconocido |
# MAGIC | Montos y duración como texto en MoreInfo | `regexp_replace` / `regexp_extract` + conversión segura |
# MAGIC | Géneros como lista en un solo texto | `split` + `explode` en `silver.peliculas_genero` |

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


def a_entero_seguro(columna):
    """Quita todo lo que no sea dígito ('$25,000,000' -> 25000000). Devuelve null si no queda un número."""
    limpio = F.regexp_replace(F.col(columna), r"[^0-9]", "")
    return F.when(limpio.rlike(r"^[0-9]+$"), limpio.cast("bigint"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura de bronze

# COMMAND ----------

bronze_movies = spark.table(f"{catalogo}.bronze.movies")
bronze_detalles = spark.table(f"{catalogo}.bronze.film_details")
bronze_info = spark.table(f"{catalogo}.bronze.more_info")
bronze_posters = spark.table(f"{catalogo}.bronze.poster_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpieza de cada fuente

# COMMAND ----------

movies = (
    bronze_movies
    .filter(F.col("id").isNotNull())
    .select(
        F.col("id").cast("int").alias("id"),
        F.trim("title").alias("titulo"),
        F.regexp_replace(F.trim("genres"), r"\s*,\s*", ", ").alias("generos"),
        F.lower(F.trim("language")).alias("idioma"),
        F.col("user_score").cast("double").alias("puntuacion"),
        F.col("vote_count").cast("int").alias("votos"),
        (F.col("runtime_hour") * 60 + F.col("runtime_min")).cast("int").alias("duracion_min"),
        F.when(
            F.col("release_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date("release_date", "yyyy-MM-dd"),
        ).alias("fecha_estreno"),
    )
)

detalles = bronze_detalles.select(
    F.col("id").cast("int").alias("id"),
    F.trim("director").alias("director"),
    F.trim("top_billed").alias("reparto"),
    F.trim(F.split(F.col("top_billed"), ",").getItem(0)).alias("actor_principal"),
    F.col("budget_usd").cast("bigint").alias("presupuesto_detalle"),
    F.col("revenue_usd").cast("bigint").alias("ingresos_detalle"),
)

horas_texto = F.regexp_extract("runtime", r"(\d+)\s*h", 1)
minutos_texto = F.regexp_extract("runtime", r"(\d+)\s*min", 1)

info = bronze_info.select(
    F.col("film_id").cast("int").alias("id"),
    a_entero_seguro("budget").alias("presupuesto_texto"),
    a_entero_seguro("revenue").alias("ingresos_texto"),
    F.when(
        (horas_texto != "") & (minutos_texto != ""),
        horas_texto.cast("int") * 60 + minutos_texto.cast("int"),
    ).alias("duracion_texto_min"),
)

posters = bronze_posters.select(
    F.col("id").cast("int").alias("id"),
    F.col("poster_path").alias("poster_url"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplicación
# MAGIC Una película se considera repetida si coincide en título, fecha de estreno y duración. Se conserva el `id` más bajo.

# COMMAND ----------

ventana_duplicados = Window.partitionBy("titulo", "fecha_estreno", "duracion_min").orderBy("id")

movies_unicas = (
    movies
    .withColumn("orden", F.row_number().over(ventana_duplicados))
    .filter(F.col("orden") == 1)
    .drop("orden")
)

duplicados_eliminados = movies.count() - movies_unicas.count()
print(f"Películas duplicadas eliminadas: {duplicados_eliminados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Unión y enriquecimiento

# COMMAND ----------

peliculas = (
    movies_unicas
    .join(detalles, on="id", how="left")
    .join(info, on="id", how="left")
    .join(F.broadcast(posters), on="id", how="left")
    # Completar montos faltantes con la otra fuente; 0 o negativo = desconocido
    .withColumn("presupuesto_usd", F.coalesce("presupuesto_detalle", "presupuesto_texto"))
    .withColumn("ingresos_usd", F.coalesce("ingresos_detalle", "ingresos_texto"))
    .withColumn("presupuesto_usd", F.when(F.col("presupuesto_usd") > 0, F.col("presupuesto_usd")))
    .withColumn("ingresos_usd", F.when(F.col("ingresos_usd") > 0, F.col("ingresos_usd")))
    .withColumn("ganancia_usd", F.col("ingresos_usd") - F.col("presupuesto_usd"))
    .withColumn(
        "roi",
        F.when(
            F.col("presupuesto_usd") > 0,
            F.round(F.col("ganancia_usd") / F.col("presupuesto_usd"), 2),
        ),
    )
    # Columnas derivadas
    .withColumn("anio_estreno", F.year("fecha_estreno"))
    .withColumn("decada", (F.floor(F.col("anio_estreno") / 10) * 10).cast("int"))
    .withColumn("genero_principal", F.trim(F.split(F.col("generos"), ",").getItem(0)))
    .withColumn(
        "categoria_duracion",
        F.when(F.col("duracion_min") < 90, "Corta")
        .when(F.col("duracion_min") <= 150, "Media")
        .otherwise("Larga"),
    )
    .withColumn(
        "categoria_puntuacion",
        F.when(F.col("puntuacion") >= 8, "Excelente")
        .when(F.col("puntuacion") >= 7, "Buena")
        .when(F.col("puntuacion") >= 5, "Regular")
        .otherwise("Baja"),
    )
    # Control de calidad: la duración de Movies coincide con la de MoreInfo
    .withColumn("duracion_consistente", F.col("duracion_min") == F.col("duracion_texto_min"))
    .withColumn("fecha_proceso", F.current_timestamp())
    .select(
        F.col("id").cast("int"),
        F.col("titulo").cast("string"),
        F.col("generos").cast("string"),
        F.col("genero_principal").cast("string"),
        F.col("idioma").cast("string"),
        F.col("puntuacion").cast("double"),
        F.col("categoria_puntuacion").cast("string"),
        F.col("votos").cast("int"),
        F.col("duracion_min").cast("int"),
        F.col("categoria_duracion").cast("string"),
        F.col("duracion_consistente").cast("boolean"),
        F.col("fecha_estreno").cast("date"),
        F.col("anio_estreno").cast("int"),
        F.col("decada").cast("int"),
        F.col("director").cast("string"),
        F.col("actor_principal").cast("string"),
        F.col("reparto").cast("string"),
        F.col("presupuesto_usd").cast("bigint"),
        F.col("ingresos_usd").cast("bigint"),
        F.col("ganancia_usd").cast("bigint"),
        F.col("roi").cast("double"),
        F.col("poster_url").cast("string"),
        F.col("fecha_proceso").cast("timestamp"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Géneros en filas (una fila por película y género)

# COMMAND ----------

peliculas_genero = (
    peliculas
    .withColumn("genero", F.explode(F.split(F.col("generos"), ",")))
    .withColumn("genero", F.trim("genero"))
    .filter(F.col("genero") != "")
    .select(
        F.col("id").cast("int"),
        F.col("titulo").cast("string"),
        F.col("genero").cast("string"),
        F.col("puntuacion").cast("double"),
        F.col("votos").cast("int"),
        F.col("anio_estreno").cast("int"),
        F.col("presupuesto_usd").cast("bigint"),
        F.col("ingresos_usd").cast("bigint"),
        F.col("ganancia_usd").cast("bigint"),
        F.col("fecha_proceso").cast("timestamp"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validaciones de calidad

# COMMAND ----------

total_peliculas = peliculas.count()
ids_repetidos = total_peliculas - peliculas.select("id").distinct().count()
sin_fecha = peliculas.filter(F.col("fecha_estreno").isNull()).count()
inconsistentes = peliculas.filter(~F.col("duracion_consistente")).count()

print(f"Películas en silver: {total_peliculas}")
print(f"Películas sin fecha de estreno: {sin_fecha}")
print(f"Duraciones inconsistentes entre fuentes: {inconsistentes}")

if total_peliculas == 0:
    raise ValueError("silver.peliculas quedó vacía")
if ids_repetidos > 0:
    raise ValueError(f"Hay {ids_repetidos} ids repetidos en silver.peliculas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Load en silver

# COMMAND ----------

peliculas.write.format("delta").mode("overwrite").saveAsTable(f"{catalogo}.silver.peliculas")
peliculas_genero.write.format("delta").mode("overwrite").saveAsTable(f"{catalogo}.silver.peliculas_genero")

dbutils.jobs.taskValues.set(key="filas_silver_peliculas", value=total_peliculas)
display(spark.table(f"{catalogo}.silver.peliculas").limit(10))

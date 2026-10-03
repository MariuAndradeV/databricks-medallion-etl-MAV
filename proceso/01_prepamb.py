# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Preparación de ambiente
# MAGIC Crea las external locations, el catálogo del entorno, los esquemas de la arquitectura medallion (bronze, silver, golden) y las tablas Delta.
# MAGIC
# MAGIC - El acceso al Data Lake se hace solo con **Managed Identity** (Access Connector) mediante la storage credential `sc_peliculas`.
# MAGIC - El mismo notebook sirve para dev y prod: el parámetro `entorno` define el catálogo (`peliculas_dev` o `peliculas_prod`).

# COMMAND ----------

dbutils.widgets.text("entorno", "dev", "Entorno (dev/prod)")
dbutils.widgets.text("storage_account", "adlspeliculas2003", "Storage account")
dbutils.widgets.text("storage_credential", "sc_peliculas", "Storage credential")

entorno = dbutils.widgets.get("entorno").strip().lower()
storage_account = dbutils.widgets.get("storage_account").strip()
credencial = dbutils.widgets.get("storage_credential").strip()

if entorno not in ("dev", "prod"):
    raise ValueError(f"Entorno no válido: {entorno}")

catalogo = f"peliculas_{entorno}"


def ruta(contenedor, subruta=""):
    return f"abfss://{contenedor}@{storage_account}.dfs.core.windows.net/{subruta}"


print(f"Entorno: {entorno} | Catálogo: {catalogo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. External locations
# MAGIC Una por contenedor del Data Lake. Son objetos del metastore, compartidos por los workspaces dev y prod.

# COMMAND ----------

for contenedor in ["raw", "bronze", "silver", "golden", "catalogo"]:
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS el_{contenedor}
        URL '{ruta(contenedor)}'
        WITH (STORAGE CREDENTIAL {credencial})
        COMMENT 'Contenedor {contenedor} del proyecto peliculas'
    """)
    print(f"External location lista: el_{contenedor}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catálogo y esquemas
# MAGIC Cada entorno guarda sus datos en una subcarpeta propia de cada contenedor (`/dev` o `/prod`).

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalogo}
    MANAGED LOCATION '{ruta('catalogo', entorno)}'
    COMMENT 'Proyecto medallion de peliculas - entorno {entorno}'
""")

for capa in ["bronze", "silver", "golden"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {catalogo}.{capa}
        MANAGED LOCATION '{ruta(capa, entorno)}'
        COMMENT 'Capa {capa} de la arquitectura medallion'
    """)
    print(f"Esquema listo: {catalogo}.{capa}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tablas Delta

# COMMAND ----------

AUDITORIA = "fecha_ingesta TIMESTAMP, archivo_origen STRING"

TABLAS = {
    # Bronze: datos tal como vienen de raw + columnas de auditoría
    "bronze.movies": (
        "id INT, title STRING, genres STRING, language STRING, user_score DOUBLE, "
        f"runtime_hour INT, runtime_min INT, release_date STRING, vote_count INT, {AUDITORIA}"
    ),
    "bronze.film_details": (
        f"id INT, director STRING, top_billed STRING, budget_usd BIGINT, revenue_usd BIGINT, {AUDITORIA}"
    ),
    "bronze.more_info": (
        f"id INT, runtime STRING, budget STRING, revenue STRING, film_id INT, {AUDITORIA}"
    ),
    "bronze.poster_path": (
        f"id INT, poster_path STRING, backdrop_path STRING, {AUDITORIA}"
    ),
    # Silver: datos limpios, tipados, deduplicados y unidos
    "silver.peliculas": (
        "id INT, titulo STRING, generos STRING, genero_principal STRING, idioma STRING, "
        "puntuacion DOUBLE, categoria_puntuacion STRING, votos INT, duracion_min INT, "
        "categoria_duracion STRING, duracion_consistente BOOLEAN, fecha_estreno DATE, "
        "anio_estreno INT, decada INT, director STRING, actor_principal STRING, reparto STRING, "
        "presupuesto_usd BIGINT, ingresos_usd BIGINT, ganancia_usd BIGINT, roi DOUBLE, "
        "poster_url STRING, fecha_proceso TIMESTAMP"
    ),
    "silver.peliculas_genero": (
        "id INT, titulo STRING, genero STRING, puntuacion DOUBLE, votos INT, anio_estreno INT, "
        "presupuesto_usd BIGINT, ingresos_usd BIGINT, ganancia_usd BIGINT, fecha_proceso TIMESTAMP"
    ),
    # Golden: tablas agregadas para el dashboard
    "golden.kpi_genero": (
        "genero STRING, total_peliculas BIGINT, puntuacion_promedio DOUBLE, total_votos BIGINT, "
        "presupuesto_promedio_usd DOUBLE, ingresos_totales_usd BIGINT, ganancia_total_usd BIGINT, "
        "ranking_ingresos INT, fecha_proceso TIMESTAMP"
    ),
    "golden.kpi_anual": (
        "anio_estreno INT, decada INT, total_peliculas BIGINT, puntuacion_promedio DOUBLE, "
        "duracion_promedio_min DOUBLE, ingresos_totales_usd BIGINT, ganancia_total_usd BIGINT, "
        "fecha_proceso TIMESTAMP"
    ),
    "golden.top_directores": (
        "ranking INT, director STRING, total_peliculas BIGINT, puntuacion_promedio DOUBLE, "
        "total_votos BIGINT, ingresos_totales_usd BIGINT, fecha_proceso TIMESTAMP"
    ),
    "golden.top_peliculas_genero": (
        "genero STRING, ranking INT, id INT, titulo STRING, puntuacion DOUBLE, votos INT, "
        "anio_estreno INT, poster_url STRING, fecha_proceso TIMESTAMP"
    ),
}

for nombre, columnas in TABLAS.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {catalogo}.{nombre} ({columnas}) USING DELTA")
    print(f"Tabla lista: {catalogo}.{nombre}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación

# COMMAND ----------

for capa in ["bronze", "silver", "golden"]:
    display(spark.sql(f"SHOW TABLES IN {catalogo}.{capa}"))

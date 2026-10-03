# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión del entorno
# MAGIC Elimina las tablas lógicas (catálogo, esquemas y tablas) y las rutas físicas del entorno en el Data Lake.
# MAGIC **No borra la capa raw.** Para confirmar, escribe `SI` en el parámetro `confirmar`.

# COMMAND ----------

dbutils.widgets.text("entorno", "dev", "Entorno (dev/prod)")
dbutils.widgets.text("storage_account", "adlspeliculas2003", "Storage account")
dbutils.widgets.text("confirmar", "NO", "Escribe SI para confirmar")

entorno = dbutils.widgets.get("entorno").strip().lower()
storage_account = dbutils.widgets.get("storage_account").strip()
confirmar = dbutils.widgets.get("confirmar").strip().upper()

if entorno not in ("dev", "prod"):
    raise ValueError(f"Entorno no válido: {entorno}")
if confirmar != "SI":
    dbutils.notebook.exit("Reversión cancelada: el parámetro 'confirmar' no es SI")

catalogo = f"peliculas_{entorno}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tablas lógicas

# COMMAND ----------

spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
print(f"Catálogo eliminado: {catalogo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rutas físicas
# MAGIC Unity Catalog también purga los archivos de tablas administradas por su cuenta, pero aquí se eliminan de forma explícita.

# COMMAND ----------

rutas = [
    f"abfss://{contenedor}@{storage_account}.dfs.core.windows.net/{entorno}"
    for contenedor in ["bronze", "silver", "golden", "catalogo"]
]

for ruta in rutas:
    try:
        dbutils.fs.rm(ruta, recurse=True)
        print(f"Ruta eliminada: {ruta}")
    except Exception as e:
        print(f"No se pudo eliminar {ruta}: {str(e)[:200]}")

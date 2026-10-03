# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Grants
# MAGIC Otorga permisos sobre el catálogo del entorno a dos grupos de cuenta:
# MAGIC
# MAGIC | Grupo | Permisos |
# MAGIC |---|---|
# MAGIC | `ingenieros_datos` | Uso del catálogo; lectura, escritura y creación de tablas en bronze, silver y golden |
# MAGIC | `analistas_peliculas` | Uso del catálogo; solo lectura en golden |
# MAGIC
# MAGIC Los grupos se crean antes en la consola de cuenta de Databricks (User management → Groups).

# COMMAND ----------

dbutils.widgets.text("entorno", "dev", "Entorno (dev/prod)")
dbutils.widgets.text("grupo_ingenieros", "ingenieros_datos", "Grupo ingenieros")
dbutils.widgets.text("grupo_analistas", "analistas_peliculas", "Grupo analistas")

entorno = dbutils.widgets.get("entorno").strip().lower()
grupo_ingenieros = dbutils.widgets.get("grupo_ingenieros").strip()
grupo_analistas = dbutils.widgets.get("grupo_analistas").strip()

if entorno not in ("dev", "prod"):
    raise ValueError(f"Entorno no válido: {entorno}")

catalogo = f"peliculas_{entorno}"

# COMMAND ----------

permisos = [
    # Ingenieros de datos: trabajan en todas las capas
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{grupo_ingenieros}`",
    *[
        f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA {catalogo}.{capa} TO `{grupo_ingenieros}`"
        for capa in ["bronze", "silver", "golden"]
    ],
    # Analistas: solo consumen la capa golden
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{grupo_analistas}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalogo}.golden TO `{grupo_analistas}`",
]

errores = []
for sentencia in permisos:
    try:
        spark.sql(sentencia)
        print(f"OK    {sentencia}")
    except Exception as e:
        errores.append(sentencia)
        print(f"ERROR {sentencia}\n      {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON CATALOG {catalogo}"))
display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.golden"))

if errores:
    raise Exception(f"{len(errores)} grants fallaron. Verifica que los grupos existan en la cuenta de Databricks.")

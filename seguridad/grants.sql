-- =====================================================================
-- SEGURIDAD - Grants del proyecto medallion de películas
-- Entorno: dev (para prod, reemplazar 'peliculas_dev' por 'peliculas_prod').
--
-- Requisito previo: crear los grupos en la consola de cuenta de Databricks
-- (accounts.azuredatabricks.net -> User management -> Groups):
--   * ingenieros_datos     -> construyen y mantienen el ETL
--   * analistas_peliculas  -> consumen la capa golden (dashboard)
-- =====================================================================

-- 1. Ingenieros de datos: acceso de lectura y escritura a todas las capas
GRANT USE CATALOG ON CATALOG peliculas_dev TO `ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA peliculas_dev.bronze TO `ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA peliculas_dev.silver TO `ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA peliculas_dev.golden TO `ingenieros_datos`;

-- Lectura de archivos de la capa raw (para depurar ingestas)
GRANT READ FILES ON EXTERNAL LOCATION el_raw TO `ingenieros_datos`;

-- 2. Analistas: solo lectura de la capa golden
GRANT USE CATALOG ON CATALOG peliculas_dev TO `analistas_peliculas`;
GRANT USE SCHEMA, SELECT ON SCHEMA peliculas_dev.golden TO `analistas_peliculas`;

-- 3. Verificación
SHOW GRANTS ON CATALOG peliculas_dev;
SHOW GRANTS ON SCHEMA peliculas_dev.golden;
SHOW GRANTS `analistas_peliculas` ON SCHEMA peliculas_dev.golden;

-- 4. Revocación (ejemplo, ejecutar solo si se necesita quitar accesos)
-- REVOKE SELECT ON SCHEMA peliculas_dev.golden FROM `analistas_peliculas`;
-- REVOKE MODIFY ON SCHEMA peliculas_dev.bronze FROM `ingenieros_datos`;

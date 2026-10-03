-- =====================================================================
-- PREPARACIÓN DE AMBIENTE - Proyecto medallion de películas
-- Ejecutar en el SQL editor o en un notebook SQL de Databricks.
-- Entorno: dev. Para prod, reemplazar 'peliculas_dev' por 'peliculas_prod'
-- y '/dev' por '/prod' en las rutas MANAGED LOCATION.
-- Requisito previo (UI): storage credential 'sc_peliculas' creada con el
-- Access Connector 'ac-peliculas' (Managed Identity).
-- =====================================================================

-- 1. External locations (una por contenedor del Data Lake)
CREATE EXTERNAL LOCATION IF NOT EXISTS el_raw
  URL 'abfss://raw@adlspeliculas2003.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL sc_peliculas)
  COMMENT 'Contenedor raw del proyecto peliculas';

CREATE EXTERNAL LOCATION IF NOT EXISTS el_bronze
  URL 'abfss://bronze@adlspeliculas2003.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL sc_peliculas)
  COMMENT 'Contenedor bronze del proyecto peliculas';

CREATE EXTERNAL LOCATION IF NOT EXISTS el_silver
  URL 'abfss://silver@adlspeliculas2003.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL sc_peliculas)
  COMMENT 'Contenedor silver del proyecto peliculas';

CREATE EXTERNAL LOCATION IF NOT EXISTS el_golden
  URL 'abfss://golden@adlspeliculas2003.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL sc_peliculas)
  COMMENT 'Contenedor golden del proyecto peliculas';

CREATE EXTERNAL LOCATION IF NOT EXISTS el_catalogo
  URL 'abfss://catalogo@adlspeliculas2003.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL sc_peliculas)
  COMMENT 'Contenedor catalogo del proyecto peliculas';

-- 2. Catálogo
CREATE CATALOG IF NOT EXISTS peliculas_dev
  MANAGED LOCATION 'abfss://catalogo@adlspeliculas2003.dfs.core.windows.net/dev'
  COMMENT 'Proyecto medallion de peliculas - entorno dev';

-- 3. Esquemas (capas medallion)
CREATE SCHEMA IF NOT EXISTS peliculas_dev.bronze
  MANAGED LOCATION 'abfss://bronze@adlspeliculas2003.dfs.core.windows.net/dev'
  COMMENT 'Capa bronze de la arquitectura medallion';

CREATE SCHEMA IF NOT EXISTS peliculas_dev.silver
  MANAGED LOCATION 'abfss://silver@adlspeliculas2003.dfs.core.windows.net/dev'
  COMMENT 'Capa silver de la arquitectura medallion';

CREATE SCHEMA IF NOT EXISTS peliculas_dev.golden
  MANAGED LOCATION 'abfss://golden@adlspeliculas2003.dfs.core.windows.net/dev'
  COMMENT 'Capa golden de la arquitectura medallion';

-- 4. Tablas Delta
CREATE TABLE IF NOT EXISTS peliculas_dev.bronze.movies (
  id INT,
  title STRING,
  genres STRING,
  language STRING,
  user_score DOUBLE,
  runtime_hour INT,
  runtime_min INT,
  release_date STRING,
  vote_count INT,
  fecha_ingesta TIMESTAMP,
  archivo_origen STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.bronze.film_details (
  id INT,
  director STRING,
  top_billed STRING,
  budget_usd BIGINT,
  revenue_usd BIGINT,
  fecha_ingesta TIMESTAMP,
  archivo_origen STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.bronze.more_info (
  id INT,
  runtime STRING,
  budget STRING,
  revenue STRING,
  film_id INT,
  fecha_ingesta TIMESTAMP,
  archivo_origen STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.bronze.poster_path (
  id INT,
  poster_path STRING,
  backdrop_path STRING,
  fecha_ingesta TIMESTAMP,
  archivo_origen STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.silver.peliculas (
  id INT,
  titulo STRING,
  generos STRING,
  genero_principal STRING,
  idioma STRING,
  puntuacion DOUBLE,
  categoria_puntuacion STRING,
  votos INT,
  duracion_min INT,
  categoria_duracion STRING,
  duracion_consistente BOOLEAN,
  fecha_estreno DATE,
  anio_estreno INT,
  decada INT,
  director STRING,
  actor_principal STRING,
  reparto STRING,
  presupuesto_usd BIGINT,
  ingresos_usd BIGINT,
  ganancia_usd BIGINT,
  roi DOUBLE,
  poster_url STRING,
  fecha_proceso TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.silver.peliculas_genero (
  id INT,
  titulo STRING,
  genero STRING,
  puntuacion DOUBLE,
  votos INT,
  anio_estreno INT,
  presupuesto_usd BIGINT,
  ingresos_usd BIGINT,
  ganancia_usd BIGINT,
  fecha_proceso TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.golden.kpi_genero (
  genero STRING,
  total_peliculas BIGINT,
  puntuacion_promedio DOUBLE,
  total_votos BIGINT,
  presupuesto_promedio_usd DOUBLE,
  ingresos_totales_usd BIGINT,
  ganancia_total_usd BIGINT,
  ranking_ingresos INT,
  fecha_proceso TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.golden.kpi_anual (
  anio_estreno INT,
  decada INT,
  total_peliculas BIGINT,
  puntuacion_promedio DOUBLE,
  duracion_promedio_min DOUBLE,
  ingresos_totales_usd BIGINT,
  ganancia_total_usd BIGINT,
  fecha_proceso TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.golden.top_directores (
  ranking INT,
  director STRING,
  total_peliculas BIGINT,
  puntuacion_promedio DOUBLE,
  total_votos BIGINT,
  ingresos_totales_usd BIGINT,
  fecha_proceso TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS peliculas_dev.golden.top_peliculas_genero (
  genero STRING,
  ranking INT,
  id INT,
  titulo STRING,
  puntuacion DOUBLE,
  votos INT,
  anio_estreno INT,
  poster_url STRING,
  fecha_proceso TIMESTAMP
) USING DELTA;

-- 5. Verificación
SHOW TABLES IN peliculas_dev.bronze;
SHOW TABLES IN peliculas_dev.silver;
SHOW TABLES IN peliculas_dev.golden;

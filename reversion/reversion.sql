-- =====================================================================
-- REVERSIÓN - Elimina los objetos lógicos del proyecto
-- Entorno: dev (para prod, reemplazar 'peliculas_dev' por 'peliculas_prod').
-- Las rutas físicas se eliminan con reversion/reversion.py.
-- =====================================================================

-- 1. Tablas golden
DROP TABLE IF EXISTS peliculas_dev.golden.kpi_genero;
DROP TABLE IF EXISTS peliculas_dev.golden.kpi_anual;
DROP TABLE IF EXISTS peliculas_dev.golden.top_directores;
DROP TABLE IF EXISTS peliculas_dev.golden.top_peliculas_genero;

-- 2. Tablas silver
DROP TABLE IF EXISTS peliculas_dev.silver.peliculas;
DROP TABLE IF EXISTS peliculas_dev.silver.peliculas_genero;

-- 3. Tablas bronze
DROP TABLE IF EXISTS peliculas_dev.bronze.movies;
DROP TABLE IF EXISTS peliculas_dev.bronze.film_details;
DROP TABLE IF EXISTS peliculas_dev.bronze.more_info;
DROP TABLE IF EXISTS peliculas_dev.bronze.poster_path;

-- 4. Esquemas y catálogo
DROP SCHEMA IF EXISTS peliculas_dev.golden CASCADE;
DROP SCHEMA IF EXISTS peliculas_dev.silver CASCADE;
DROP SCHEMA IF EXISTS peliculas_dev.bronze CASCADE;
DROP CATALOG IF EXISTS peliculas_dev CASCADE;

-- 5. External locations (solo al desmontar TODO el proyecto:
--    son compartidas por dev y prod)
-- DROP EXTERNAL LOCATION IF EXISTS el_golden;
-- DROP EXTERNAL LOCATION IF EXISTS el_silver;
-- DROP EXTERNAL LOCATION IF EXISTS el_bronze;
-- DROP EXTERNAL LOCATION IF EXISTS el_catalogo;
-- DROP EXTERNAL LOCATION IF EXISTS el_raw;
-- La storage credential 'sc_peliculas' se elimina desde Catalog -> External data -> Credentials.

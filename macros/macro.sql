-- To check if the column is numeric
{%- macro is_numeric(dtype) -%}
    {% set is_numeric = dtype.startswith("int") or dtype.startswith("float") or "numeric" in dtype or "number" in dtype or "double" in dtype %}
    {{ is_numeric }}
{%- endmacro -%}

-- To check if the column is date/time
{%- macro is_date_or_time(dtype) -%}
    {% set is_date_or_time = dtype.startswith("timestamp") or dtype.startswith("date") or dtype.startswith("time") %}
    {{ is_date_or_time }}
{%- endmacro -%}

-- Macro to create a new schema if not exists
{%- macro create_new_schema(schema_name) -%}
 DO $$
 BEGIN
     IF NOT EXISTS (
         SELECT 1 
         FROM information_schema.schemata 
         WHERE schema_name = '{{ schema_name }}'
     ) THEN
         EXECUTE 'CREATE SCHEMA {{ schema_name }}';
     END IF;
 END
 $$;
{%- endmacro -%}

-- Create table if not exists
{%- macro create_data_profile_table(db_name, schema_name, table_name) -%}
 CREATE TABLE IF NOT EXISTS {{ db_name }}.{{ schema_name }}.{{ table_name }}(
     database                     VARCHAR(100),
     schema                       VARCHAR(100),
     table_name                   VARCHAR(100),
     column_name                  VARCHAR(500),
     data_type                    VARCHAR(100),
     row_count                    BIGINT,
     not_null_count               BIGINT,
     null_count                   BIGINT,
     not_null_percentage          NUMERIC(38,2),
     null_percentage              NUMERIC(38,2),
     distinct_count               BIGINT,
     distinct_percent             NUMERIC(38,2),
     is_unique                    BOOLEAN,
     min                          VARCHAR(250),
     max                          VARCHAR(250),
     avg                          NUMERIC(38,2),
     standard_deviation           NUMERIC(38,2),
     top_5_values                 VARCHAR(1000),
     least_5_values               VARCHAR(1000),    
     profiled_at                  TIMESTAMP
 );
{%- endmacro -%}

-- Reading the information schema based on the source database and filters provided
{%- macro read_information_schema(source_database, include_schemas, exclude_schemas, include_tables, exclude_tables) -%}
 WITH all_tables AS (
     SELECT 
         table_catalog AS database_name,
         table_schema AS schema_name,
         table_name
     FROM information_schema.tables
     WHERE table_catalog = '{{ source_database }}'
         AND table_type = 'BASE TABLE'
         {% if include_schemas | length > 0 %}
         AND table_schema IN ('{{ include_schemas | join("', '") }}')
         {% endif %}
         {% if exclude_schemas | length > 0 %}
         AND table_schema NOT IN ('{{ exclude_schemas | join("', '") }}')
         {% endif %}
         {% if include_tables | length > 0 %}
         AND table_name IN ('{{ include_tables | join("', '") }}')
         {% endif %}
         {% if exclude_tables | length > 0 %}
         AND table_name NOT IN ('{{ exclude_tables | join("', '") }}')
         {% endif %}
 )
 SELECT 
     database_name,
     schema_name,
     table_name
 FROM all_tables;
{%- endmacro -%}

{%- macro do_data_profile(information_schema_data, source_table_name, chunk_column, current_date_and_time) -%}
 SELECT 
     '{{ information_schema_data[0] }}' AS database,
     '{{ information_schema_data[1] }}' AS schema,
     '{{ information_schema_data[2] }}' AS table_name,
     '{{ chunk_column[0] }}' AS column_name,
     '{{ chunk_column[1] }}' AS data_type,
     COUNT(*) AS row_count,
     COUNT({{ adapter.quote(chunk_column[0]) }}) AS not_null_count,
     COUNT(*) - COUNT({{ adapter.quote(chunk_column[0]) }}) AS null_count,
     ROUND((COUNT({{ adapter.quote(chunk_column[0]) }})::numeric / COUNT(*)) * 100, 2) AS not_null_percentage,
     ROUND((COUNT(*) - COUNT({{ adapter.quote(chunk_column[0]) }})::numeric / COUNT(*)) * 100, 2) AS null_percentage,
     COUNT(DISTINCT {{ adapter.quote(chunk_column[0]) }}) AS distinct_count,
     ROUND((COUNT(DISTINCT {{ adapter.quote(chunk_column[0]) }})::numeric / COUNT(*)) * 100, 2) AS distinct_percent,
     COUNT(DISTINCT {{ adapter.quote(chunk_column[0]) }}) = COUNT(*) AS is_unique,
     {% if chunk_column[1]|lower in ['integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'smallint'] %}
    CAST(MIN({{ adapter.quote(chunk_column[0]) }}) AS VARCHAR) AS min,
    CAST(MAX({{ adapter.quote(chunk_column[0]) }}) AS VARCHAR) AS max,
    ROUND(AVG({{ adapter.quote(chunk_column[0]) }}), 2) AS avg,
    ROUND(STDDEV_SAMP({{ adapter.quote(chunk_column[0]) }}), 2) AS standard_deviation,
    {% else %}
    NULL AS min,
    NULL AS max,
    NULL AS avg,
    CAST(NULL AS NUMERIC) AS standard_deviation,
    {% endif %}
    (SELECT STRING_AGG(value::VARCHAR, ', ') 
     FROM (SELECT DISTINCT {{ adapter.quote(chunk_column[0]) }} AS value 
           FROM {{ source_table_name }} 
           WHERE {{ adapter.quote(chunk_column[0]) }} IS NOT NULL 
           ORDER BY value DESC 
           LIMIT 5) AS subquery) AS top_5_values,
    (SELECT STRING_AGG(value::VARCHAR, ', ') 
     FROM (SELECT DISTINCT {{ adapter.quote(chunk_column[0]) }} AS value 
           FROM {{ source_table_name }} 
           WHERE {{ adapter.quote(chunk_column[0]) }} IS NOT NULL 
           ORDER BY value ASC 
           LIMIT 5) AS subquery) AS least_5_values,

     TIMESTAMP '{{ current_date_and_time }}' AS profiled_at
 FROM {{ source_table_name }}
{%- endmacro -%}

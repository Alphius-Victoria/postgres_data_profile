{{
    config(
        alias='TMP_DATA_PROFILE',
        materialized='table',
        post_hook='drop table {{ this }}'
    )
}}
{% set empty_columns_query %}
    SELECT *
    FROM "postgres_test"."test"."config_data_profile"
    WHERE destination_database IS NULL 
        OR destination_schema IS NULL
        OR destination_table IS NULL
        OR source_database IS NULL;
{% endset %}

{% set empty_columns_result = run_query(empty_columns_query) %}
{% if empty_columns_result | length > 0 %}
    {% set error_message = "Validation Error: Empty column(s) found in seed table" %}
    {{ print("\033[91m" ~ error_message ~ "\033[0m") }}
    {{ exceptions.raise_compiler_error(error_message) }}
{% else %}
{% if execute %}
    {{ print("Execution started") }}
    {%- set seed_table = '"postgres_test"."test"."config_data_profile"' -%}
    {%- set read_config_table = run_query('SELECT * FROM ' ~ seed_table) -%}
    {{ print("Config table read") }}
    {% set get_current_timestamp %}
        SELECT current_timestamp AT TIME ZONE 'UTC' AS utc_time_zone;
    {% endset %}
    {% if execute %}
        {% set profiled_at = run_query(get_current_timestamp).columns[0].values()[0] %}
        {{ print("Current timestamp obtained: " ~ profiled_at) }}
    {% endif %}

    {% for profile_detail in read_config_table %}
        {{ print("Processing profile detail: " ~ profile_detail) }}
        {%- set destination_database = profile_detail[0] %}
        {%- set destination_schema = profile_detail[1] %}
        {%- set destination_table = profile_detail[2] %}
        {%- set source_database = profile_detail[3] %}

        -- Create a temporary table to store the data
        {% set schema_create %}
            {{ create_new_schema(destination_schema) }}
        {% endset %}
        {{ print("Schema create statement: " ~ schema_create) }}
        {% do run_query(schema_create) %}
        {{ print("Schema created") }}

        {% set create_table %}
            {{ create_data_profile_table(destination_database, destination_schema, destination_table) }}
        {% endset %}
        {{ print("Create table statement: " ~ create_table) }}
        {% do run_query(create_table) %}
        {{ print("Table created") }}
        {%- set include_schemas = profile_detail[4].split(',') if profile_detail[4] is not none else [] %}
        {%- set exclude_schemas = profile_detail[5].split(',') if profile_detail[5] is not none else [] %}
        {%- set include_tables = profile_detail[6].split(',') if profile_detail[6] is not none else [] %}
        {%- set exclude_tables = profile_detail[7].split(',') if profile_detail[7] is not none else [] %}

        {{ print("Include schemas: " ~ include_schemas) }}
        {{ print("Exclude schemas: " ~ exclude_schemas) }}
        {{ print("Include tables: " ~ include_tables) }}
        {{ print("Exclude tables: " ~ exclude_tables) }}

        -- Read the table names from the information schema for that particular layer
        {%- set read_information_schema_datas = read_information_schema(source_database, include_schemas, exclude_schemas, include_tables, exclude_tables) %}
        {{ print("Read information schema statement: " ~ read_information_schema_datas) }}
        {% set information_schema_datas = run_query(read_information_schema_datas) %}
        {{ print("Information schema data: " ~ information_schema_datas) }}
        {% set filtered_information_schema_datas = information_schema_datas | selectattr(2, 'ne', destination_table) | list %}
        {{ print("Filtered information schema data: " ~ filtered_information_schema_datas) }}
        {% for information_schema_data in filtered_information_schema_datas %}
            {{ print("Processing information schema data: " ~ information_schema_data) }}
            {%- set source_table_name = information_schema_data[0] ~ '.' ~ information_schema_data[1] ~ '.' ~ information_schema_data[2] -%}
            {{ print("Source table name: " ~ source_table_name) }}
            {% set validator_query %}
                SELECT 
                    'Query ID :' || query_id || ';\n' || ERROR_MESSAGE AS error_info
                FROM information_schema.query_history
                WHERE 
                    session_id = CURRENT_SESSION()
                    AND execution_status LIKE 'FAILED%';
            {% endset %}
            {{ print("Validator query: " ~ validator_query) }}
            {% set chunk_columns = [] %}
            {{ print("Initializing chunk columns") }}

            -- Create the source columns query string
            {%- set source_columns_query = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'' ~ information_schema_data[2] ~ '\' AND table_schema = \'' ~ information_schema_data[1] ~ '\' AND table_catalog = \'' ~ information_schema_data[0] ~ '\'' -%}
            {{ print("Source columns query: " ~ source_columns_query) }}
            {% set source_columns = run_query(source_columns_query) %}
            {{ print("Source columns: " ~ source_columns) }}
            {%- for source_column in source_columns -%}
                {{ print("Processing source column: " ~ source_column) }}
                {% do chunk_columns.append(source_column) %}
                {{ print("Chunk columns: " ~ chunk_columns) }}
                {% if loop.index % 100 == 0 or loop.last %}
                    {{ print("Processing chunk columns, loop index: " ~ loop.index) }}
                    {% set data_profile_queries = [] %}
                    {%- for chunk_column in chunk_columns %}
                        {% set data_profile_queries = [] %}
                        {% set data_profile_query = do_data_profile(information_schema_data, source_table_name, chunk_column, profiled_at) %}
                        {{ print("Data profile query: " ~ data_profile_query) }}
                        {% do data_profile_queries.append(data_profile_query) %}
                        {% set insert_rows %}
                            INSERT INTO {{ destination_database }}.{{ destination_schema }}.{{ destination_table }} (
                                {%- for query in data_profile_queries %}
                                    {{ query }}{% if not loop.last %} UNION ALL {% endif %}
                                {%- endfor %}
                            )                                
                        {% endset %}

                        {{ print("Insert rows statement: " ~ insert_rows) }}
                        {% do run_query(insert_rows) %}
                        {{ print("Rows inserted") }}
                        {% set chunk_columns = [] %}
                        {{ print("Chunk columns reset") }}
                    {%- endfor %}
                {% endif %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
{% endif %}
{% endif %}
SELECT 'temp_data_for_creating_the_table' AS temp_column

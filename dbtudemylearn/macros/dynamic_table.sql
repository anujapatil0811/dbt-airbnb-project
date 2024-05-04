{% macro create_dynamic_table(table_name, target_lag, warehouse) %}
   CREATE OR REPLACE TABLE {{ table_name }}
   TARGET_LAG = '{{ target_lag }}'
   WAREHOUSE = {{ warehouse }}
   AS
   SELECT
       var:id::int AS id,
       var:fname::string AS first_name,
       var:lname::string AS last_name
   FROM raw;
{% endmacro %}
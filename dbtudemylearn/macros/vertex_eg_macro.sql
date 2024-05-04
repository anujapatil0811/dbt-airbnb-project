{% macro generate_full_merge_task(sourceDatabaseName, sourceSchemaName, sourceTableName, targetDatabaseName, targetSchemaName, targetTableName, sourceModelName, matchingKeys, taskSchedule='USING CRON 0 0 * * * UTC', warehouseName=var('merge_warehouse')[target.name], doDelete=true) %}
 
    {%- set sourceDatabaseFullName -%}
    {{ generate_db_source_name(sourceDatabaseName) }}
    {%- endset -%}
 
    {%- set sourceSchemaFullName -%}
    {{ generate_schema_name(sourceSchemaName) }}
    {%- endset -%}
 
    {%- set targetDatabaseFullName -%}
    {{ generate_db_source_name(targetDatabaseName) }}
    {%- endset -%}
 
    {%- set targetSchemaFullName -%}
    {{ generate_schema_name(targetSchemaName) }}
    {%- endset -%}
 
 
{% if execute %}
 
     {% set sourceTableColumns = adapter.get_columns_in_relation(ref(sourceModelName)) %}
 
        {%- set taskSQL -%}
            CREATE OR REPLACE task {{ targetDatabaseFullName }}.{{ targetSchemaFullName }}.TSK_{{ target.name }}_UPDATE_{{ targetTableName }}
            warehouse = '{{warehouseName}}'
            schedule = '{{taskSchedule}}'
            as
                MERGE INTO {{ targetDatabaseFullName}}.{{ targetSchemaFullName }}.{{ targetTableName }} t
                USING
                (
                    SELECT
                        {% for col in sourceTableColumns -%}
                            {% set columnName = col.name -%}
                            S.{{columnName}},
                        {% endfor -%}
                        {% for matchKey in matchingKeys -%}
                            NVL(S.{{matchKey}}, t.{{matchKey}}) as comb_{{matchKey}}
                            {% if not loop.last -%},{% endif -%}
                        {% endfor -%}
                    FROM {{ sourceDatabaseFullName}}.{{ sourceSchemaFullName }}.{{ sourceTableName }} S
                    FULL OUTER JOIN {{ targetDatabaseFullName}}.{{ targetSchemaFullName }}.{{ targetTableName }} t ON
                    {% for matchKey in matchingKeys -%}
                        EQUAL_NULL(S.{{matchKey}},t.{{matchKey}})
                        {% if not loop.last -%} AND {% endif -%}
                    {% endfor -%}
                ) source
                ON
                {% for matchKey in matchingKeys -%}
                    EQUAL_NULL(source.comb_{{matchKey}},t.{{matchKey}})
                    {% if not loop.last -%} AND {% endif -%}
                {% endfor -%}
                --Case 1: data is updated
                WHEN MATCHED AND (
                {% for matchKey in matchingKeys -%}
                    source.{{matchKey}} IS NOT NULL
                    {% if not loop.last -%} OR {% endif -%}
                {% endfor -%} )
                THEN UPDATE SET
                {% for col in sourceTableColumns -%}
                    {% set columnName = col.name -%}
                    t.{{columnName}}=source.{{columnName}}
                    {% if not loop.last -%},{% endif -%}
                {% endfor -%}
                --Case 2: data is deleted
                {% if doDelete == true -%}
                WHEN MATCHED AND
                {% for matchKey in matchingKeys -%}
                    source.{{matchKey}} IS NULL
                    {% if not loop.last -%} AND {% endif -%}
                {% endfor -%}
                THEN DELETE
                {% endif -%}
                --Case 3: new data is added
                WHEN NOT MATCHED
                THEN INSERT
                (
                {% for col in sourceTableColumns -%}
                    {% set columnName = col.name -%}
                    {{columnName}}
                    {% if not loop.last -%},{% endif -%}
                {% endfor -%}
                )
                VALUES
                (
                {% for col in sourceTableColumns -%}
                    {% set columnName = col.name -%}
                    source.{{columnName}}
                    {% if not loop.last -%},{% endif -%}
                {% endfor -%}
                );
    {%- endset -%}
 
    {%- set resumeSQL -%}
        ALTER TASK {{ targetDatabaseFullName }}.{{ targetSchemaFullName }}.TSK_{{ target.name }}_UPDATE_{{ targetTableName }} RESUME;
    {%- endset -%}
 
    {%- do run_query(taskSQL) -%}
    {%- if target.name != 'QA' and target.name != 'STG' -%}
        {%- do run_query(resumeSQL) -%}
    {%- endif -%}
 
{% endif -%}
 
{% endmacro %}
 
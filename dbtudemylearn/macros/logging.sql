{% macro learn_logging() %}
    {{log("Call the macro", info=True)}}
{% endmacro %}

{# command : dbt run-operation learn_logging #}

--Disable logging
{#
    If we just comment out the log function 
    it not disable after running the macro because
    There are 2 layes of executions in DBT, so when 
    macro gets executed , jinja part of macro 
    will get rendered and executed. And then output of this jinja
    will be the SQL and this SQL might be picked up by model and so on..
    -- : instructs the SQL engine to skip the line but this got already 
    evaluated in first .

    To fix this add jinja command to comment out it [{#...}]
#}
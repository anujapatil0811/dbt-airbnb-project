{% macro learn_variables() %}

{#Jinja Variable#}
    {% set your_name_jinja = "Anuja"%}
    {{ log ("Hello " ~ your_name_jinja, info=True)}}
    

{#dbt Variable#}
    {#
    {{ log("Hello dbt user " ~var("user_name") ~ "!", info=True) }}
    #}

    {{ log("Hello dbt user " ~var("user_name", "NO username is SET!!") ~ "!", info=True) }}
{% endmacro %}

{# to run: dbt run-operation learn_variables --vars '{user_name: ANUJAPATIL}'#}

{#
    Two Types of variables:
    Jinja Variables
    Simple vars thate come from JInja Languge
    Define and use in jinja

    DBT variables
    dbt specific vars which can be passed to dbt 
    command line 
#}


{#
    Dealing with missing variable values : 3 ways
    1. define a default value in macro or in SQL or whenever you want to use this variable

    {{ log("Hello dbt user " ~var("user_name", "NO username is SET!!") ~ "!", info=True) }}

    2. Pass a deffered value in DB project file
    3. Use Jinja control structure to check Variable existence

    To check variable existence:
    {% if var("var_name", False ) %}

    
    dbt run --select fct_reviews --vars "{start_date: '2024-02-15 00:00:00', end_date: '2024-03-15 23:59:59'}"
    
   
   
    We have the following situation.
    The variable called user_name  is defined in dbt_project.yml :
        vars:
          user_name: default_username
    We also pass a value for this variable from the command line using --vars '{user_name: my_username}'
    What will var("user_name", "no_username_provided") evaluate to?

    --> my_username
#}
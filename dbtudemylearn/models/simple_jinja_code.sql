{#This jinja code generates SELECT statements to print #}
{% set max_number = 10 %}
{% for i in range(max_number) %}
    SELECT {{ i }} AS number
    {% if not loop.last %}
      UNION
    {% endif %}
{% endfor %}
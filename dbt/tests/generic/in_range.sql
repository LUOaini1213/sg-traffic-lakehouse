{#- Fails for every non-null value outside [min_value, max_value]. -#}
{% test in_range(model, column_name, min_value=none, max_value=none) %}
select {{ column_name }} as value
from {{ model }}
where {{ column_name }} is not null
  and (
    {%- if min_value is not none %} {{ column_name }} < {{ min_value }}{% else %} false{% endif %}
    or
    {%- if max_value is not none %} {{ column_name }} > {{ max_value }}{% else %} false{% endif %}
  )
{% endtest %}

{#- Fails once for every combination of the columns that appears more than once. -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{#
    Project-local generic tests.

    Deliberately not dbt_utils: one range check is not worth a package
    dependency that every clone would have to `dbt deps` before the project
    runs at all. These are a few lines each and have no install step.
#}

{% test value_in_range(model, column_name, min_value, max_value) %}

    {#
        Fails on any non-null value outside [min_value, max_value].
        NULLs are ignored - absence is a not_null test's job, not this one's.
    #}
    select
        {{ column_name }} as offending_value
    from {{ model }}
    where {{ column_name }} is not null
      and (
            {{ column_name }} < {{ min_value }}
         or {{ column_name }} > {{ max_value }}
      )

{% endtest %}


{% test unique_combination(model, combination_of_columns) %}

    {#
        Grain assertion for a composite key, which built-in `unique` cannot
        express. Returns one row per duplicated combination.
    #}
    {% set column_list = combination_of_columns | join(', ') %}

    select
        {{ column_list }},
        count(*) as rows_for_combination
    from {{ model }}
    group by {{ column_list }}
    having count(*) > 1

{% endtest %}


{% test is_true(model, column_name) %}

    {#
        Fails on any row where the boolean column is false. NULL counts as a
        failure too: "we could not tell" is not "it is fine".
    #}
    select
        *
    from {{ model }}
    where {{ column_name }} is null
       or not {{ column_name }}

{% endtest %}

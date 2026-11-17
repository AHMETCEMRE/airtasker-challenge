{#
    The score-to-label mapping, in exactly one place.

    Product tunes the thresholds after launch and Operations counts high-risk
    tasks per day. If the CASE expression were written out in both marts, a
    retune would have to be applied twice and the two teams could silently
    disagree about how many tasks are high risk. Here it cannot happen: there
    is one expression, reading two vars.

    'unknown' is deliberately a label rather than a NULL label, and is NOT
    folded into 'low'. A task with no usable forecast is not a safe task - it
    is a task we cannot speak about - and calling it 'low' would suppress a
    warning the pipeline has no evidence to suppress.
#}

{% macro weather_risk_label(score_expression) %}
    case
        when {{ score_expression }} is null
            then 'unknown'
        when {{ score_expression }} >= {{ var('risk_threshold_high') }}
            then 'high'
        when {{ score_expression }} >= {{ var('risk_threshold_medium') }}
            then 'medium'
        else 'low'
    end
{% endmacro %}


{#
    "Now" for the eligibility filter.

    Defaults to the dbt invocation's start time so that every model in a run
    shares one clock - current_timestamp would let two models disagree about
    whether a task scheduled in the next second is upcoming. The var override
    exists so a past run can be reproduced exactly.
#}

{% macro risk_as_of_utc() %}
    {%- if var('risk_as_of_utc') -%}
        cast('{{ var('risk_as_of_utc') }}' as timestamp)
    {%- else -%}
        cast('{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S') }}' as timestamp)
    {%- endif -%}
{% endmacro %}

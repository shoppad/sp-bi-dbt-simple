-- tests/workflow_saves_count_proportion.sql
{% set lookback_window_days = 30 %}
{% set min_save_percent = 0.05 %}

WITH
recent_workflows AS (
    SELECT
        *
    FROM
        {{ ref('workflows') }}
    WHERE
        created_at_pt >= CURRENT_DATE - INTERVAL '{{ lookback_window_days }} days'

),

filtered_data AS (
    SELECT
        *
    FROM
        recent_workflows
    WHERE save_count >= 1
),

total_count AS (
    SELECT
        COUNT(*) AS total
    FROM
        recent_workflows
),

filtered_count AS (
    SELECT
        COUNT(*) AS filtered
    FROM
        filtered_data
),

proportion_data AS (
    SELECT
        filtered::FLOAT / total AS proportion
    FROM
        (SELECT filtered FROM filtered_count) AS fc,
        (SELECT total FROM total_count) AS tc
)

SELECT
        'The proportion of workflows saved in the last {{ lookback_window_days }} days is below {{ min_save_percent * 100 }}%' AS error_message,
    proportion
FROM
    proportion_data
WHERE
    proportion < {{ min_save_percent }}

{% macro generate_date_spine(start_date, end_date) %}
    WITH date_spine AS (
        SELECT 
            DATEADD(
                DAY,
                ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
                '{{ start_date }}'::DATE
            ) AS date_day
        FROM TABLE(GENERATOR(ROWCOUNT => 10000))
        QUALIFY date_day <= '{{ end_date }}'::DATE
    )
    SELECT date_day FROM date_spine
{% endmacro %}

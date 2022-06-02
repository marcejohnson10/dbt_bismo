with current_view as (

select * from {{ ref('current_view') }}

where max_collector_tstamp >= ‘{{ run_started_at }}’

),

historical_table as (

select * from {{ ref('historical_table') }}

where max_collector_tstamp < '{{ run_started_at }}'

),

unioned_tables as (

select * from current_view

union all

select * from historical_table
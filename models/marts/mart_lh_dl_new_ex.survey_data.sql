{{ config(
  materialized='table')}}

WITH cte AS (
select *
FROM {{ref('int_lh_dl_new_ex.survey_23-24')}}
)
SELECT * FROM cte
-- Basic counts / indicators
with a as (select count(*) as patients from {{ ref('dim_patient') }}),
b as (select count(*) as practitioners from {{ ref('dim_practitioner') }}),
c as (select count(*) as encounters from {{ ref('fact_encounter') }}),
d as (
    select practitioner_id, count(*) as encounters_per_practitioner
    from {{ ref('fact_encounter') }}
    group by 1
)
select
  a.patients,
  b.practitioners,
  c.encounters
from a,b,c

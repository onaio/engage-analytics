with a as (
  select * from {{ ref('int_qr_answers_long') }}
),
map as (
  select * from {{ ref('score_map') }}
)
select
  a.*,
  coalesce(
    a.value_num,
    case when a.value_code is not null then
      (select m.numeric_value from map m
       where a.link_id = m.link_id
         and a.value_code = m.coding_code
         and a.questionnaire_canonical ilike '%' || m.questionnaire_match || '%'
       limit 1)
    end
  ) as score_value
from a

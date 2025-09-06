{% macro meta_tag_pivot(resource_name) -%}
with unnested_tags AS (
    select
        id,
        jsonb_array_elements(coalesce(meta::jsonb -> 'tag', '[]'::jsonb)) ->> 'code'    as meta_tag_code,
        jsonb_array_elements(coalesce(meta::jsonb -> 'tag', '[]'::jsonb)) ->> 'display' as meta_tag_display,
        _airbyte_emitted_at
    from {{ ref(resource_name) }}
),
tags_renamed as (
    select
      id as resource_id,
      _airbyte_emitted_at,
      meta_tag_code,
      case
        when meta_tag_display = 'Practitioner Location'    then 'practitioner_location_id'
        when meta_tag_display = 'Practitioner Organization' then 'practitioner_organization_id'
        when meta_tag_display = 'Practitioner'              then 'practitioner_id'
        when meta_tag_display = 'Practitioner CareTeam'     then 'practitioner_careteam_id'
        when meta_tag_display = 'Application Version'       then 'application_version'
        else null
      end as display_name
    from unnested_tags
)
select
  resource_id,
  _airbyte_emitted_at,
  {{ dbt_utils.pivot(
      'display_name',
      ['practitioner_location_id','practitioner_organization_id','practitioner_id','practitioner_careteam_id','application_version'],
      agg='max',
      then_value='meta_tag_code',
      else_value='null'
  ) }}
from tags_renamed
{% if is_incremental() %}
  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}
group by resource_id, _airbyte_emitted_at
{%- endmacro -%}

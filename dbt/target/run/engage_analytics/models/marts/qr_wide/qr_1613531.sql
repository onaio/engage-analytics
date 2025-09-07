
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_1613531__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- Sociodemographic Survey (id: 1613531, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/1613531') ),
  base as (
    select
      a.qr_id,
      a.linkid,
      string_agg(distinct a.answer_value_text, ' | ' order by a.answer_value_text) as answer_value_text
    from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_answers_long" a
    join ids on a.questionnaire_id = ids.ident
    group by 1,2
  ),
  header as (
    select h.*
    from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header" h
    join ids on h.questionnaire_id = ids.ident
  ),
  tags as (
    select * from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
  ),
  pivoted as (
    select
      b.qr_id,
      
  
    max(
      
      case
      when linkid = '9cce9788-d7e7-4456-d455-5bd8dc3d2f2a'
        then answer_value_text
      else null
      end
    )
    
      
            as "9cce9788-d7e7-4456-d455-5bd8dc3d2f2a"
      
    
    ,
  
    max(
      
      case
      when linkid = '7b68e9a3-703a-4b57-876e-8dd400826db9'
        then answer_value_text
      else null
      end
    )
    
      
            as "7b68e9a3-703a-4b57-876e-8dd400826db9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-age'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-age"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b4afce83-4279-4d19-8a8b-1e0b7e1ba73c'
        then answer_value_text
      else null
      end
    )
    
      
            as "b4afce83-4279-4d19-8a8b-1e0b7e1ba73c"
      
    
    ,
  
    max(
      
      case
      when linkid = '6e621fcc-a25c-4070-8fda-894bb9df68e4'
        then answer_value_text
      else null
      end
    )
    
      
            as "6e621fcc-a25c-4070-8fda-894bb9df68e4"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f11fe547-9be9-4693-8a85-f252e4a6262f'
        then answer_value_text
      else null
      end
    )
    
      
            as "f11fe547-9be9-4693-8a85-f252e4a6262f"
      
    
    ,
  
    max(
      
      case
      when linkid = '5b133a13-0303-4d2f-8884-ffa931d2c173'
        then answer_value_text
      else null
      end
    )
    
      
            as "5b133a13-0303-4d2f-8884-ffa931d2c173"
      
    
    ,
  
    max(
      
      case
      when linkid = '05990b62-11a8-4ab7-8988-1d2a17edcff7'
        then answer_value_text
      else null
      end
    )
    
      
            as "05990b62-11a8-4ab7-8988-1d2a17edcff7"
      
    
    ,
  
    max(
      
      case
      when linkid = '0e027468-c1e9-4977-f5f5-1cfa00ab8af5'
        then answer_value_text
      else null
      end
    )
    
      
            as "0e027468-c1e9-4977-f5f5-1cfa00ab8af5"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-gender-identity'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-gender-identity"
      
    
    ,
  
    max(
      
      case
      when linkid = '8f600ee9-8d17-43df-ed68-c9a7de2da7d0'
        then answer_value_text
      else null
      end
    )
    
      
            as "8f600ee9-8d17-43df-ed68-c9a7de2da7d0"
      
    
    ,
  
    max(
      
      case
      when linkid = 'task-id-socio-pdf'
        then answer_value_text
      else null
      end
    )
    
      
            as "task-id-socio-pdf"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-how-you-think-of-yourself'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-how-you-think-of-yourself"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-pronouns'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-pronouns"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd7e0e5c9-35bd-40bc-9224-9cf9e4b9dbba'
        then answer_value_text
      else null
      end
    )
    
      
            as "d7e0e5c9-35bd-40bc-9224-9cf9e4b9dbba"
      
    
    ,
  
    max(
      
      case
      when linkid = '2f8a7973-d9bb-493b-aa68-fb01e0f1aefe'
        then answer_value_text
      else null
      end
    )
    
      
            as "2f8a7973-d9bb-493b-aa68-fb01e0f1aefe"
      
    
    ,
  
    max(
      
      case
      when linkid = '1fdf1a17-7af6-413f-81e9-cf6823fe7fde'
        then answer_value_text
      else null
      end
    )
    
      
            as "1fdf1a17-7af6-413f-81e9-cf6823fe7fde"
      
    
    ,
  
    max(
      
      case
      when linkid = '3409c1f0-adf5-4b3b-8ea3-e7672e4cb5af'
        then answer_value_text
      else null
      end
    )
    
      
            as "3409c1f0-adf5-4b3b-8ea3-e7672e4cb5af"
      
    
    ,
  
    max(
      
      case
      when linkid = '92fed595-49f2-4fab-81ff-885941f31e07'
        then answer_value_text
      else null
      end
    )
    
      
            as "92fed595-49f2-4fab-81ff-885941f31e07"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-name'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-name"
      
    
    ,
  
    max(
      
      case
      when linkid = '66d30814-73ea-4fa4-edf8-d0d79378b130'
        then answer_value_text
      else null
      end
    )
    
      
            as "66d30814-73ea-4fa4-edf8-d0d79378b130"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-biological-sex'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-biological-sex"
      
    
    ,
  
    max(
      
      case
      when linkid = '3960c72d-b6e7-418e-86e9-f8348a49e739'
        then answer_value_text
      else null
      end
    )
    
      
            as "3960c72d-b6e7-418e-86e9-f8348a49e739"
      
    
    ,
  
    max(
      
      case
      when linkid = '4baa9e68-fe7a-489a-8b03-cc045b53d93d'
        then answer_value_text
      else null
      end
    )
    
      
            as "4baa9e68-fe7a-489a-8b03-cc045b53d93d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-dob'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-dob"
      
    
    
  

    from base b
    group by b.qr_id
  )
  select
    h.questionnaire_id,
    h.subject_patient_id,
    h.encounter_id,
    h.author_practitioner_id,
    t.practitioner_location_id,
    t.practitioner_organization_id,
    t.practitioner_id,
    t.practitioner_careteam_id,
    t.application_version,
    p.*
  from pivoted p
  join header h using (qr_id)
  left join tags t on t.resource_id = h.qr_id

  
  


  );
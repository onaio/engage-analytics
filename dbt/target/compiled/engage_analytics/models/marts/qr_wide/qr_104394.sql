-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- SPI Subform 4 (id: ipc-104394, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/104394') ),
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
      when linkid = 'c1d9c871-c1f8-41cc-8c54-edd4e5b162fe'
        then answer_value_text
      else null
      end
    )
    
      
            as "c1d9c871-c1f8-41cc-8c54-edd4e5b162fe"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf'
        then answer_value_text
      else null
      end
    )
    
      
            as "cd8e3d6d-e9ff-458d-d122-57070bebffaf"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f952feaf-7abc-4f6f-a3bc-d10a43ea62fa'
        then answer_value_text
      else null
      end
    )
    
      
            as "f952feaf-7abc-4f6f-a3bc-d10a43ea62fa"
      
    
    ,
  
    max(
      
      case
      when linkid = '3785a264-8bd5-44ab-bf08-8aa33735c0eb'
        then answer_value_text
      else null
      end
    )
    
      
            as "3785a264-8bd5-44ab-bf08-8aa33735c0eb"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e510a8cf-0bc7-4577-b9ee-ca758d431aa0'
        then answer_value_text
      else null
      end
    )
    
      
            as "e510a8cf-0bc7-4577-b9ee-ca758d431aa0"
      
    
    ,
  
    max(
      
      case
      when linkid = '9d203bac-75fd-4f31-bed6-073502a977f3'
        then answer_value_text
      else null
      end
    )
    
      
            as "9d203bac-75fd-4f31-bed6-073502a977f3"
      
    
    ,
  
    max(
      
      case
      when linkid = 'bd7644ee-28f1-4f43-a654-17fd5034d1f6'
        then answer_value_text
      else null
      end
    )
    
      
            as "bd7644ee-28f1-4f43-a654-17fd5034d1f6"
      
    
    ,
  
    max(
      
      case
      when linkid = '14ec8995-eee9-416f-83ad-9f44631ac9b0'
        then answer_value_text
      else null
      end
    )
    
      
            as "14ec8995-eee9-416f-83ad-9f44631ac9b0"
      
    
    ,
  
    max(
      
      case
      when linkid = '7ca03155-c9ec-45e1-8cd0-5b86fa444ad0'
        then answer_value_text
      else null
      end
    )
    
      
            as "7ca03155-c9ec-45e1-8cd0-5b86fa444ad0"
      
    
    ,
  
    max(
      
      case
      when linkid = '54b75d55-1c43-4552-acbf-d9754831add8'
        then answer_value_text
      else null
      end
    )
    
      
            as "54b75d55-1c43-4552-acbf-d9754831add8"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c396a3af-6f0d-4a8e-a42b-4f47bbce5f46'
        then answer_value_text
      else null
      end
    )
    
      
            as "c396a3af-6f0d-4a8e-a42b-4f47bbce5f46"
      
    
    ,
  
    max(
      
      case
      when linkid = '3e388595-a32f-4623-8c9f-3c67bad04070'
        then answer_value_text
      else null
      end
    )
    
      
            as "3e388595-a32f-4623-8c9f-3c67bad04070"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cf896559-dff5-4704-94a0-6a6aedee53c5'
        then answer_value_text
      else null
      end
    )
    
      
            as "cf896559-dff5-4704-94a0-6a6aedee53c5"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-2'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-2"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-3'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-3"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f7496859-63bb-4b2f-8f75-96e44e893bf7'
        then answer_value_text
      else null
      end
    )
    
      
            as "f7496859-63bb-4b2f-8f75-96e44e893bf7"
      
    
    ,
  
    max(
      
      case
      when linkid = '65252389-fa9e-4ef4-93ca-e952c77c4c05'
        then answer_value_text
      else null
      end
    )
    
      
            as "65252389-fa9e-4ef4-93ca-e952c77c4c05"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c1e99f8b-b1ab-46d1-b205-ecbbb4240fdf'
        then answer_value_text
      else null
      end
    )
    
      
            as "c1e99f8b-b1ab-46d1-b205-ecbbb4240fdf"
      
    
    ,
  
    max(
      
      case
      when linkid = '3314932c-64e4-4192-b583-eb31e87da1d0'
        then answer_value_text
      else null
      end
    )
    
      
            as "3314932c-64e4-4192-b583-eb31e87da1d0"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e7ba8485-5154-41b8-ac18-35410a83cd99'
        then answer_value_text
      else null
      end
    )
    
      
            as "e7ba8485-5154-41b8-ac18-35410a83cd99"
      
    
    ,
  
    max(
      
      case
      when linkid = '18d8ee56-ffd8-4711-9074-126ef2cdc564'
        then answer_value_text
      else null
      end
    )
    
      
            as "18d8ee56-ffd8-4711-9074-126ef2cdc564"
      
    
    ,
  
    max(
      
      case
      when linkid = '74d0f0df-e726-4c40-84ce-efece882818b'
        then answer_value_text
      else null
      end
    )
    
      
            as "74d0f0df-e726-4c40-84ce-efece882818b"
      
    
    ,
  
    max(
      
      case
      when linkid = '9e11ce97-90e8-42f2-ba18-8c4f0720c131'
        then answer_value_text
      else null
      end
    )
    
      
            as "9e11ce97-90e8-42f2-ba18-8c4f0720c131"
      
    
    ,
  
    max(
      
      case
      when linkid = '40d6a87f-9498-4cfd-b233-4610e6de08c2'
        then answer_value_text
      else null
      end
    )
    
      
            as "40d6a87f-9498-4cfd-b233-4610e6de08c2"
      
    
    ,
  
    max(
      
      case
      when linkid = '57e87170-761c-4b57-a09c-d50cab02860c'
        then answer_value_text
      else null
      end
    )
    
      
            as "57e87170-761c-4b57-a09c-d50cab02860c"
      
    
    ,
  
    max(
      
      case
      when linkid = '5056d0ba-67ac-4eab-b05d-d42941f7b264'
        then answer_value_text
      else null
      end
    )
    
      
            as "5056d0ba-67ac-4eab-b05d-d42941f7b264"
      
    
    ,
  
    max(
      
      case
      when linkid = '2b2e05e9-0c11-490c-bbff-278ba90d7b4e'
        then answer_value_text
      else null
      end
    )
    
      
            as "2b2e05e9-0c11-490c-bbff-278ba90d7b4e"
      
    
    ,
  
    max(
      
      case
      when linkid = '249f5ce8-ecc0-4e43-9675-6b01ca1502a3'
        then answer_value_text
      else null
      end
    )
    
      
            as "249f5ce8-ecc0-4e43-9675-6b01ca1502a3"
      
    
    ,
  
    max(
      
      case
      when linkid = '9f46e0ff-0532-4105-be36-0cfa9d0770f2'
        then answer_value_text
      else null
      end
    )
    
      
            as "9f46e0ff-0532-4105-be36-0cfa9d0770f2"
      
    
    ,
  
    max(
      
      case
      when linkid = '0397084a-cd51-4b3a-809d-970a2406ea49'
        then answer_value_text
      else null
      end
    )
    
      
            as "0397084a-cd51-4b3a-809d-970a2406ea49"
      
    
    ,
  
    max(
      
      case
      when linkid = '808ba31c-cd29-4743-8801-dfce357d5956'
        then answer_value_text
      else null
      end
    )
    
      
            as "808ba31c-cd29-4743-8801-dfce357d5956"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd82c6ae9-0516-4513-91fa-664686779d6a'
        then answer_value_text
      else null
      end
    )
    
      
            as "d82c6ae9-0516-4513-91fa-664686779d6a"
      
    
    ,
  
    max(
      
      case
      when linkid = 'bc981d8a-7f22-450b-abab-3acca810ccad'
        then answer_value_text
      else null
      end
    )
    
      
            as "bc981d8a-7f22-450b-abab-3acca810ccad"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd0aa3679-e465-4225-85d6-5822efb10eba'
        then answer_value_text
      else null
      end
    )
    
      
            as "d0aa3679-e465-4225-85d6-5822efb10eba"
      
    
    ,
  
    max(
      
      case
      when linkid = '86da79f0-44cb-417d-ab20-c7ecc6f8f7c8'
        then answer_value_text
      else null
      end
    )
    
      
            as "86da79f0-44cb-417d-ab20-c7ecc6f8f7c8"
      
    
    ,
  
    max(
      
      case
      when linkid = '24e98b7c-2b1c-4ebe-946f-bcb77bc2aa36'
        then answer_value_text
      else null
      end
    )
    
      
            as "24e98b7c-2b1c-4ebe-946f-bcb77bc2aa36"
      
    
    ,
  
    max(
      
      case
      when linkid = '46c2861b-70a9-4fdf-94ea-e1a01cae3149'
        then answer_value_text
      else null
      end
    )
    
      
            as "46c2861b-70a9-4fdf-94ea-e1a01cae3149"
      
    
    ,
  
    max(
      
      case
      when linkid = '41cce38a-ca5c-4e24-904a-25b1027336ed'
        then answer_value_text
      else null
      end
    )
    
      
            as "41cce38a-ca5c-4e24-904a-25b1027336ed"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd2572206-b47c-4d1e-85cb-812d13a60a85'
        then answer_value_text
      else null
      end
    )
    
      
            as "d2572206-b47c-4d1e-85cb-812d13a60a85"
      
    
    ,
  
    max(
      
      case
      when linkid = 'bd62b649-7209-4d32-9bbb-dc2b2823aadb'
        then answer_value_text
      else null
      end
    )
    
      
            as "bd62b649-7209-4d32-9bbb-dc2b2823aadb"
      
    
    ,
  
    max(
      
      case
      when linkid = '53def3a1-a7e5-4326-95f2-2561c26ecb47'
        then answer_value_text
      else null
      end
    )
    
      
            as "53def3a1-a7e5-4326-95f2-2561c26ecb47"
      
    
    ,
  
    max(
      
      case
      when linkid = '33ae3bce-ebd5-4dbc-b40c-a4cbc30736e6'
        then answer_value_text
      else null
      end
    )
    
      
            as "33ae3bce-ebd5-4dbc-b40c-a4cbc30736e6"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b6f126dd-eac5-42e4-8896-e7400b0c0496'
        then answer_value_text
      else null
      end
    )
    
      
            as "b6f126dd-eac5-42e4-8896-e7400b0c0496"
      
    
    ,
  
    max(
      
      case
      when linkid = '509b140e-b195-4696-bc5f-f2fc2d2f77dd'
        then answer_value_text
      else null
      end
    )
    
      
            as "509b140e-b195-4696-bc5f-f2fc2d2f77dd"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ae4fa455-f173-435a-bb1b-4df1d1316bd9'
        then answer_value_text
      else null
      end
    )
    
      
            as "ae4fa455-f173-435a-bb1b-4df1d1316bd9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e6262800-1e24-4348-9bf2-9bb070d2f20a'
        then answer_value_text
      else null
      end
    )
    
      
            as "e6262800-1e24-4348-9bf2-9bb070d2f20a"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ae949697-92ae-476a-96cc-f8b0adc4221f'
        then answer_value_text
      else null
      end
    )
    
      
            as "ae949697-92ae-476a-96cc-f8b0adc4221f"
      
    
    ,
  
    max(
      
      case
      when linkid = '92ab6e65-f9bd-4718-8af6-f731425d3031'
        then answer_value_text
      else null
      end
    )
    
      
            as "92ab6e65-f9bd-4718-8af6-f731425d3031"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ec54e447-c981-4dd2-aaa4-659f8e494248'
        then answer_value_text
      else null
      end
    )
    
      
            as "ec54e447-c981-4dd2-aaa4-659f8e494248"
      
    
    ,
  
    max(
      
      case
      when linkid = '4a1c4c0d-158f-494c-a33f-1f7f8e41f21f'
        then answer_value_text
      else null
      end
    )
    
      
            as "4a1c4c0d-158f-494c-a33f-1f7f8e41f21f"
      
    
    ,
  
    max(
      
      case
      when linkid = '656a57f8-f6f0-4fc5-b347-71dd8d38f596'
        then answer_value_text
      else null
      end
    )
    
      
            as "656a57f8-f6f0-4fc5-b347-71dd8d38f596"
      
    
    ,
  
    max(
      
      case
      when linkid = '245e5273-e28b-41d2-a152-1f70d85576c8'
        then answer_value_text
      else null
      end
    )
    
      
            as "245e5273-e28b-41d2-a152-1f70d85576c8"
      
    
    ,
  
    max(
      
      case
      when linkid = '2728e75c-a0e9-4cd9-8d35-962e4f71bf41'
        then answer_value_text
      else null
      end
    )
    
      
            as "2728e75c-a0e9-4cd9-8d35-962e4f71bf41"
      
    
    ,
  
    max(
      
      case
      when linkid = '22c11dbc-73e0-4470-9ef6-d9bd01f48c99'
        then answer_value_text
      else null
      end
    )
    
      
            as "22c11dbc-73e0-4470-9ef6-d9bd01f48c99"
      
    
    ,
  
    max(
      
      case
      when linkid = '84018cd8-9134-429c-8dfe-fd6a16e4bd17'
        then answer_value_text
      else null
      end
    )
    
      
            as "84018cd8-9134-429c-8dfe-fd6a16e4bd17"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f895ed4f-5004-4da4-9d59-c21293674b21'
        then answer_value_text
      else null
      end
    )
    
      
            as "f895ed4f-5004-4da4-9d59-c21293674b21"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c5720a67-803f-4917-8261-3738de838418'
        then answer_value_text
      else null
      end
    )
    
      
            as "c5720a67-803f-4917-8261-3738de838418"
      
    
    ,
  
    max(
      
      case
      when linkid = '7e38a614-b0a2-48ea-b7c7-ca2bdc1f00db'
        then answer_value_text
      else null
      end
    )
    
      
            as "7e38a614-b0a2-48ea-b7c7-ca2bdc1f00db"
      
    
    ,
  
    max(
      
      case
      when linkid = 'be88b027-64b6-4c3c-bf59-36a94dc79b12'
        then answer_value_text
      else null
      end
    )
    
      
            as "be88b027-64b6-4c3c-bf59-36a94dc79b12"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a0017b28-0aa9-4926-a7be-813f0a5e6bd0'
        then answer_value_text
      else null
      end
    )
    
      
            as "a0017b28-0aa9-4926-a7be-813f0a5e6bd0"
      
    
    ,
  
    max(
      
      case
      when linkid = '01353b1d-f816-41c1-b330-910e9a76ed12'
        then answer_value_text
      else null
      end
    )
    
      
            as "01353b1d-f816-41c1-b330-910e9a76ed12"
      
    
    ,
  
    max(
      
      case
      when linkid = '658e8c22-d6ba-480a-81e0-dc4b6164de13'
        then answer_value_text
      else null
      end
    )
    
      
            as "658e8c22-d6ba-480a-81e0-dc4b6164de13"
      
    
    ,
  
    max(
      
      case
      when linkid = '796f5440-54f8-4d69-b961-a5b7e6a63240'
        then answer_value_text
      else null
      end
    )
    
      
            as "796f5440-54f8-4d69-b961-a5b7e6a63240"
      
    
    ,
  
    max(
      
      case
      when linkid = '5e2ab0f5-f5d9-4502-befe-aba770f0e206'
        then answer_value_text
      else null
      end
    )
    
      
            as "5e2ab0f5-f5d9-4502-befe-aba770f0e206"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-4-review'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-4-review"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c802f463-c8f5-4056-bc45-8ab8d64a1966'
        then answer_value_text
      else null
      end
    )
    
      
            as "c802f463-c8f5-4056-bc45-8ab8d64a1966"
      
    
    ,
  
    max(
      
      case
      when linkid = '706fc36f-efef-4bf9-827e-7dc100cc92bc'
        then answer_value_text
      else null
      end
    )
    
      
            as "706fc36f-efef-4bf9-827e-7dc100cc92bc"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c631e361-027c-4e4d-b894-48785a3e6dd0'
        then answer_value_text
      else null
      end
    )
    
      
            as "c631e361-027c-4e4d-b894-48785a3e6dd0"
      
    
    ,
  
    max(
      
      case
      when linkid = '6ac23c5d-0035-4895-85f2-33c2fbba8f3f'
        then answer_value_text
      else null
      end
    )
    
      
            as "6ac23c5d-0035-4895-85f2-33c2fbba8f3f"
      
    
    ,
  
    max(
      
      case
      when linkid = '45772f3f-c610-408b-942b-359769945cf4'
        then answer_value_text
      else null
      end
    )
    
      
            as "45772f3f-c610-408b-942b-359769945cf4"
      
    
    ,
  
    max(
      
      case
      when linkid = 'dae7f411-758d-4800-9842-fc0038a6b4a4'
        then answer_value_text
      else null
      end
    )
    
      
            as "dae7f411-758d-4800-9842-fc0038a6b4a4"
      
    
    ,
  
    max(
      
      case
      when linkid = '3cae39d6-8d44-4fe4-835b-3f3ecd9b64fe'
        then answer_value_text
      else null
      end
    )
    
      
            as "3cae39d6-8d44-4fe4-835b-3f3ecd9b64fe"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fc5c673b-41d4-4352-86bb-043c73ed9807'
        then answer_value_text
      else null
      end
    )
    
      
            as "fc5c673b-41d4-4352-86bb-043c73ed9807"
      
    
    ,
  
    max(
      
      case
      when linkid = '229e58ac-a18a-48dd-be10-f8b024a74eb3'
        then answer_value_text
      else null
      end
    )
    
      
            as "229e58ac-a18a-48dd-be10-f8b024a74eb3"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e8726638-cfdb-40ae-b72c-f1d594f64de5'
        then answer_value_text
      else null
      end
    )
    
      
            as "e8726638-cfdb-40ae-b72c-f1d594f64de5"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b676a0c2-cda0-4762-be79-2a94205ee68b'
        then answer_value_text
      else null
      end
    )
    
      
            as "b676a0c2-cda0-4762-be79-2a94205ee68b"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fe2f0911-8ddc-412b-a3e6-f216620d147e'
        then answer_value_text
      else null
      end
    )
    
      
            as "fe2f0911-8ddc-412b-a3e6-f216620d147e"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f5ab53d4-824c-460f-8939-b86ff3cd97a3'
        then answer_value_text
      else null
      end
    )
    
      
            as "f5ab53d4-824c-460f-8939-b86ff3cd97a3"
      
    
    ,
  
    max(
      
      case
      when linkid = '7b51625b-73b9-472d-9c71-299beb26694f'
        then answer_value_text
      else null
      end
    )
    
      
            as "7b51625b-73b9-472d-9c71-299beb26694f"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ec2a78e1-e70b-4f66-86b1-561cffaf9ffd'
        then answer_value_text
      else null
      end
    )
    
      
            as "ec2a78e1-e70b-4f66-86b1-561cffaf9ffd"
      
    
    ,
  
    max(
      
      case
      when linkid = '44f02467-8ee0-49f0-8da7-5e7ed827125a'
        then answer_value_text
      else null
      end
    )
    
      
            as "44f02467-8ee0-49f0-8da7-5e7ed827125a"
      
    
    ,
  
    max(
      
      case
      when linkid = '88e9b865-b81a-494b-aef2-39e803b94e73'
        then answer_value_text
      else null
      end
    )
    
      
            as "88e9b865-b81a-494b-aef2-39e803b94e73"
      
    
    ,
  
    max(
      
      case
      when linkid = '7845c436-4904-41fa-9553-2c401935623b'
        then answer_value_text
      else null
      end
    )
    
      
            as "7845c436-4904-41fa-9553-2c401935623b"
      
    
    ,
  
    max(
      
      case
      when linkid = '892337bd-d7fd-46a7-bf72-08e87c9f412c'
        then answer_value_text
      else null
      end
    )
    
      
            as "892337bd-d7fd-46a7-bf72-08e87c9f412c"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd5194db0-0546-4bd1-96d8-d56457930c03'
        then answer_value_text
      else null
      end
    )
    
      
            as "d5194db0-0546-4bd1-96d8-d56457930c03"
      
    
    ,
  
    max(
      
      case
      when linkid = '40d8ef64-02f0-42ae-9478-77651bd26dfa'
        then answer_value_text
      else null
      end
    )
    
      
            as "40d8ef64-02f0-42ae-9478-77651bd26dfa"
      
    
    ,
  
    max(
      
      case
      when linkid = '9faa2b78-1842-4326-bf7f-9975bf04dc2c'
        then answer_value_text
      else null
      end
    )
    
      
            as "9faa2b78-1842-4326-bf7f-9975bf04dc2c"
      
    
    ,
  
    max(
      
      case
      when linkid = '370c849b-0e00-42ac-be9d-3b4c719ac31a'
        then answer_value_text
      else null
      end
    )
    
      
            as "370c849b-0e00-42ac-be9d-3b4c719ac31a"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-spi'
        then answer_value_text
      else null
      end
    )
    
      
            as "a11ebb0d-acb3-4038-956b-293a41acb85b-spi"
      
    
    ,
  
    max(
      
      case
      when linkid = 'df53c378-27fe-483d-a630-f7b171f72980'
        then answer_value_text
      else null
      end
    )
    
      
            as "df53c378-27fe-483d-a630-f7b171f72980"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd1c2fae1-f0f5-4333-97f6-2220d2ff3d95'
        then answer_value_text
      else null
      end
    )
    
      
            as "d1c2fae1-f0f5-4333-97f6-2220d2ff3d95"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ac3206dd-b60b-4cb1-a559-3a770a685668'
        then answer_value_text
      else null
      end
    )
    
      
            as "ac3206dd-b60b-4cb1-a559-3a770a685668"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f39df336-a0b8-4d0b-9bd2-6c4284985491'
        then answer_value_text
      else null
      end
    )
    
      
            as "f39df336-a0b8-4d0b-9bd2-6c4284985491"
      
    
    ,
  
    max(
      
      case
      when linkid = 'faefef23-d2e6-4777-b628-8f83921e8fd8'
        then answer_value_text
      else null
      end
    )
    
      
            as "faefef23-d2e6-4777-b628-8f83921e8fd8"
      
    
    ,
  
    max(
      
      case
      when linkid = '0c308fef-8ce5-4217-a8d4-8e6ff00c8e14'
        then answer_value_text
      else null
      end
    )
    
      
            as "0c308fef-8ce5-4217-a8d4-8e6ff00c8e14"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a57b5b34-12e5-4ec8-8215-7f316267323a'
        then answer_value_text
      else null
      end
    )
    
      
            as "a57b5b34-12e5-4ec8-8215-7f316267323a"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cb75bba9-0bd8-4dce-ba2f-d1b21a97c2a0'
        then answer_value_text
      else null
      end
    )
    
      
            as "cb75bba9-0bd8-4dce-ba2f-d1b21a97c2a0"
      
    
    ,
  
    max(
      
      case
      when linkid = '2e03fdf1-5274-494d-88e9-dd4b41ec90e6'
        then answer_value_text
      else null
      end
    )
    
      
            as "2e03fdf1-5274-494d-88e9-dd4b41ec90e6"
      
    
    ,
  
    max(
      
      case
      when linkid = '62b0eb17-46e5-4c50-a133-087cc190a123'
        then answer_value_text
      else null
      end
    )
    
      
            as "62b0eb17-46e5-4c50-a133-087cc190a123"
      
    
    ,
  
    max(
      
      case
      when linkid = '991ba845-38e5-4622-8528-dfeefbf20d3d'
        then answer_value_text
      else null
      end
    )
    
      
            as "991ba845-38e5-4622-8528-dfeefbf20d3d"
      
    
    ,
  
    max(
      
      case
      when linkid = '83dfd82f-0277-459d-abe3-e8795750f288'
        then answer_value_text
      else null
      end
    )
    
      
            as "83dfd82f-0277-459d-abe3-e8795750f288"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b36fdd5f-ccac-457c-a8ea-f9497204ef1d'
        then answer_value_text
      else null
      end
    )
    
      
            as "b36fdd5f-ccac-457c-a8ea-f9497204ef1d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f7513e58-a495-49d7-a549-ba4d84fb5003'
        then answer_value_text
      else null
      end
    )
    
      
            as "f7513e58-a495-49d7-a549-ba4d84fb5003"
      
    
    ,
  
    max(
      
      case
      when linkid = '101d2b7f-2b68-407d-aa49-30cb4dadc888'
        then answer_value_text
      else null
      end
    )
    
      
            as "101d2b7f-2b68-407d-aa49-30cb4dadc888"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a5d96f90-cd91-4a5b-8862-c4e3f88694af'
        then answer_value_text
      else null
      end
    )
    
      
            as "a5d96f90-cd91-4a5b-8862-c4e3f88694af"
      
    
    ,
  
    max(
      
      case
      when linkid = '9ea8d78b-3288-4ec8-8288-459c03df64f0'
        then answer_value_text
      else null
      end
    )
    
      
            as "9ea8d78b-3288-4ec8-8288-459c03df64f0"
      
    
    ,
  
    max(
      
      case
      when linkid = '2b96f1d9-3450-423d-a277-060fcfdadc6b'
        then answer_value_text
      else null
      end
    )
    
      
            as "2b96f1d9-3450-423d-a277-060fcfdadc6b"
      
    
    ,
  
    max(
      
      case
      when linkid = '9aeae470-6ea4-4e7f-9644-ed0375fb6369'
        then answer_value_text
      else null
      end
    )
    
      
            as "9aeae470-6ea4-4e7f-9644-ed0375fb6369"
      
    
    ,
  
    max(
      
      case
      when linkid = '176aeeb0-e173-482a-b3eb-60f59bc04f98'
        then answer_value_text
      else null
      end
    )
    
      
            as "176aeeb0-e173-482a-b3eb-60f59bc04f98"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-4'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-4"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-6a-review'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-6a-review"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-1-review'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-1-review"
      
    
    ,
  
    max(
      
      case
      when linkid = '53072f88-a81c-4f18-a49f-f85f64fb5be7'
        then answer_value_text
      else null
      end
    )
    
      
            as "53072f88-a81c-4f18-a49f-f85f64fb5be7"
      
    
    ,
  
    max(
      
      case
      when linkid = '4d334b16-ea16-4299-94ed-32e84ef97021'
        then answer_value_text
      else null
      end
    )
    
      
            as "4d334b16-ea16-4299-94ed-32e84ef97021"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cf16ca6e-8b1b-467d-b807-fc5d50339f71'
        then answer_value_text
      else null
      end
    )
    
      
            as "cf16ca6e-8b1b-467d-b807-fc5d50339f71"
      
    
    ,
  
    max(
      
      case
      when linkid = '9f531e61-26cf-4223-8653-39c24d6dd478'
        then answer_value_text
      else null
      end
    )
    
      
            as "9f531e61-26cf-4223-8653-39c24d6dd478"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f311f3ca-5537-4134-8828-5e537bb26a9c'
        then answer_value_text
      else null
      end
    )
    
      
            as "f311f3ca-5537-4134-8828-5e537bb26a9c"
      
    
    ,
  
    max(
      
      case
      when linkid = '9b1b11e8-608f-4ea9-911b-88aae467ac27'
        then answer_value_text
      else null
      end
    )
    
      
            as "9b1b11e8-608f-4ea9-911b-88aae467ac27"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a25cb682-be35-4909-965e-01b8c2d036a2'
        then answer_value_text
      else null
      end
    )
    
      
            as "a25cb682-be35-4909-965e-01b8c2d036a2"
      
    
    ,
  
    max(
      
      case
      when linkid = 'calculated-month'
        then answer_value_text
      else null
      end
    )
    
      
            as "calculated-month"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e45b778d-ee8c-4ee9-81f4-8927006160f9'
        then answer_value_text
      else null
      end
    )
    
      
            as "e45b778d-ee8c-4ee9-81f4-8927006160f9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cce2de12-cf62-44ff-84e8-9eb4d864d048'
        then answer_value_text
      else null
      end
    )
    
      
            as "cce2de12-cf62-44ff-84e8-9eb4d864d048"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1fef9d7-2ab9-4ead-9c24-c4dc9c7ed4cc'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1fef9d7-2ab9-4ead-9c24-c4dc9c7ed4cc"
      
    
    ,
  
    max(
      
      case
      when linkid = '5151ea93-a0f2-478d-adb6-f31374107b52'
        then answer_value_text
      else null
      end
    )
    
      
            as "5151ea93-a0f2-478d-adb6-f31374107b52"
      
    
    ,
  
    max(
      
      case
      when linkid = '72e5c1b7-7ce9-49bd-834b-d98b73ba7090'
        then answer_value_text
      else null
      end
    )
    
      
            as "72e5c1b7-7ce9-49bd-834b-d98b73ba7090"
      
    
    ,
  
    max(
      
      case
      when linkid = '5846d53f-171d-47c4-89ad-4016c3be8233'
        then answer_value_text
      else null
      end
    )
    
      
            as "5846d53f-171d-47c4-89ad-4016c3be8233"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e552afd3-2816-446e-9ff5-e898bc813660'
        then answer_value_text
      else null
      end
    )
    
      
            as "e552afd3-2816-446e-9ff5-e898bc813660"
      
    
    ,
  
    max(
      
      case
      when linkid = '8a5b1c7d-e3ef-4443-85f1-fa5213d89f8d'
        then answer_value_text
      else null
      end
    )
    
      
            as "8a5b1c7d-e3ef-4443-85f1-fa5213d89f8d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c6c266cc-1c85-41c0-97ce-9ab491be45db'
        then answer_value_text
      else null
      end
    )
    
      
            as "c6c266cc-1c85-41c0-97ce-9ab491be45db"
      
    
    ,
  
    max(
      
      case
      when linkid = '3ab931b8-6bfd-4e5f-b86b-3c7377907331'
        then answer_value_text
      else null
      end
    )
    
      
            as "3ab931b8-6bfd-4e5f-b86b-3c7377907331"
      
    
    ,
  
    max(
      
      case
      when linkid = 'calculated-year'
        then answer_value_text
      else null
      end
    )
    
      
            as "calculated-year"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fefdc90f-fe1d-4bfb-af93-94df8c482897'
        then answer_value_text
      else null
      end
    )
    
      
            as "fefdc90f-fe1d-4bfb-af93-94df8c482897"
      
    
    ,
  
    max(
      
      case
      when linkid = '83661ebe-71cf-4eab-88e3-bfecbfe35164'
        then answer_value_text
      else null
      end
    )
    
      
            as "83661ebe-71cf-4eab-88e3-bfecbfe35164"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fd1498e5-abf7-4ebe-9252-97994d6b9449'
        then answer_value_text
      else null
      end
    )
    
      
            as "fd1498e5-abf7-4ebe-9252-97994d6b9449"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-6a'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-6a"
      
    
    ,
  
    max(
      
      case
      when linkid = '92da95cc-50dd-4bf0-acca-4465c07d44cc'
        then answer_value_text
      else null
      end
    )
    
      
            as "92da95cc-50dd-4bf0-acca-4465c07d44cc"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cebffdf4-8fd1-4829-884c-41cc42bc0111'
        then answer_value_text
      else null
      end
    )
    
      
            as "cebffdf4-8fd1-4829-884c-41cc42bc0111"
      
    
    ,
  
    max(
      
      case
      when linkid = '52ef2ff5-ced6-418a-b9e7-f2ef47123df5'
        then answer_value_text
      else null
      end
    )
    
      
            as "52ef2ff5-ced6-418a-b9e7-f2ef47123df5"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e9c42647-eefb-42bd-9080-f83630729606'
        then answer_value_text
      else null
      end
    )
    
      
            as "e9c42647-eefb-42bd-9080-f83630729606"
      
    
    ,
  
    max(
      
      case
      when linkid = '13e2f934-82ed-4b23-af6e-95523dc3746a'
        then answer_value_text
      else null
      end
    )
    
      
            as "13e2f934-82ed-4b23-af6e-95523dc3746a"
      
    
    ,
  
    max(
      
      case
      when linkid = '8891cd7b-ad1c-4030-afe4-6752cd273cf4'
        then answer_value_text
      else null
      end
    )
    
      
            as "8891cd7b-ad1c-4030-afe4-6752cd273cf4"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cece37bc-897b-4b48-a9c9-59ecc9f997f7'
        then answer_value_text
      else null
      end
    )
    
      
            as "cece37bc-897b-4b48-a9c9-59ecc9f997f7"
      
    
    ,
  
    max(
      
      case
      when linkid = '58721b3d-88ae-4888-b5ec-ef7dfe30fb3d'
        then answer_value_text
      else null
      end
    )
    
      
            as "58721b3d-88ae-4888-b5ec-ef7dfe30fb3d"
      
    
    ,
  
    max(
      
      case
      when linkid = '895fad45-c3b5-47c5-9edf-5f9b9e793fbf'
        then answer_value_text
      else null
      end
    )
    
      
            as "895fad45-c3b5-47c5-9edf-5f9b9e793fbf"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a33cdecd-b7e6-45d8-94b6-52fa80c64a6d'
        then answer_value_text
      else null
      end
    )
    
      
            as "a33cdecd-b7e6-45d8-94b6-52fa80c64a6d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-1'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-1"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f14c3c34-f80a-4481-9d9c-9b05267d4c39'
        then answer_value_text
      else null
      end
    )
    
      
            as "f14c3c34-f80a-4481-9d9c-9b05267d4c39"
      
    
    ,
  
    max(
      
      case
      when linkid = '82d6b50b-0bb2-40f6-94b1-7a4c58df43f9'
        then answer_value_text
      else null
      end
    )
    
      
            as "82d6b50b-0bb2-40f6-94b1-7a4c58df43f9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'encounter-id'
        then answer_value_text
      else null
      end
    )
    
      
            as "encounter-id"
      
    
    ,
  
    max(
      
      case
      when linkid = '1a00c7b7-e80d-4e90-8ed4-efd8f6d96268'
        then answer_value_text
      else null
      end
    )
    
      
            as "1a00c7b7-e80d-4e90-8ed4-efd8f6d96268"
      
    
    ,
  
    max(
      
      case
      when linkid = '07c2e8dc-b48b-4bc7-b21b-c196cce32709'
        then answer_value_text
      else null
      end
    )
    
      
            as "07c2e8dc-b48b-4bc7-b21b-c196cce32709"
      
    
    ,
  
    max(
      
      case
      when linkid = 'dfa1f239-2a4c-40bd-ab15-561f6d87febf'
        then answer_value_text
      else null
      end
    )
    
      
            as "dfa1f239-2a4c-40bd-ab15-561f6d87febf"
      
    
    ,
  
    max(
      
      case
      when linkid = '53932020-eed8-46f4-8236-d548a6335a59'
        then answer_value_text
      else null
      end
    )
    
      
            as "53932020-eed8-46f4-8236-d548a6335a59"
      
    
    ,
  
    max(
      
      case
      when linkid = '8a1155e2-6c49-4f82-a9fc-2807ee298410'
        then answer_value_text
      else null
      end
    )
    
      
            as "8a1155e2-6c49-4f82-a9fc-2807ee298410"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ce18eee3-aa64-4423-8db1-50fe7f00850d'
        then answer_value_text
      else null
      end
    )
    
      
            as "ce18eee3-aa64-4423-8db1-50fe7f00850d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c37407c5-003a-4b7c-b6a3-e10ef146a47b'
        then answer_value_text
      else null
      end
    )
    
      
            as "c37407c5-003a-4b7c-b6a3-e10ef146a47b"
      
    
    ,
  
    max(
      
      case
      when linkid = '39de8b74-5cbc-4225-b27b-c97e72f08af3'
        then answer_value_text
      else null
      end
    )
    
      
            as "39de8b74-5cbc-4225-b27b-c97e72f08af3"
      
    
    ,
  
    max(
      
      case
      when linkid = 'bfe595d6-325f-492a-b3b6-cc5541e18c3f'
        then answer_value_text
      else null
      end
    )
    
      
            as "bfe595d6-325f-492a-b3b6-cc5541e18c3f"
      
    
    ,
  
    max(
      
      case
      when linkid = '2cedd4dd-15e5-4951-8674-ea4d4eeb7cfc'
        then answer_value_text
      else null
      end
    )
    
      
            as "2cedd4dd-15e5-4951-8674-ea4d4eeb7cfc"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a320b787-4cba-457f-9c60-67ff76c7020f'
        then answer_value_text
      else null
      end
    )
    
      
            as "a320b787-4cba-457f-9c60-67ff76c7020f"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e9e056c2-0469-4523-a28f-1000fed1a4f1'
        then answer_value_text
      else null
      end
    )
    
      
            as "e9e056c2-0469-4523-a28f-1000fed1a4f1"
      
    
    ,
  
    max(
      
      case
      when linkid = '527c0166-cc35-4cc3-a9d9-7eeb0a54635e'
        then answer_value_text
      else null
      end
    )
    
      
            as "527c0166-cc35-4cc3-a9d9-7eeb0a54635e"
      
    
    ,
  
    max(
      
      case
      when linkid = '7d8621a4-dcc8-4f48-9210-3fb7d0dcc3c7'
        then answer_value_text
      else null
      end
    )
    
      
            as "7d8621a4-dcc8-4f48-9210-3fb7d0dcc3c7"
      
    
    ,
  
    max(
      
      case
      when linkid = '5324bdce-baaa-45bf-935a-eb2f65bd7124'
        then answer_value_text
      else null
      end
    )
    
      
            as "5324bdce-baaa-45bf-935a-eb2f65bd7124"
      
    
    ,
  
    max(
      
      case
      when linkid = '544a444d-b101-4d56-9351-00bca8ec749e'
        then answer_value_text
      else null
      end
    )
    
      
            as "544a444d-b101-4d56-9351-00bca8ec749e"
      
    
    ,
  
    max(
      
      case
      when linkid = '13321778-b5d4-4b01-8eda-610cf0007c86'
        then answer_value_text
      else null
      end
    )
    
      
            as "13321778-b5d4-4b01-8eda-610cf0007c86"
      
    
    ,
  
    max(
      
      case
      when linkid = '90cedcbf-bf11-4dc3-9733-42e6900a09fc'
        then answer_value_text
      else null
      end
    )
    
      
            as "90cedcbf-bf11-4dc3-9733-42e6900a09fc"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c0655b11-8f21-47ed-8ede-95498c2e8200'
        then answer_value_text
      else null
      end
    )
    
      
            as "c0655b11-8f21-47ed-8ede-95498c2e8200"
      
    
    ,
  
    max(
      
      case
      when linkid = '15e5007d-028f-4644-8fb6-0346730c3101'
        then answer_value_text
      else null
      end
    )
    
      
            as "15e5007d-028f-4644-8fb6-0346730c3101"
      
    
    ,
  
    max(
      
      case
      when linkid = '8c1ac4fe-1d76-4436-8f34-d315c6c17b9c'
        then answer_value_text
      else null
      end
    )
    
      
            as "8c1ac4fe-1d76-4436-8f34-d315c6c17b9c"
      
    
    ,
  
    max(
      
      case
      when linkid = '87dab385-fe6b-44ce-b6b2-5771c41d82c8'
        then answer_value_text
      else null
      end
    )
    
      
            as "87dab385-fe6b-44ce-b6b2-5771c41d82c8"
      
    
    ,
  
    max(
      
      case
      when linkid = '40fbe8d0-4380-4efc-bb04-26fc7d3714eb'
        then answer_value_text
      else null
      end
    )
    
      
            as "40fbe8d0-4380-4efc-bb04-26fc7d3714eb"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c3b4cf6e-385d-40b7-9380-4de2f81463de'
        then answer_value_text
      else null
      end
    )
    
      
            as "c3b4cf6e-385d-40b7-9380-4de2f81463de"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a842d271-cd48-47c7-963e-dcd0376ef185'
        then answer_value_text
      else null
      end
    )
    
      
            as "a842d271-cd48-47c7-963e-dcd0376ef185"
      
    
    ,
  
    max(
      
      case
      when linkid = '9f3a622a-d39b-49b0-84e7-0bcf4f832a4f'
        then answer_value_text
      else null
      end
    )
    
      
            as "9f3a622a-d39b-49b0-84e7-0bcf4f832a4f"
      
    
    ,
  
    max(
      
      case
      when linkid = '69d5aec4-65d3-44b1-99ba-badd201769c5'
        then answer_value_text
      else null
      end
    )
    
      
            as "69d5aec4-65d3-44b1-99ba-badd201769c5"
      
    
    ,
  
    max(
      
      case
      when linkid = '6bbdb2cf-e7f6-443a-8f4e-841e16bb9b75'
        then answer_value_text
      else null
      end
    )
    
      
            as "6bbdb2cf-e7f6-443a-8f4e-841e16bb9b75"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f8861b1c-781f-4f12-b4c8-f8110f0c2cc0'
        then answer_value_text
      else null
      end
    )
    
      
            as "f8861b1c-781f-4f12-b4c8-f8110f0c2cc0"
      
    
    ,
  
    max(
      
      case
      when linkid = '53a575e3-aa59-408d-a0dd-8df237b68bf7'
        then answer_value_text
      else null
      end
    )
    
      
            as "53a575e3-aa59-408d-a0dd-8df237b68bf7"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a3670632-ce8f-468a-b2f3-6934245d3531'
        then answer_value_text
      else null
      end
    )
    
      
            as "a3670632-ce8f-468a-b2f3-6934245d3531"
      
    
    ,
  
    max(
      
      case
      when linkid = '8a9424b0-c893-4371-b5c9-bc0008be14b4'
        then answer_value_text
      else null
      end
    )
    
      
            as "8a9424b0-c893-4371-b5c9-bc0008be14b4"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fba783e8-91d0-4a49-ba80-c55fa9e41a54'
        then answer_value_text
      else null
      end
    )
    
      
            as "fba783e8-91d0-4a49-ba80-c55fa9e41a54"
      
    
    ,
  
    max(
      
      case
      when linkid = '437d9ea2-adb5-471a-828b-a0285a282217'
        then answer_value_text
      else null
      end
    )
    
      
            as "437d9ea2-adb5-471a-828b-a0285a282217"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cecb89ae-abce-411c-b4ce-f84ebf59ca14'
        then answer_value_text
      else null
      end
    )
    
      
            as "cecb89ae-abce-411c-b4ce-f84ebf59ca14"
      
    
    ,
  
    max(
      
      case
      when linkid = 'beb4ad5f-2fd8-4b80-aa93-aa9ff1896dd0'
        then answer_value_text
      else null
      end
    )
    
      
            as "beb4ad5f-2fd8-4b80-aa93-aa9ff1896dd0"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ea95a315-f903-4351-87ed-c1e7e7b74198'
        then answer_value_text
      else null
      end
    )
    
      
            as "ea95a315-f903-4351-87ed-c1e7e7b74198"
      
    
    ,
  
    max(
      
      case
      when linkid = '98407058-4855-44cc-ac45-6e0d0c072fdd'
        then answer_value_text
      else null
      end
    )
    
      
            as "98407058-4855-44cc-ac45-6e0d0c072fdd"
      
    
    ,
  
    max(
      
      case
      when linkid = '705df2d9-af92-4f4f-b513-17e01d07b609'
        then answer_value_text
      else null
      end
    )
    
      
            as "705df2d9-af92-4f4f-b513-17e01d07b609"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ff115c26-95b9-4e0d-87ed-80817d438fc7'
        then answer_value_text
      else null
      end
    )
    
      
            as "ff115c26-95b9-4e0d-87ed-80817d438fc7"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fdd7c3c3-f2c3-43bd-8a80-6b48aa2b9604'
        then answer_value_text
      else null
      end
    )
    
      
            as "fdd7c3c3-f2c3-43bd-8a80-6b48aa2b9604"
      
    
    ,
  
    max(
      
      case
      when linkid = '28b2e31a-ee4d-4e99-8974-b616145311eb'
        then answer_value_text
      else null
      end
    )
    
      
            as "28b2e31a-ee4d-4e99-8974-b616145311eb"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e4a1be29-fe8b-4423-91bc-6b4f5d730e6b'
        then answer_value_text
      else null
      end
    )
    
      
            as "e4a1be29-fe8b-4423-91bc-6b4f5d730e6b"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-2-review'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-2-review"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd617b1e1-ea22-4863-a878-627f6b81963e'
        then answer_value_text
      else null
      end
    )
    
      
            as "d617b1e1-ea22-4863-a878-627f6b81963e"
      
    
    ,
  
    max(
      
      case
      when linkid = '5a79866d-f1fb-4bd8-b4c4-f18c982bef3c'
        then answer_value_text
      else null
      end
    )
    
      
            as "5a79866d-f1fb-4bd8-b4c4-f18c982bef3c"
      
    
    ,
  
    max(
      
      case
      when linkid = '2a6c5feb-8034-418b-8644-142a5e31df63'
        then answer_value_text
      else null
      end
    )
    
      
            as "2a6c5feb-8034-418b-8644-142a5e31df63"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd3340d7e-9b84-4559-8a11-bed5a662a6f2'
        then answer_value_text
      else null
      end
    )
    
      
            as "d3340d7e-9b84-4559-8a11-bed5a662a6f2"
      
    
    ,
  
    max(
      
      case
      when linkid = '08b5e17c-5e14-4b19-ac93-9e479cd2a2bb'
        then answer_value_text
      else null
      end
    )
    
      
            as "08b5e17c-5e14-4b19-ac93-9e479cd2a2bb"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ee5d4acf-ecf0-4e62-8924-cff17a33679d'
        then answer_value_text
      else null
      end
    )
    
      
            as "ee5d4acf-ecf0-4e62-8924-cff17a33679d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd4ebe784-07d0-4312-8a46-634c0e338a54'
        then answer_value_text
      else null
      end
    )
    
      
            as "d4ebe784-07d0-4312-8a46-634c0e338a54"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd8615ac7-96b2-46a2-b3a5-b5b83945b444'
        then answer_value_text
      else null
      end
    )
    
      
            as "d8615ac7-96b2-46a2-b3a5-b5b83945b444"
      
    
    ,
  
    max(
      
      case
      when linkid = 'db779cb5-5335-4eb6-8ccc-47ec94c626c8'
        then answer_value_text
      else null
      end
    )
    
      
            as "db779cb5-5335-4eb6-8ccc-47ec94c626c8"
      
    
    ,
  
    max(
      
      case
      when linkid = '603d1492-316f-497f-967b-5c4b1ab983ea'
        then answer_value_text
      else null
      end
    )
    
      
            as "603d1492-316f-497f-967b-5c4b1ab983ea"
      
    
    ,
  
    max(
      
      case
      when linkid = '6a273a98-82ca-4d36-ade0-ce98f7899972'
        then answer_value_text
      else null
      end
    )
    
      
            as "6a273a98-82ca-4d36-ade0-ce98f7899972"
      
    
    ,
  
    max(
      
      case
      when linkid = '06f9231a-999b-4459-8260-6fbbcdfc5d81'
        then answer_value_text
      else null
      end
    )
    
      
            as "06f9231a-999b-4459-8260-6fbbcdfc5d81"
      
    
    ,
  
    max(
      
      case
      when linkid = 'other-signs-step-1'
        then answer_value_text
      else null
      end
    )
    
      
            as "other-signs-step-1"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b7e42cdb-a42b-4d22-b7a6-204de5305351'
        then answer_value_text
      else null
      end
    )
    
      
            as "b7e42cdb-a42b-4d22-b7a6-204de5305351"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-response-step-3-review'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-response-step-3-review"
      
    
    ,
  
    max(
      
      case
      when linkid = '0b247120-23cf-45fc-9e4d-d959f0dda79f'
        then answer_value_text
      else null
      end
    )
    
      
            as "0b247120-23cf-45fc-9e4d-d959f0dda79f"
      
    
    ,
  
    max(
      
      case
      when linkid = 'd1efad02-3686-4ce3-a96d-68d8c638ff77'
        then answer_value_text
      else null
      end
    )
    
      
            as "d1efad02-3686-4ce3-a96d-68d8c638ff77"
      
    
    ,
  
    max(
      
      case
      when linkid = '20cae99e-ab90-4d75-92a2-c0af5dd60942'
        then answer_value_text
      else null
      end
    )
    
      
            as "20cae99e-ab90-4d75-92a2-c0af5dd60942"
      
    
    ,
  
    max(
      
      case
      when linkid = '12d89dfd-5e43-4f00-8446-b3af23323969'
        then answer_value_text
      else null
      end
    )
    
      
            as "12d89dfd-5e43-4f00-8446-b3af23323969"
      
    
    ,
  
    max(
      
      case
      when linkid = '62a04638-8928-4aab-b490-1a33739b7a5c'
        then answer_value_text
      else null
      end
    )
    
      
            as "62a04638-8928-4aab-b490-1a33739b7a5c"
      
    
    ,
  
    max(
      
      case
      when linkid = '965b7874-dd24-4215-acd4-8d241a262638'
        then answer_value_text
      else null
      end
    )
    
      
            as "965b7874-dd24-4215-acd4-8d241a262638"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a6d066a2-0286-49b0-b688-34696e7fb256'
        then answer_value_text
      else null
      end
    )
    
      
            as "a6d066a2-0286-49b0-b688-34696e7fb256"
      
    
    ,
  
    max(
      
      case
      when linkid = '2f44652f-df2d-476e-933d-75b6ed55f5f2'
        then answer_value_text
      else null
      end
    )
    
      
            as "2f44652f-df2d-476e-933d-75b6ed55f5f2"
      
    
    ,
  
    max(
      
      case
      when linkid = '6f68c416-a5de-4f68-8ad0-33c6e85a9a68'
        then answer_value_text
      else null
      end
    )
    
      
            as "6f68c416-a5de-4f68-8ad0-33c6e85a9a68"
      
    
    ,
  
    max(
      
      case
      when linkid = '1895c0a7-b090-4f49-9b0c-08dca9a7b225'
        then answer_value_text
      else null
      end
    )
    
      
            as "1895c0a7-b090-4f49-9b0c-08dca9a7b225"
      
    
    ,
  
    max(
      
      case
      when linkid = '2c7b6bd7-36f6-495b-9f41-eb492c07c7b2'
        then answer_value_text
      else null
      end
    )
    
      
            as "2c7b6bd7-36f6-495b-9f41-eb492c07c7b2"
      
    
    ,
  
    max(
      
      case
      when linkid = '36dd1467-a7a1-428c-bdca-17d352a79904'
        then answer_value_text
      else null
      end
    )
    
      
            as "36dd1467-a7a1-428c-bdca-17d352a79904"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ce0fdd41-6b09-4104-9998-a04c5dd9a5b7'
        then answer_value_text
      else null
      end
    )
    
      
            as "ce0fdd41-6b09-4104-9998-a04c5dd9a5b7"
      
    
    ,
  
    max(
      
      case
      when linkid = '68608473-77e6-4341-9f1b-d4d18b17d3de'
        then answer_value_text
      else null
      end
    )
    
      
            as "68608473-77e6-4341-9f1b-d4d18b17d3de"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a1c56f60-b823-4c15-b99d-a315d2ca1871'
        then answer_value_text
      else null
      end
    )
    
      
            as "a1c56f60-b823-4c15-b99d-a315d2ca1871"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94'
        then answer_value_text
      else null
      end
    )
    
      
            as "b5bc7f80-4a0c-486c-e5eb-32c750036f94"
      
    
    ,
  
    max(
      
      case
      when linkid = 'dfaddf2d-30a3-4f8f-b2fa-ffa675daa5c9'
        then answer_value_text
      else null
      end
    )
    
      
            as "dfaddf2d-30a3-4f8f-b2fa-ffa675daa5c9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a20b29af-3317-415e-9c6e-d9f3bbc4df33'
        then answer_value_text
      else null
      end
    )
    
      
            as "a20b29af-3317-415e-9c6e-d9f3bbc4df33"
      
    
    ,
  
    max(
      
      case
      when linkid = '11aadd88-0cf9-48b2-939d-2b3e28191c5f'
        then answer_value_text
      else null
      end
    )
    
      
            as "11aadd88-0cf9-48b2-939d-2b3e28191c5f"
      
    
    ,
  
    max(
      
      case
      when linkid = '68fa6f2e-84ec-4dbc-900e-10ede297c488'
        then answer_value_text
      else null
      end
    )
    
      
            as "68fa6f2e-84ec-4dbc-900e-10ede297c488"
      
    
    ,
  
    max(
      
      case
      when linkid = '1d0ad825-09de-4018-a680-cd0e6bf0983d'
        then answer_value_text
      else null
      end
    )
    
      
            as "1d0ad825-09de-4018-a680-cd0e6bf0983d"
      
    
    ,
  
    max(
      
      case
      when linkid = '1fc8f8e7-2022-48d2-b41d-4e8ad22af344'
        then answer_value_text
      else null
      end
    )
    
      
            as "1fc8f8e7-2022-48d2-b41d-4e8ad22af344"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ce56c71d-9ea4-4c8e-9e9a-d1da93bac1d4'
        then answer_value_text
      else null
      end
    )
    
      
            as "ce56c71d-9ea4-4c8e-9e9a-d1da93bac1d4"
      
    
    
  

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

  
  


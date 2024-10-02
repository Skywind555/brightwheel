{{ config(
    materialized='table',
    tags=['clean']
) }}

WITH typed_source1 AS (

    SELECT
    LOWER(name) AS name,
    LOWER(credential_type) AS credential_type,  
    REPLACE(credential_number, '-', '') AS credential_number,  
    LOWER(status) AS status,  
    PARSE_DATE('%m/%d/%y', expiration_date) AS expiration_date,  
    LOWER(disciplinary_action) AS disciplinary_action,  
    LOWER(address) AS address,  
    LOWER(state) AS state,  
    LOWER(county) AS county,  
    address AS zip, 
    phone, 
    PARSE_DATE('%m/%d/%y', first_issue_date) AS first_issue_date,  
    LOWER(primary_contact_name) AS primary_contact_name,
    LOWER(primary_contact_role) AS primary_contact_role 
  FROM `{{ var('project_id') }}.{{ var('dataset_name') }}.raw_source1`
),

source1_with_city AS (
  SELECT
    t1.*,
    t2.city  
  FROM typed_source1 AS t1
  LEFT JOIN {{ ref('county_to_city_mapping') }} AS t2
    ON t1.county = t2.county
)

SELECT  DISTINCT
  CAST(NULL AS STRING) AS accepts_financial_aid, 
  CAST(NULL AS STRING) AS ages_served,            
  CAST(NULL AS NUMERIC) AS capacity,               
  expiration_date AS certificate_expiration_date,
  city,
  address AS address1,
  CAST(NULL AS STRING) AS address2,               
  name AS company,
  REPLACE(phone, '-', '') AS phone,
  CAST(NULL AS STRING) AS phone2,                 
  county,
  CASE WHEN name LIKE '%montessori%' THEN 'montessori' ELSE 'other' END AS curriculum_type,
  CAST(NULL AS STRING) AS email,                  
  CAST(NULL AS STRING) AS first_name,             
  'english' AS language,               
  CAST(NULL AS STRING) AS last_name,              
  status AS license_status,
  first_issue_date AS license_issued,
  CAST(REPLACE(credential_number, '-', '') AS NUMERIC) AS license_number,
  CAST(NULL AS DATE) AS license_renewed,        
  CASE WHEN credential_type in ('family care', 'accomodation') THEN 'family care' ELSE 'care center' END AS license_type,
  CAST(NULL AS STRING) AS licensee_name,          
  CAST(NULL AS NUMERIC)  AS max_age,                
  CAST(NULL AS NUMERIC)  AS min_age,                
  primary_contact_name AS operator,
  CAST(NULL AS STRING) AS provider_id,            
  CAST(NULL AS STRING) AS schedule,               
  state,
  primary_contact_role AS title,
  CAST(NULL AS STRING) AS website_address,        
  RIGHT(zip, 5) AS zip,
  CASE WHEN name LIKE '%family%' OR name LIKE '%child care%' OR name LIKE '%childcare' THEN 'family care' 
       ELSE 'care center' END AS facility_type
FROM source1_with_city

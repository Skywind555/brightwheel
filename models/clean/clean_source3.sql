{{ config(
    materialized='table',
    tags=['clean']
) }}

WITH typed_source3 AS (
  SELECT
    operation,  
    agency_number,  
    LOWER(operation_name) AS operation_name,  
    LOWER(address) AS address,  
    LOWER(city) AS city,  
    LOWER(state) AS state, 
    zip,  
    LOWER(county) AS county, 
    phone, 
    LOWER(type) AS type,  
    LOWER(status) AS status,  
    PARSE_DATE('%m/%d/%Y', issue_date) AS issue_date,  
    capacity,  
    LOWER(email_address) AS email_address,  
    facility_id,  
    LOWER(monitoring_frequency) AS monitoring_frequency, 
    LOWER(infant) AS infant,  
    LOWER(toddler) AS toddler,  
    LOWER(preschool) AS preschool,  
    LOWER(school) AS school 
  FROM `{{ var('project_id') }}.{{ var('dataset_name') }}.raw_source3`
),

ages_served_mapping AS (

SELECT *,
CASE WHEN infant = 'y' THEN  'Less than 1 year' ELSE NULL END AS infant_age,
CASE WHEN toddler = 'y' THEN '1-2 years' ELSE NULL END AS toddler_age,
CASE WHEN preschool = 'y' THEN '2-4 years' ELSE NULL END AS preschool_age,
CASE WHEN school = 'y' THEN '5+ years' ELSE NULL END AS school_age

FROM typed_source3

)

SELECT DISTINCT
  CAST(NULL AS STRING) AS accepts_financial_aid,  
  CONCAT(infant_age, ' | ', toddler_age, ' | ', preschool_age, ' | ', school_age) AS ages_served,
  CAST(capacity AS NUMERIC) AS capacity,
  CAST(NULL AS DATE) AS certificate_expiration_date,  
  city,
  address AS address1,
  CAST(NULL AS STRING) AS address2,               
  operation_name AS company,
  REPLACE(phone, '-', '') AS phone,
  CAST(NULL AS STRING) AS phone2,                 
  county,
  CASE WHEN operation_name LIKE '%montessori%' THEN 'montessori' ELSE 'other' END AS curriculum_type,
  email_address AS email,
  CAST(NULL AS STRING) AS first_name,             
  'english' AS language,               
  CAST(NULL AS STRING) AS last_name,              
  status AS license_status,
  DATE(issue_date) AS license_issued,
  CAST(operation AS NUMERIC) AS license_number,
  CAST(NULL AS DATE) AS license_renewed,        
  CASE WHEN SPLIT(type, '-')[SAFE_OFFSET(0)] LIKE '%center%' THEN 'care center' ELSE 'child care' END AS license_type,
  CAST(NULL AS STRING) AS licensee_name,          
  CASE WHEN school = 'y' THEN 8
       WHEN preschool = 'y' THEN 4
       WHEN toddler = 'y' THEN 2
       WHEN infant = 'y' THEN 1 ELSE NULL END AS max_age,
  CASE WHEN infant = 'y' THEN 0
       WHEN toddler = 'y' THEN 1
       WHEN preschool = 'y' THEN 2
       WHEN school = 'y' THEN 5 ELSE NULL END AS min_age,
  CAST(NULL AS STRING) AS operator,               
  CAST(NULL AS STRING) AS provider_id,            
  CAST(NULL AS STRING) AS schedule,               
  CAST(NULL AS STRING) AS state,                  
  CAST(NULL AS STRING) AS title,                  
  CAST(NULL AS STRING) AS website_address,        
  CAST(NULL AS STRING) AS zip,                    
  CASE WHEN SPLIT(type, '-')[SAFE_OFFSET(0)] LIKE '%center%' THEN 'care center' ELSE 'child care' END AS facility_type
FROM ages_served_mapping

{{ config(
    materialized='table',
    tags=['clean']
) }}

WITH typed_source2 AS (
  SELECT
    LOWER(type_license) AS type_license,  
    LOWER(company) AS company,  
    LOWER(accepts_subsidy) AS accepts_financial_aid, 
    LOWER(year_round) AS year_round,  
    LOWER(daytime_hours) AS daytime_hours,  
    LOWER(star_level) AS star_level,  
    LOWER(mon) AS monday, 
    LOWER(tues) AS tuesday, 
    LOWER(wed) AS wednesday,  
    LOWER(thurs) AS thursday,  
    LOWER(friday) AS friday, 
    LOWER(saturday) AS saturday,  
    LOWER(sunday) AS sunday, 
    LOWER(primary_caregiver) AS primary_caregiver,  
    phone,  
    LOWER(email) AS email,  
    LOWER(address1) AS address1,  
    LOWER(address2) AS address2,  
    LOWER(city) AS city,  
    LOWER(state) AS state, 
    zip, 
    LOWER(subsidy_contract_number) AS subsidy_contract_number,  
    CAST(total_cap AS NUMERIC) AS capacity,  
    LOWER(ages_accepted_1) AS ages_accepted_1,  
    LOWER(aa2) AS aa2,  
    LOWER(aa3) AS aa3,  
    LOWER(aa4) AS aa4,

    DATE(RIGHT(license_monitoring_since, 10)) AS license_issued,
    
    LOWER(school_year_only) AS school_year_only,  
    LOWER(evening_hours) AS evening_hours 


  FROM `{{ var('project_id') }}.{{ var('dataset_name') }}.raw_source2`
),

mapping AS (

SELECT
  accepts_financial_aid,
  CONCAT(ages_accepted_1, ' | ', aa2, ' | ', aa3, ' | ', aa4) AS ages_served,
  capacity,
  address1,
  address2,
  company,
  REGEXP_REPLACE(phone, r'[\(\)-]', '') AS phone,
  email,
  CASE WHEN license_issued IS NOT NULL THEN 'active' ELSE 'inactive' END AS license_status,
  license_issued,
  CASE WHEN type_license LIKE '%family%' THEN 'family care' ELSE 'care center' END AS license_type,
  CAST(SPLIT(type_license, ' - K')[SAFE_OFFSET(1)] AS NUMERIC) AS license_number,

  SPLIT(primary_caregiver,'zzz')[SAFE_OFFSET(0)] AS licensee_name,
  CONCAT('Monday: ', monday, ' | Tuesday: ', tuesday, ' | Wednesday: ', 
        wednesday, ' | Thursday: ', thursday, ' | Friday: ', friday,
        ' | Saturday: ', saturday, ' | Sunday: ', sunday) AS schedule,
  state,
  SPLIT(primary_caregiver,'zzz')[SAFE_OFFSET(1)] AS title,
  zip,
  CASE WHEN company LIKE '%center%' THEN 'care center' ELSE 'family care' END AS facility_type 

FROM typed_source2 
),

get_names AS (

SELECT *,
SPLIT(licensee_name, ' ')[SAFE_OFFSET(0)] AS first_name,
SPLIT(licensee_name, ' ')[SAFE_OFFSET(1)] AS last_name,
licensee_name AS operator
FROM mapping
)

SELECT DISTINCT
  accepts_financial_aid,
  ages_served,
  capacity,
  CAST(NULL AS DATE) AS certificate_expiration_date,  
  CAST(NULL AS STRING) AS city,                         
  address1,
  address2,
  company,
  phone,
  CAST(NULL AS STRING) AS phone2,                       
  CAST(NULL AS STRING) AS county,                       
  CAST(NULL AS STRING) AS curriculum_type,              
  email,
  first_name,
  'english' AS language,                     
  last_name,
  license_status,
  license_issued,
  license_number,
  CAST(NULL AS DATE) AS license_renewed,              
  license_type,
  licensee_name,
  CASE WHEN ages_served LIKE '%school-age%' THEN 8
       WHEN ages_served LIKE '%preschool%' THEN 4
       WHEN ages_served LIKE '%toddler%'  THEN 2
       WHEN ages_served LIKE '%infant%' THEN 1 ELSE NULL END AS max_age,
  CASE WHEN ages_served LIKE '%infant%' THEN 0
       WHEN ages_served LIKE '%toddler%' THEN 1
       WHEN ages_served LIKE '%preschool%'  THEN 2
       WHEN ages_served LIKE '%school-age%' THEN 5 ELSE NULL END AS min_age,
  operator,
  CAST(NULL AS STRING) AS provider_id,                  
  schedule,
  state,
  title,
  CAST(NULL AS STRING) AS website_address,              
  zip,
  facility_type
FROM get_names



{{ config(
    materialized='table',
    tags=['clean']
) }}

WITH typed_salesforce_leads AS (
  SELECT
    CAST(LOWER(id) AS STRING) AS id,  
    CAST(LOWER(is_deleted) AS BOOLEAN) AS is_deleted,
    CAST(LOWER(last_name) AS STRING) AS last_name,
    CAST(LOWER(first_name) AS STRING) AS first_name,
    CAST(LOWER(title) AS STRING) AS title,
    CAST(LOWER(company) AS STRING) AS company,
    CAST(LOWER(street) AS STRING) AS street,
    CAST(LOWER(city) AS STRING) AS city,
    CAST(LOWER(state) AS STRING) AS state,
    CAST(LOWER(postal_code) AS STRING) AS zip,
    CAST(LOWER(country) AS STRING) AS country,
    CAST(LOWER(phone) AS STRING) AS phone,
    CAST(LOWER(mobile_phone) AS STRING) AS mobile_phone,
    CAST(LOWER(email) AS STRING) AS email,
    CAST(LOWER(website) AS STRING) AS website,
    CAST(LOWER(lead_source) AS STRING) AS lead_source,
    CAST(LOWER(status) AS STRING) AS status,
    CAST(LOWER(is_converted) AS BOOLEAN) AS is_converted,  
    CAST(created_date AS TIMESTAMP) AS created_date,  
    CAST(last_modified_date AS TIMESTAMP) AS last_modified_date,
    CAST(last_activity_date AS TIMESTAMP) AS last_activity_date,
    CAST(last_viewed_date AS TIMESTAMP) AS last_viewed_date,
    CAST(last_referenced_date AS TIMESTAMP) AS last_referenced_date,
    CAST(LOWER(email_bounced_reason) AS STRING) AS email_bounced_reason,
    CAST(email_bounced_date AS TIMESTAMP) AS email_bounced_date,  
    CAST(LOWER(outreach_stage_c) AS STRING) AS outreach_stage_c,
    CAST(LOWER(current_enrollment_c) AS INTEGER) AS current_enrollment_c,  
    CAST(capacity_c AS INTEGER) AS capacity,  
    CAST(lead_source_last_updated_c AS TIMESTAMP) AS lead_source_last_updated_c,  
    CAST(LOWER(brightwheel_school_uuid_c) AS STRING) AS brightwheel_school_uuid_c 
  FROM `{{ var('project_id') }}.{{ var('dataset_name') }}.raw_salesforce_leads`
),

standardized_states AS (

SELECT t.*, s.state_short
FROM typed_salesforce_leads t
LEFT JOIN {{ ref('state_abbreviations') }} s ON LOWER(s.state) = t.state

)

SELECT DISTINCT
  CAST(NULL AS STRING) AS accepts_financial_aid,  
  CAST(NULL AS STRING) AS ages_served,            
  CAST(capacity AS NUMERIC) AS capacity,
  CAST(NULL AS DATE) AS certificate_expiration_date,  
  city,
  street AS address1,
  CAST(NULL AS STRING) AS address2,              
  company,
  phone AS phone,
  mobile_phone AS phone2,
  CAST(NULL AS STRING) AS county,                 
  CASE WHEN company LIKE '%montessori%' THEN 'montessori' ELSE 'other' END AS curriculum_type,
  email,
  first_name,
  'english' AS language,              
  last_name,
  CAST(NULL AS STRING) AS license_status,
  CAST(NULL AS DATE) AS license_issued,
  CAST(NULL AS NUMERIC) AS license_number,
  CAST(NULL AS DATE) AS license_renewed,
  CAST(NULL AS STRING) AS license_type,
  CAST(NULL AS STRING) AS licensee_name,
  CAST(NULL AS NUMERIC) AS max_age,
  CAST(NULL AS NUMERIC)  AS min_age,
  CAST(NULL AS STRING) AS operator,
  CAST(NULL AS STRING) AS provider_id, 
  CAST(NULL AS STRING) AS schedule,         
  CASE WHEN state_short IS NULL THEN state ELSE state_short END AS state,
  title,
  website AS website_address,
  LEFT(zip, 5) AS zip,
  CASE WHEN company LIKE '%family%' OR company LIKE '%child care%' OR company LIKE '%childcare' THEN 'family care' 
       ELSE 'care center' END AS facility_type
FROM standardized_states


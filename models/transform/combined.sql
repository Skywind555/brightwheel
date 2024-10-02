{{ config(
    materialized='table',
    tags=['transform']
) }}

SELECT * FROM {{ ref('clean_salesforce_leads') }}

UNION ALL

SELECT * FROM {{ ref('clean_source1') }}

UNION ALL

SELECT * FROM {{ ref('clean_source2') }}

UNION ALL

SELECT * FROM {{ ref('clean_source3') }}

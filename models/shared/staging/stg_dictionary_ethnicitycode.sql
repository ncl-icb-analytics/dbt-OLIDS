-- Staging model for dictionary.EthnicityCode
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_EthnicityID" as sk_ethnicityid,
    "EthnicityCodeType" as ethnicitycodetype,
    "EthnicCategoryCode" as ethniccategorycode,
    "EthnicGroupCode" as ethnicgroupcode,
    "ICCode" as iccode,
    "PDSEthnicCategoryCode" as pdsethniccategorycode,
    "ReadCode" as readcode,
    "SDECode" as sdecode,
    "Description" as description,
    "Priority" as priority,
    "Snomed" as snomed
from {{ source('dictionary', 'EthnicityCode') }}

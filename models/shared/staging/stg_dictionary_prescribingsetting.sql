-- Staging model for dictionary.PrescribingSetting
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_PrescribingSettingID" as sk_prescribingsettingid,
    "BK_PrescribingSetting" as bk_prescribingsetting,
    "PrescribingSetting" as prescribingsetting
from {{ source('dictionary', 'PrescribingSetting') }}

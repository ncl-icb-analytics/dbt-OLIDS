-- Staging model for dictionary.Urgent_Emergency_Care_Activity_Type
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_Urgent_Emergency_Care_Activity_Type_ID" as sk_urgent_emergency_care_activity_type_id,
    "BK_Urgent_Emergency_Care_Activity_Type_Code" as bk_urgent_emergency_care_activity_type_code,
    "Urgent_Emergency_Care_Activity_Type_Description" as urgent_emergency_care_activity_type_description,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Urgent_Emergency_Care_Activity_Type') }}

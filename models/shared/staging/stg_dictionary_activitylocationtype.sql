-- Staging model for dictionary.ActivityLocationType
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ActivityLocationTypeID" as sk_activitylocationtypeid,
    "BK_ActivityLocationTypeCode" as bk_activitylocationtypecode,
    "ActivityLocationTypeCategory" as activitylocationtypecategory,
    "ActivityLocationTypeDescription" as activitylocationtypedescription,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'ActivityLocationType') }}

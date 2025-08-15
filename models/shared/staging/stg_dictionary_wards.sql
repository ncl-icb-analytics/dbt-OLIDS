-- Staging model for dictionary.Wards
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_WardID" as sk_wardid,
    "WardCode" as wardcode,
    "WardName" as wardname,
    "GeoCentroid_text" as geocentroid_text,
    "GeoCentroid" as geocentroid,
    "WardShape_text" as wardshape_text,
    "WardShape" as wardshape,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'Wards') }}

-- Staging model for dictionary.OverseasVisitorStatusClassification
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OverseasVisitorStatusClassificationID" as sk_overseasvisitorstatusclassificationid,
    "BK_OverseasVisitorStatusClassification" as bk_overseasvisitorstatusclassification,
    "OverseasVisitorStatusClassification" as overseasvisitorstatusclassification
from {{ source('dictionary', 'OverseasVisitorStatusClassification') }}

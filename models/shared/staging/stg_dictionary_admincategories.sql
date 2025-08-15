-- Staging model for dictionary.AdminCategories
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_AdminCategoryID" as sk_admincategoryid,
    "BK_AdminCategoryCode" as bk_admincategorycode,
    "AdminCategoryName" as admincategoryname,
    "AdminCategoryFullName" as admincategoryfullname,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated,
    "BK_AdminCategoryCode_TinyInt" as bk_admincategorycode_tinyint
from {{ source('dictionary', 'AdminCategories') }}

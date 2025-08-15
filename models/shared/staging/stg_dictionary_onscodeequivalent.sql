-- Staging model for dictionary.ONSCodeEquivalent
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "Geography_Code" as geography_code,
    "Geography_Name" as geography_name,
    "Geography_Name_Welsh" as geography_name_welsh,
    "ONS_Geography_Code" as ons_geography_code,
    "ONS_Geography_Name" as ons_geography_name,
    "DCLG_Geography_Code" as dclg_geography_code,
    "DCLG_Geography_Name" as dclg_geography_name,
    "DH_Geography_Code" as dh_geography_code,
    "DH_Geography_Name" as dh_geography_name,
    "Scottish_Geography_Code" as scottish_geography_code,
    "Scottish_Geography_Name" as scottish_geography_name,
    "NI_Geography_Code" as ni_geography_code,
    "NI_Geography_Name" as ni_geography_name,
    "WG_Geography_Code" as wg_geography_code,
    "WG_Geography_Name" as wg_geography_name,
    "WG_Geography_Name_Welsh" as wg_geography_name_welsh,
    "Entity_Code" as entity_code,
    "Status" as status,
    "Date_Of_Introduction" as date_of_introduction,
    "Date_Of_Termination" as date_of_termination,
    "Created_Date" as created_date,
    "Import_Date" as import_date
from {{ source('dictionary', 'ONSCodeEquivalent') }}

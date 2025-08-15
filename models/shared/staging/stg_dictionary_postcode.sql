-- Staging model for dictionary.Postcode
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_PostcodeID" as sk_postcodeid,
    "Postcode_8_chars" as postcode_8_chars,
    "Postcode_single_space_e_Gif" as postcode_single_space_e_gif,
    "Postcode_no_space" as postcode_no_space,
    "Date_of_Introduction" as date_of_introduction,
    "Date_of_Termination" as date_of_termination,
    "Grid_Ref_Easting" as grid_ref_easting,
    "Grid_Ref_Northing" as grid_ref_northing,
    "Local_Authority_District_Unitary_Authority" as local_authority_district_unitary_authority,
    "Electoral_Ward_or_Division" as electoral_ward_or_division,
    "Strategic_Health_Authority" as strategic_health_authority,
    "Primary_Care_Organisation" as primary_care_organisation,
    "yr1998_Ward_Code" as yr1998_ward_code,
    "Old_PCT" as old_pct,
    "yr2001_LSOA" as yr2001_lsoa,
    "yr2001_MSOA" as yr2001_msoa,
    "yr2011_OA" as yr2011_oa,
    "yr2011_LSOA" as yr2011_lsoa,
    "yr2011_MSOA" as yr2011_msoa,
    "Latitude" as latitude,
    "Longitude" as longitude,
    "FirstCreated" as firstcreated,
    "LastUpdated" as lastupdated,
    "GOR_Code" as gor_code,
    "Postcode_Usertype" as postcode_usertype,
    "Area_Team_Code" as area_team_code,
    "Postcode" as postcode,
    "LSOA" as lsoa,
    "MSOA" as msoa,
    "SK_PseudoPostcodeID" as sk_pseudopostcodeid,
    "SK_Postcode_ID" as sk_postcode_id
from {{ source('dictionary', 'Postcode') }}

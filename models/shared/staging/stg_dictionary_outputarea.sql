-- Staging model for dictionary.OutputArea
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_OutputAreaID" as sk_outputareaid,
    "SK_OutputAreaParentID" as sk_outputareaparentid,
    "CensusYear" as censusyear,
    "OACode" as oacode,
    "OAName" as oaname,
    "OAType" as oatype,
    "GeoEasting" as geoeasting,
    "GeoNorthing" as geonorthing,
    "GeoLatitude" as geolatitude,
    "GeoLongitude" as geolongitude,
    "GeoCentroid_text" as geocentroid_text,
    "GeoCentroid" as geocentroid,
    "PopEasting" as popeasting,
    "PopNorthing" as popnorthing,
    "PopLatitude" as poplatitude,
    "PopLongitude" as poplongitude,
    "PopCentroid_text" as popcentroid_text,
    "PopCentroid" as popcentroid,
    "OAShape_text" as oashape_text,
    "OAShape" as oashape,
    "TownsendScore" as townsendscore
from {{ source('dictionary', 'OutputArea') }}

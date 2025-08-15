-- Staging model for dictionary.PODTeams
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_PODTeamID" as sk_podteamid,
    "PODTeamCode" as podteamcode,
    "PODTeamName" as podteamname,
    "SK_ServiceProviderGroupID" as sk_serviceprovidergroupid,
    "IsTestOrganisation" as istestorganisation,
    "Region" as region
from {{ source('dictionary', 'PODTeams') }}

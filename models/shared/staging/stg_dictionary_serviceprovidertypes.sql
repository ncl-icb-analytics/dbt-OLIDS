-- Staging model for dictionary.ServiceProviderTypes
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_ServiceProviderTypeID" as sk_serviceprovidertypeid,
    "ServiceProviderTypeDescription" as serviceprovidertypedescription
from {{ source('dictionary', 'ServiceProviderTypes') }}

-- Staging model for dictionary.AgeBand_GP
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_AgeBandGPID" as sk_agebandgpid,
    "BK_AgeBandGP" as bk_agebandgp,
    "AgeBandStarts" as agebandstarts,
    "AgeBandEnds" as agebandends,
    "CreatedDateTime" as createddatetime,
    "LastUpdateDateTime" as lastupdatedatetime,
    "SK_AgeBandID" as sk_agebandid,
    "BK_AgeBand" as bk_ageband
from {{ source('dictionary', 'AgeBand_GP') }}

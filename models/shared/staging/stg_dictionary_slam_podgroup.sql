-- Staging model for dictionary.SLAM_PODGroup
-- Source: "Dictionary"."dbo"
-- Description: Reference data including lookups and terminology mappings

select
    "SK_SLAMPODGroupID" as sk_slampodgroupid,
    "Trust_Code" as trust_code,
    "POD_Code" as pod_code,
    "CSU_POD_Code" as csu_pod_code,
    "SLAMHRGCode" as slamhrgcode,
    "SLAMSpecCode" as slamspeccode,
    "PBR_NPBR" as pbr_npbr,
    "SBSCostCentre" as sbscostcentre,
    "AdHoc_Code" as adhoc_code,
    "CCG_Code" as ccg_code,
    "Site_Code" as site_code,
    "Phasing" as phasing,
    "DateCreated" as datecreated,
    "BeginMonth" as beginmonth,
    "EndMonth" as endmonth,
    "BeginYear" as beginyear,
    "EndYear" as endyear
from {{ source('dictionary', 'SLAM_PODGroup') }}

/*
Flu Code Clusters Staging
Source: seeds/flu_code_clusters.csv

Clinical code cluster definitions for flu programme rules.
These clusters define which clinical codes belong to each rule group
and how they should be queried (date qualifiers).

Clusters are reusable across campaign years unless UKHSA updates clinical codes.
*/

SELECT 
    rule_group_id,
    cluster_id,
    data_source_type,
    date_qualifier,
    cluster_description
FROM {{ ref('flu_code_clusters') }}
ORDER BY rule_group_id, cluster_id
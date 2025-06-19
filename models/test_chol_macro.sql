-- Test what macro returns for cholesterol
SELECT 
    observation_id,
    COUNT(*) as count,
    STRING_AGG(DISTINCT cluster_id, ', ') as clusters,
    STRING_AGG(DISTINCT mapped_concept_code, ', ') as codes
FROM ({{ get_observations("'CHOL2_COD'") }})
GROUP BY observation_id
HAVING COUNT(*) > 1
ORDER BY count DESC
LIMIT 10; 
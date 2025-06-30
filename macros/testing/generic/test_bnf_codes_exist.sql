{% test bnf_codes_exist(model, bnf_codes) %}
    -- Generic test to verify that BNF codes used exist in BNF codeset
    WITH required_bnf AS (
        SELECT TRIM(value) AS bnf_code
        FROM TABLE(SPLIT_TO_TABLE(
            '{{ bnf_codes }}',
            ','
        ))
    )
    SELECT
        rb.bnf_code,
        'BNF code not found in codesets_bnf_latest' AS failure_reason
    FROM required_bnf rb
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ ref('stg_codesets_bnf_latest') }} bnf
        WHERE rb.bnf_code LIKE bnf.bnf_code || '%'
    )
{% endtest %}

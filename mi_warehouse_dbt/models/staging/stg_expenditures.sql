{{ config(materialized='external', location='../data/staging/stg_expenditures.parquet', format='parquet') }}

WITH raw_expenditures AS (
    SELECT
        cfr_com_id AS cfr_committee_id,
        com_legal_name AS committee_legal_name,
        common_name AS committee_common_name,
        com_type AS committee_type,
        doc_stmnt_year AS doc_statement_year,
        doc_seq_no AS document_sequence_number,
        page_no AS page_number,
        expenditure_type,
        expense_id AS expenditure_id,
        detail_id,
        exp_desc AS expenditure_description,
        purpose AS expenditure_purpose,
        extra_desc AS expenditure_extra_description,
        f_name AS expenditure_first_name,
        lname_or_org AS expenditure_last_name_or_org,
        address AS expenditure_address,
        city AS expenditure_city,
        state AS expenditure_state,
        zip AS expenditure_zip,
        try_strptime(exp_date, '%m/%d/%Y') AS expenditure_date,
        CASE
            WHEN try_cast(amount AS FLOAT) IS NULL THEN NULL ELSE
                round(try_cast(amount AS FLOAT), 2)
        END AS expenditure_amount
    FROM
        read_parquet(
            '../data/raw/*_mi_cfr_expenditures.parquet', union_by_name = TRUE
        )
),

output AS (
    SELECT
        *,
        date_part('year', expenditure_date) AS expenditure_year,
        date_part('month', expenditure_date) AS expenditure_month
    FROM raw_expenditures
)

SELECT * FROM output

{{ config(materialized='view', location='../data/staging/stg_contributions.parquet', format='parquet') }}

WITH raw_contributions AS (
    SELECT
        cfr_com_id AS cfr_committee_id,
        com_legal_name AS committee_legal_name,
        common_name AS committee_common_name,
        com_type AS committee_type,
        doc_stmnt_year AS doc_statement_year,
        doc_seq_no AS document_sequence_number,
        page_no AS page_number,
        contribution_id,
        contribtype AS contribution_type,
        f_name AS donor_first_name,
        l_name_or_org AS donor_last_name_or_org_name,
        occupation AS donor_occupation,
        employer,
        state AS donor_state,
        city AS donor_city,
        zip AS donor_zip,
        address AS donor_address,
        extra_desc AS donation_extra_description,
        CASE WHEN cont_detail_id = 0 THEN NULL ELSE cont_detail_id END
            AS contribution_detail_id,
        try_strptime(received_date, '%m/%d/%Y') AS contribution_received_date,
        round(try_cast(amount AS FLOAT), 2) AS contribution_amount
    FROM
        read_parquet(
            '../data/raw/mi_cfr/parquet/*_mi_cfr_contributions*.parquet', union_by_name = TRUE
        )
),

output AS (
    SELECT
        *,
        date_part('year', contribution_received_date) AS donation_received_year,
        date_part('month', contribution_received_date)
            AS donation_received_month,
        concat(
            cfr_committee_id,
            doc_statement_year,
            document_sequence_number,
            page_number,
            contribution_id,
            contribution_detail_id
        ) AS unique_line_identifier
    FROM raw_contributions
)

SELECT * FROM output

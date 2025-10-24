"""
Compare registration counts between PDS registry and OLIDS source data.
Validates that OLIDS episode of care data aligns with PDS registration records.
Uses sk_patient_id for counting as person_id is incomplete in source.
"""

import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

# Load environment variables
load_dotenv()

# Configuration
OLIDS_DATABASE = '"Data_Store_OLIDS_Alpha"'
PDS_DATABASE = '"Data_Store_Registries"'
DICTIONARY_DATABASE = '"Dictionary"'
TARGET_DATE = '2025-09-01'
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
ROLE = os.getenv('SNOWFLAKE_ROLE')


def get_pds_registrations(session):
    """Get current registrations from PDS registry data."""
    query = f"""
    SELECT
        "REG"."Primary Care Provider" AS practice_code,
        Prac."Organisation_Name" AS practice_name,
        ICB."Organisation_Name" AS icb_name,
        COUNT(*) AS pds_unmerged_persons,
        COUNT(DISTINCT COALESCE("MERG"."Pseudo Superseded NHS Number", "REG"."Pseudo NHS Number")) AS pds_merged_persons
    FROM {PDS_DATABASE}."pds"."PDS_Patient_Care_Practice" "REG"
    LEFT JOIN {PDS_DATABASE}."pds"."PDS_Person_Merger" "MERG"
        ON "REG"."Pseudo NHS Number" = "MERG"."Pseudo NHS Number"
    LEFT JOIN {PDS_DATABASE}."pds"."PDS_Person" "PER"
        ON "REG"."Pseudo NHS Number" = "PER"."Pseudo NHS Number"
        AND "PER"."Person Business Effective From Date" <= COALESCE("REG"."Primary Care Provider Business Effective To Date", '9999-12-31')
        AND COALESCE("PER"."Person Business Effective To Date", '9999-12-31') >= "REG"."Primary Care Provider Business Effective From Date"
        AND '{TARGET_DATE}' BETWEEN
            "PER"."Person Business Effective From Date"
            AND COALESCE("PER"."Person Business Effective To Date", '9999-12-31')
    LEFT JOIN {PDS_DATABASE}."pds"."PDS_Reason_For_Removal" "REAS"
        ON "REG"."Pseudo NHS Number" = "REAS"."Pseudo NHS Number"
        AND "REAS"."Reason for Removal Business Effective From Date" <= COALESCE("REG"."Primary Care Provider Business Effective To Date", '9999-12-31')
        AND COALESCE("REAS"."Reason for Removal Business Effective To Date", '9999-12-31') >= "REG"."Primary Care Provider Business Effective From Date"
        AND '{TARGET_DATE}' BETWEEN
            "REAS"."Reason for Removal Business Effective From Date"
            AND COALESCE("REAS"."Reason for Removal Business Effective To Date", '9999-12-31')
    INNER JOIN {DICTIONARY_DATABASE}."dbo"."Organisation" Prac
        ON "REG"."Primary Care Provider" = Prac."Organisation_Code"
    INNER JOIN {DICTIONARY_DATABASE}."dbo"."Organisation" ICB
        ON Prac."SK_ParentOrg_ID" = ICB."SK_OrganisationID"
        AND ICB."Organisation_Code" = '93C'
        AND Prac."EndDate" IS NULL
    WHERE "PER"."Death Status" IS NULL
        AND "PER"."Date of Death" IS NULL
        AND "REG"."Pseudo NHS Number" IS NOT NULL
        AND '{TARGET_DATE}' BETWEEN
            "REG"."Primary Care Provider Business Effective From Date"
            AND COALESCE("REG"."Primary Care Provider Business Effective To Date", '9999-12-31')
        AND "REAS"."Reason for Removal" IS NULL
    GROUP BY
        "REG"."Primary Care Provider",
        Prac."Organisation_Name",
        ICB."Organisation_Name"
    ORDER BY pds_merged_persons DESC
    """

    return session.sql(query).collect()


def get_olids_registrations(session):
    """Get current registrations from OLIDS source data using episode of care."""
    query = f"""
    WITH patient_death_date AS (
        SELECT
            id AS patient_id,
            death_year,
            death_month,
            death_year IS NOT NULL AS is_deceased,
            CASE
                WHEN death_year IS NOT NULL AND death_month IS NOT NULL
                    THEN DATEADD(
                        DAY,
                        FLOOR(DAY(LAST_DAY(TO_DATE(death_year || '-' || death_month || '-01'))) / 2),
                        TO_DATE(death_year || '-' || death_month || '-01')
                    )
            END AS death_date_approx
        FROM {OLIDS_DATABASE}.OLIDS_MASKED.PATIENT
    ),

    episode_type_registration AS (
        SELECT DISTINCT c.id AS concept_id
        FROM {OLIDS_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT c
        WHERE c.code = '24531000000104'  -- Registration type
            AND c.display = 'Registration type'
    ),

    patient_episodes AS (
        SELECT
            eoc.patient_id,
            eoc.organisation_id,
            eoc.episode_of_care_start_date,
            eoc.episode_of_care_end_date,
            p.sk_patient_id,
            pdd.is_deceased,
            pdd.death_date_approx
        FROM {OLIDS_DATABASE}.OLIDS_COMMON.EPISODE_OF_CARE eoc
        INNER JOIN {OLIDS_DATABASE}.OLIDS_MASKED.PATIENT p
            ON eoc.patient_id = p.id
        LEFT JOIN patient_death_date pdd
            ON eoc.patient_id = pdd.patient_id
        -- Join through concept map to get episode type
        LEFT JOIN {OLIDS_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT_MAP cm
            ON eoc.episode_type_source_concept_id = cm.source_code_id
        INNER JOIN episode_type_registration etr
            ON cm.target_code_id = etr.concept_id
        WHERE eoc.episode_of_care_start_date IS NOT NULL
            AND eoc.patient_id IS NOT NULL
            AND eoc.organisation_id IS NOT NULL
            AND p.sk_patient_id IS NOT NULL
            -- Episode active on target date (must start before/on target date)
            AND eoc.episode_of_care_start_date <= '{TARGET_DATE}'
            -- Episode not ended, or ended after target date
            AND (
                eoc.episode_of_care_end_date IS NULL
                OR eoc.episode_of_care_end_date > '{TARGET_DATE}'
            )
            -- Patient not deceased, or deceased after target date
            AND (
                NOT pdd.is_deceased
                OR pdd.death_date_approx IS NULL
                OR pdd.death_date_approx > '{TARGET_DATE}'
            )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                p.sk_patient_id,
                eoc.organisation_id
            ORDER BY eoc.episode_of_care_start_date DESC, eoc.id
        ) = 1
    )
    SELECT
        o.organisation_code AS practice_code,
        o.name AS practice_name,
        COUNT(DISTINCT pe.sk_patient_id) AS olids_registered_patients
    FROM patient_episodes pe
    INNER JOIN {OLIDS_DATABASE}.OLIDS_COMMON.ORGANISATION o
        ON pe.organisation_id = o.id
    WHERE o.organisation_code IS NOT NULL
    GROUP BY
        o.organisation_code,
        o.name
    ORDER BY olids_registered_patients DESC
    """

    return session.sql(query).collect()


def main():
    """Execute registration count comparison."""
    print(f"\n{'='*100}")
    print(f"REGISTRATION COUNT COMPARISON")
    print(f"{'='*100}")
    print(f"Target Date: {TARGET_DATE}")
    print(f"PDS Database: {PDS_DATABASE}")
    print(f"OLIDS Database: {OLIDS_DATABASE}")

    print(f"\nConnecting to Snowflake...")

    # Create Snowpark session with SSO authentication
    connection_parameters = {
        "account": ACCOUNT,
        "user": USER,
        "authenticator": "externalbrowser",
        "warehouse": WAREHOUSE,
        "role": ROLE
    }

    session = Session.builder.configs(connection_parameters).create()
    print("✓ Connected successfully")

    try:
        # Get PDS registrations
        print(f"\nQuerying PDS registrations...")
        pds_results = get_pds_registrations(session)
        print(f"✓ Found {len(pds_results)} practices in PDS")

        # Convert to dict for easy lookup
        pds_by_practice = {}
        for row in pds_results:
            pds_by_practice[row['PRACTICE_CODE']] = {
                'practice_name': row['PRACTICE_NAME'],
                'icb_name': row['ICB_NAME'],
                'pds_unmerged': int(row['PDS_UNMERGED_PERSONS']),
                'pds_merged': int(row['PDS_MERGED_PERSONS'])
            }

        # Get OLIDS registrations
        print(f"Querying OLIDS registrations...")
        olids_results = get_olids_registrations(session)
        print(f"✓ Found {len(olids_results)} practices in OLIDS\n")

        # Convert to dict for easy lookup
        olids_by_practice = {}
        for row in olids_results:
            olids_by_practice[row['PRACTICE_CODE']] = {
                'practice_name': row['PRACTICE_NAME'],
                'olids_count': int(row['OLIDS_REGISTERED_PATIENTS'])
            }

        # Compare and display results
        print("="*100)
        print("COMPARISON RESULTS")
        print("="*100)

        # Get all practice codes
        all_practices = set(pds_by_practice.keys()) | set(olids_by_practice.keys())

        # Practices in both systems
        matched = []
        # Practices only in PDS
        pds_only = []
        # Practices only in OLIDS
        olids_only = []

        for practice_code in sorted(all_practices):
            pds_data = pds_by_practice.get(practice_code)
            olids_data = olids_by_practice.get(practice_code)

            if pds_data and olids_data:
                diff = olids_data['olids_count'] - pds_data['pds_merged']
                diff_pct = (diff / pds_data['pds_merged'] * 100) if pds_data['pds_merged'] > 0 else 0

                matched.append({
                    'practice_code': practice_code,
                    'practice_name': pds_data['practice_name'],
                    'icb_name': pds_data['icb_name'],
                    'pds_count': pds_data['pds_merged'],
                    'olids_count': olids_data['olids_count'],
                    'difference': diff,
                    'diff_pct': diff_pct
                })
            elif pds_data:
                pds_only.append({
                    'practice_code': practice_code,
                    'practice_name': pds_data['practice_name'],
                    'icb_name': pds_data['icb_name'],
                    'pds_count': pds_data['pds_merged']
                })
            else:
                olids_only.append({
                    'practice_code': practice_code,
                    'practice_name': olids_data['practice_name'],
                    'olids_count': olids_data['olids_count']
                })

        # Display matched practices
        if matched:
            # Categorise by difference percentage
            major_diff = [p for p in matched if abs(p['diff_pct']) >= 20]  # 20%+ difference
            minor_diff = [p for p in matched if 5 <= abs(p['diff_pct']) < 20]  # 5-20% difference
            good_match = [p for p in matched if abs(p['diff_pct']) < 5]  # <5% difference

            # Sort each category by absolute difference
            major_diff_sorted = sorted(major_diff, key=lambda x: abs(x['difference']), reverse=True)
            minor_diff_sorted = sorted(minor_diff, key=lambda x: abs(x['difference']), reverse=True)
            good_match_sorted = sorted(good_match, key=lambda x: abs(x['difference']), reverse=True)

            # Display major differences
            if major_diff:
                print(f"\n⚠️  MAJOR DIFFERENCES (±20% or more) - {len(major_diff)} practices:\n")
                for p in major_diff_sorted:
                    print(f"  {p['practice_code']} - {p['practice_name']}")
                    print(f"    PDS: {p['pds_count']:,} | OLIDS: {p['olids_count']:,} | Diff: {p['difference']:+,} ({p['diff_pct']:+.1f}%)")
                    print()

            # Display minor differences
            if minor_diff:
                print(f"\n⚠️  MINOR DIFFERENCES (5-20%) - {len(minor_diff)} practices:\n")
                for p in minor_diff_sorted:
                    print(f"  {p['practice_code']} - {p['practice_name']}")
                    print(f"    PDS: {p['pds_count']:,} | OLIDS: {p['olids_count']:,} | Diff: {p['difference']:+,} ({p['diff_pct']:+.1f}%)")
                    print()

            # Display good matches
            if good_match:
                print(f"\n✓ GOOD MATCHES (<5% difference) - {len(good_match)} practices:\n")
                for p in good_match_sorted:
                    print(f"  {p['practice_code']} - {p['practice_name']}")
                    print(f"    PDS: {p['pds_count']:,} | OLIDS: {p['olids_count']:,} | Diff: {p['difference']:+,} ({p['diff_pct']:+.1f}%)")
                    print()

        # Display PDS-only practices
        if pds_only:
            print(f"\n⚠️  PRACTICES IN PDS ONLY ({len(pds_only)} practices):\n")
            for p in pds_only:  # Show all
                print(f"  {p['practice_code']} - {p['practice_name']}")
                print(f"    PDS: {p['pds_count']:,}")
                print()

        # Display OLIDS-only practices
        if olids_only:
            print(f"\n⚠️  PRACTICES IN OLIDS ONLY ({len(olids_only)} practices):\n")
            for p in olids_only:  # Show all
                print(f"  {p['practice_code']} - {p['practice_name']}")
                print(f"    OLIDS: {p['olids_count']:,}")
                print()

        # Summary statistics
        print("\n" + "="*100)
        print("SUMMARY")
        print("="*100)

        total_pds = sum(p['pds_count'] for p in matched)
        total_olids = sum(p['olids_count'] for p in matched)
        total_diff = total_olids - total_pds
        total_diff_pct = (total_diff / total_pds * 100) if total_pds > 0 else 0

        print(f"\nMatched practices: {len(matched)}")
        if matched:
            major_diff = [p for p in matched if abs(p['diff_pct']) >= 20]
            minor_diff = [p for p in matched if 5 <= abs(p['diff_pct']) < 20]
            good_match = [p for p in matched if abs(p['diff_pct']) < 5]
            print(f"  Major differences (±20%+): {len(major_diff)}")
            print(f"  Minor differences (5-20%): {len(minor_diff)}")
            print(f"  Good matches (<5%): {len(good_match)}")
        print(f"\nPDS only: {len(pds_only)}")
        print(f"OLIDS only: {len(olids_only)}")
        print(f"\nTotal registrations (matched practices):")
        print(f"  PDS: {total_pds:,}")
        print(f"  OLIDS: {total_olids:,}")
        print(f"  Difference: {total_diff:+,} ({total_diff_pct:+.2f}%)")

    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()

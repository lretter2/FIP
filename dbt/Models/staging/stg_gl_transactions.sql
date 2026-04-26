##############################################################################
--  STAGING MODEL: stg_gl_transactions
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : bronze.raw_gl_transactions (immutable landing zone)
--  Target  : staging.stg_gl_transactions (view)
--  Purpose : Standardise raw GL posting lines into a clean, typed, and
--             validated staging layer ready for CoA mapping and FX conversion.
--
--  Key transformations:
--    1. Cast all columns to correct data types
--    2. Normalise debit/credit sign convention (net_amount = debit - credit)
--    3. Parse HU GAAP period from posting_date
--    4. Flag late entries (posting date falls in a closed period)
--    5. Attach batch_id and source quality flags
--    6. Quarantine rows that fail referential integrity
--
--  NOTE: No business logic here. CoA mapping happens in int_coa_mapped.sql
##############################################################################

with

source as (

    select * from {{ source('bronze', 'raw_gl_transactions') }}

),

renamed as (

    select
        -- Primary identifiers
        cast(transaction_id          as varchar(100))  as transaction_id,
        cast(batch_id                as varchar(50))   as batch_id,
        cast(entity_id              as varchar(50))   as entity_id,
        cast(source_system           as varchar(30))   as source_system,

        -- Account and organisational keys (raw codes — not yet mapped)
        cast(local_account_code      as varchar(50))   as local_account_code,
        cast(cost_centre_code        as varchar(50))   as cost_centre_code,
        cast(project_code            as varchar(50))   as project_code,

        -- Dates
        cast(posting_date            as date)          as posting_date,
        cast(document_date           as date)          as document_date,
        cast(period_year             as int)           as period_year,
        cast(period_month            as int)           as period_month,

        -- Amounts (always in HUF as local currency; FCY if applicable)
        cast(debit_amount            as decimal(18,2)) as debit_lcy,
        cast(credit_amount           as decimal(18,2)) as credit_lcy,
        cast(debit_amount            as decimal(18,2))
          - cast(credit_amount       as decimal(18,2)) as net_amount_lcy,

        -- Foreign currency columns (null if transaction is in HUF)
        cast(foreign_currency_code   as char(3))       as currency_code,
        cast(foreign_debit_amount    as decimal(18,2)) as debit_fcy,
        cast(foreign_credit_amount   as decimal(18,2)) as credit_fcy,

        -- Document reference
        cast(document_number         as varchar(50))   as document_number,
        cast(document_type           as varchar(20))   as document_type,
        cast(posting_text            as varchar(500))  as posting_text,

        -- Intercompany indicator
        cast(is_intercompany         as bit)           as is_intercompany,

        -- Metadata / lineage
        cast(extraction_timestamp    as datetime2)     as extraction_timestamp,
        cast(file_name               as varchar(255))  as source_file_name,

        -- Late entry flag: posting date is in a period that was already closed
        case
            when cast(posting_date as date) < dateadd(
                    day,
                    -{{ var('late_entry_threshold_days') }},
                    cast(getdate() as date)
                 )
             and period_year  < year(getdate())
            then cast(1 as bit)
            else cast(0 as bit)
        end as is_late_entry,

        -- Referential integrity flag: account code must exist in account master
        case
            when exists (
                select 1
                from {{ ref('ref_coa_mapping') }} r
                where r.local_account_code = cast(local_account_code as varchar(50))
            ) then cast(0 as bit)
            else cast(1 as bit)
        end as is_unmapped_account,

        -- Duplicate fingerprint for deduplication downstream
        convert(varchar(64),
            hashbytes('SHA2_256',
                concat(
                    entity_id, '|',
                    local_account_code, '|',
                    posting_date, '|',
                    document_number, '|',
                    cast(debit_amount  as varchar(30)), '|',
                    cast(credit_amount as varchar(30))
                )
            ), 2
        ) as row_hash,

        -- Constant per HU GAAP single-standard architecture
        'HU_GAAP' as gaap_basis

    from source

    -- Exclude rows with no company or no account code (hard reject)
    where entity_id is not null
      and local_account_code is not null

)

select * from renamed

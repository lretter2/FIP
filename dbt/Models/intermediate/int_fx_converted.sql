##############################################################################
--  INTERMEDIATE MODEL: int_fx_converted
--  Financial Intelligence Platform · HU GAAP
--
--  Source  : int_coa_mapped + config.ref_fx_rates (NBH daily rates)
--  Target  : intermediate.int_fx_converted (view)
--  Purpose : Apply NBH (National Bank of Hungary / MNB) FX rates to all
--             foreign currency transactions, producing EUR-equivalent amounts
--             for multi-currency reporting alongside the HUF base.
--
--  FX rate source: config.ref_fx_rates — populated daily by ADF from the
--  official NBH SOAP/REST API (www.mnb.hu/arfolyamok).
--
--  Rate type: Mid-rate on posting_date (HU GAAP requirement for
--  transaction recording). Period-end rates used for balance sheet revaluation
--  are applied separately in the balance sheet mart model.
##############################################################################

with

mapped as (

    select * from {{ ref('int_coa_mapped') }}

),

fx_rates as (

    select
        rate_date,
        currency_code,
        rate_to_huf,           -- 1 FCY = X HUF (e.g. 1 EUR = 395.50 HUF)
        rate_to_eur            -- 1 HUF = X EUR (derived: 1 / rate_to_huf * EUR_HUF)
    from {{ source('config', 'ref_fx_rates') }}

),

converted as (

    select
        m.*,

        -- EUR conversion using posting-date NBH mid-rate
        coalesce(r_eur.rate_to_eur, 1.0 / 400.0) as huf_to_eur_rate,   -- fallback rate

        -- HUF → EUR (used for cross-entity reporting)
        round(m.net_amount_lcy * coalesce(r_eur.rate_to_eur, 1.0 / 400.0), 2)
            as net_amount_eur,

        -- FCY → HUF for foreign currency transactions (if not already HUF)
        case
            when m.currency_code = 'HUF' or m.currency_code is null
                then m.net_amount_lcy
            else round(
                    (m.debit_fcy - m.credit_fcy) * coalesce(r_fcy.rate_to_huf, 1.0),
                    2
                 )
        end as net_amount_lcy_fx_adjusted,

        -- FX rate metadata for audit trail
        coalesce(r_eur.rate_date, m.posting_date) as fx_rate_date_eur,
        coalesce(r_fcy.rate_date, m.posting_date) as fx_rate_date_fcy,

        -- Flag if we used a fallback rate (rate not yet loaded for that date)
        case when r_eur.rate_date is null then cast(1 as bit) else cast(0 as bit) end
            as used_fallback_eur_rate,
        case
            when m.currency_code is not null
             and m.currency_code <> 'HUF'
             and r_fcy.rate_date is null
                then cast(1 as bit)
            else cast(0 as bit)
        end as used_fallback_fcy_rate

    from mapped m

    -- EUR rate: join on posting date
    left join fx_rates r_eur
        on  r_eur.rate_date     = m.posting_date
        and r_eur.currency_code = 'EUR'

    -- Foreign currency rate: join on posting date + transaction currency
    left join fx_rates r_fcy
        on  r_fcy.rate_date     = m.posting_date
        and r_fcy.currency_code = m.currency_code
        and m.currency_code     not in ('HUF')

)

select * from converted

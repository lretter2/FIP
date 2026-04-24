##############################################################################
--  MACRO: currency_convert
--  Financial Intelligence Platform · HU GAAP
--
--  Purpose: Reusable FX conversion macros using NBH (MNB) official rates.
--           All conversions are from HUF (local currency) to a target currency.
--           Rates are stored in config.ref_fx_rates, loaded daily by ADF.
##############################################################################

-- Convert HUF amount to a target currency using the NBH rate on a given date
-- Usage: {{ currency_convert('net_amount_lcy', 'rate_date_col', 'EUR') }}
{% macro currency_convert(amount_column, date_column, target_currency='EUR') %}
    case
        when '{{ target_currency }}' = 'HUF'
            then {{ amount_column }}
        else
            {{ amount_column }} / coalesce(
                (
                    select top 1 r.rate_from_huf
                    from config.ref_fx_rates r
                    where r.currency_code = '{{ target_currency }}'
                      and r.rate_date     <= {{ date_column }}
                    order by r.rate_date desc
                ),
                -- Fallback: use latest available rate if no rate for exact date
                (
                    select top 1 r.rate_from_huf
                    from config.ref_fx_rates r
                    where r.currency_code = '{{ target_currency }}'
                    order by r.rate_date desc
                )
            )
    end
{% endmacro %}


-- Convert a foreign currency amount to HUF using the NBH rate
-- Usage: {{ fcy_to_huf('debit_fcy', 'currency_code_col', 'posting_date_col') }}
{% macro fcy_to_huf(fcy_amount_column, currency_column, date_column) %}
    case
        when {{ currency_column }} = 'HUF' or {{ currency_column }} is null
            then {{ fcy_amount_column }}
        else
            {{ fcy_amount_column }} * coalesce(
                (
                    select top 1 r.rate_to_huf
                    from config.ref_fx_rates r
                    where r.currency_code = {{ currency_column }}
                      and r.rate_date     <= {{ date_column }}
                    order by r.rate_date desc
                ),
                1.0   -- fallback: 1:1 if rate missing (triggers DQ alert separately)
            )
    end
{% endmacro %}


-- Period-end revaluation rate for balance sheet items (HUF → EUR)
-- Uses the last available NBH rate on or before the period end date
{% macro period_end_fx_rate(period_key_column, target_currency='EUR') %}
    coalesce(
        (
            select top 1 r.rate_to_huf
            from config.ref_fx_rates r
            where r.currency_code = '{{ target_currency }}'
              and r.rate_date     <= {{ period_end_date(period_key_column) }}
            order by r.rate_date desc
        ),
        (
            select top 1 r.rate_to_huf
            from config.ref_fx_rates r
            where r.currency_code = '{{ target_currency }}'
            order by r.rate_date desc
        )
    )
{% endmacro %}


-- Generates a currency selector expression matching the Power BI what-if parameter
-- Allows users to toggle between HUF / EUR / USD in dashboards
{% macro multi_currency_amount(huf_column, eur_column, selected_currency_param='HUF') %}
    case '{{ selected_currency_param }}'
        when 'HUF' then {{ huf_column }}
        when 'EUR' then {{ eur_column }}
        else {{ huf_column }}   -- default to HUF
    end
{% endmacro %}

##############################################################################
--  MACRO: fiscal_period
--  Financial Intelligence Platform · HU GAAP
--
--  Purpose: Utility macros for HU GAAP fiscal period calculations.
--           Hungarian companies typically use a calendar fiscal year (Jan-Dec)
--           but the macro supports non-calendar fiscal years via the
--           fiscal_year_start_month project variable.
##############################################################################

-- Returns the fiscal year for a given date column
{% macro fiscal_year(date_column) %}
    case
        when {{ var('fiscal_year_start_month') }} = 1
            then year({{ date_column }})
        when month({{ date_column }}) >= {{ var('fiscal_year_start_month') }}
            then year({{ date_column }})
        else
            year({{ date_column }}) - 1
    end
{% endmacro %}


-- Returns the fiscal period (month within fiscal year, 1-12)
{% macro fiscal_period(date_column) %}
    case
        when {{ var('fiscal_year_start_month') }} = 1
            then month({{ date_column }})
        else
            ((month({{ date_column }}) - {{ var('fiscal_year_start_month') }} + 12) % 12) + 1
    end
{% endmacro %}


-- Returns the fiscal quarter (1-4) for a given date
{% macro fiscal_quarter(date_column) %}
    ceiling({{ fiscal_period(date_column) }} / 3.0)
{% endmacro %}


-- Returns a YYYYMM integer period key from year and month columns
{% macro period_key(year_column, month_column) %}
    cast(
        concat(
            cast({{ year_column }} as varchar(4)),
            right('0' + cast({{ month_column }} as varchar(2)), 2)
        ) as int
    )
{% endmacro %}


-- Returns the prior month's period key (handles year boundary)
{% macro prior_period_key(period_key_column) %}
    case
        when right(cast({{ period_key_column }} as varchar(6)), 2) = '01'
            then (cast({{ period_key_column }} as int) - 100) + 11  -- Dec of prior year
        else
            cast({{ period_key_column }} as int) - 1
    end
{% endmacro %}


-- Returns the same month prior year period key
{% macro prior_year_period_key(period_key_column) %}
    cast({{ period_key_column }} as int) - 100
{% endmacro %}


-- Returns a date range filter for a given fiscal year
{% macro fiscal_year_filter(date_column, fiscal_year_value) %}
    {% set start_month = var('fiscal_year_start_month') %}
    (
        (year({{ date_column }}) = {{ fiscal_year_value }}
         and month({{ date_column }}) >= {{ start_month }})
        or
        (year({{ date_column }}) = {{ fiscal_year_value }} + 1
         and month({{ date_column }}) < {{ start_month }})
    )
{% endmacro %}


-- Returns the last day of a period (YYYYMM integer)
{% macro period_end_date(period_key_column) %}
    eomonth(
        datefromparts(
            cast(left(cast({{ period_key_column }} as varchar(6)), 4) as int),
            cast(right(cast({{ period_key_column }} as varchar(6)), 2) as int),
            1
        )
    )
{% endmacro %}

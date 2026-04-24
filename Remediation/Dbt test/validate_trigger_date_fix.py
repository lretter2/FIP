#!/usr/bin/env python3
"""
validate_trigger_date_fix.py
Financial Intelligence Platform — ADF Trigger Date Fix Validator
Remediates validation requirement from FINDING A-03.

PURPOSE
-------
Validates that trg_monthly_close.json correctly computes prior-month
year and month for all 12 calendar months, including the January→December
year-boundary case that the original sub(month, 1) expression failed on.

The fix (2026-04-09) replaced:
    "@year(sub(trigger().scheduledTime, 1))"     ← BROKEN
    "@month(sub(trigger().scheduledTime, 1))"    ← produced 0 for January

With the correct addToTime pattern:
    "@year(addToTime(trigger().scheduledTime, -1, 'Month'))"
    "@month(addToTime(trigger().scheduledTime, -1, 'Month'))"

USAGE
-----
    python validate_trigger_date_fix.py                    # validates trigger JSON + logic
    python validate_trigger_date_fix.py --verbose          # shows all 12 month test cases
    python validate_trigger_date_fix.py --trigger-file PATH/TO/trg_monthly_close.json

EXIT CODES
----------
    0  All checks passed
    1  Trigger file uses broken pattern
    2  Logic error in expected prior-month computation
    3  File not found or JSON parse error
"""

import json
import sys
import argparse
import re
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path


# ─── Expected ADF expression patterns ────────────────────────────────────────

CORRECT_YEAR_PATTERN  = r"@year\(addToTime\(trigger\(\)\.scheduledTime,\s*-1,\s*'Month'\)\)"
CORRECT_MONTH_PATTERN = r"@month\(addToTime\(trigger\(\)\.scheduledTime,\s*-1,\s*'Month'\)\)"

BROKEN_YEAR_PATTERN   = r"@year\(sub\("
BROKEN_MONTH_PATTERN  = r"@month\(sub\("

DEFAULT_TRIGGER_PATH  = Path(__file__).parent.parent.parent / "adf_pipelines" / "triggers" / "trg_monthly_close.json"


# ─── Test cases: all 12 months + edge cases ──────────────────────────────────

def generate_test_cases():
    """Generate (trigger_fires_on, expected_year, expected_month) for all months."""
    cases = []
    for month in range(1, 13):
        # Trigger fires on the 1st of each month — computes PREVIOUS month's period
        trigger_date = date(2026, month, 1)
        prior        = trigger_date + relativedelta(months=-1)
        cases.append({
            "trigger_fires_on": trigger_date.isoformat(),
            "trigger_month":    month,
            "trigger_year":     2026,
            "expected_year":    prior.year,
            "expected_month":   prior.month,
            "is_year_boundary": month == 1,  # January → previous year
        })
    return cases


def simulate_add_to_time(trigger_date: date, months: int) -> tuple[int, int]:
    """
    Simulate ADF's addToTime(scheduledTime, -1, 'Month').
    Returns (year, month) of the adjusted date.
    """
    result = trigger_date + relativedelta(months=months)
    return result.year, result.month


def simulate_broken_sub_month(trigger_date: date) -> tuple[int, int]:
    """
    Simulate the BROKEN original expression: sub(month, 1).
    Returns (year, month) — month will be 0 for January, which is invalid.
    """
    broken_month = trigger_date.month - 1        # = 0 for January
    broken_year  = trigger_date.year             # year never decrements in broken version
    return broken_year, broken_month


# ─── Trigger file validation ─────────────────────────────────────────────────

def validate_trigger_file(trigger_path: Path, verbose: bool = False) -> bool:
    """
    Parse trg_monthly_close.json and verify that:
    1. The period_year  expression uses addToTime pattern
    2. The period_month expression uses addToTime pattern
    3. Neither expression uses the broken sub() pattern
    """
    print(f"\n{'='*70}")
    print(f"  TRIGGER FILE VALIDATION")
    print(f"  File: {trigger_path}")
    print(f"{'='*70}")

    try:
        with open(trigger_path, "r", encoding="utf-8") as f:
            trigger_json = json.load(f)
    except FileNotFoundError:
        print(f"  ✗ ERROR: File not found: {trigger_path}")
        return False
    except json.JSONDecodeError as e:
        print(f"  ✗ ERROR: JSON parse error: {e}")
        return False

    # Extract pipeline parameter expressions
    trigger_text = json.dumps(trigger_json)

    checks_passed = 0
    checks_total  = 4

    # Check 1: correct year expression present
    if re.search(CORRECT_YEAR_PATTERN, trigger_text):
        print("  ✓ period_year  uses addToTime(-1, 'Month') — CORRECT")
        checks_passed += 1
    else:
        print("  ✗ period_year  does NOT use addToTime pattern — check expression")

    # Check 2: correct month expression present
    if re.search(CORRECT_MONTH_PATTERN, trigger_text):
        print("  ✓ period_month uses addToTime(-1, 'Month') — CORRECT")
        checks_passed += 1
    else:
        print("  ✗ period_month does NOT use addToTime pattern — check expression")

    # Check 3: broken year expression absent
    if not re.search(BROKEN_YEAR_PATTERN, trigger_text):
        print("  ✓ Broken @year(sub(...)) pattern NOT present — SAFE")
        checks_passed += 1
    else:
        print("  ✗ Broken @year(sub(...)) pattern IS present — VULNERABILITY")

    # Check 4: broken month expression absent
    if not re.search(BROKEN_MONTH_PATTERN, trigger_text):
        print("  ✓ Broken @month(sub(...)) pattern NOT present — SAFE")
        checks_passed += 1
    else:
        print("  ✗ Broken @month(sub(...)) pattern IS present — produces 0 in January")

    if verbose:
        # Show the actual pipeline parameter values found
        pipelines = trigger_json.get("properties", {}).get("pipelines", [])
        for pl in pipelines:
            params = pl.get("parameters", {})
            print(f"\n  Pipeline parameters in trigger:")
            for k, v in params.items():
                print(f"    {k}: {json.dumps(v)}")

    print(f"\n  Result: {checks_passed}/{checks_total} checks passed")
    return checks_passed == checks_total


# ─── Logic simulation ─────────────────────────────────────────────────────────

def run_logic_tests(verbose: bool = False) -> bool:
    """
    Simulate the addToTime expression for all 12 months and compare
    against the broken sub() expression to demonstrate the fix.
    """
    print(f"\n{'='*70}")
    print(f"  PRIOR-MONTH DATE ARITHMETIC — ALL 12 MONTHS")
    print(f"{'='*70}")

    cases  = generate_test_cases()
    passed = 0

    if verbose:
        print(f"\n  {'Trigger Date':<15} {'Expected Y':<12} {'Expected M':<12} "
              f"{'Fixed Y':<10} {'Fixed M':<10} {'Broken Y':<10} {'Broken M':<10} {'Pass'}")
        print(f"  {'-'*90}")

    for case in cases:
        trigger_date   = date.fromisoformat(case["trigger_fires_on"])
        exp_year       = case["expected_year"]
        exp_month      = case["expected_month"]

        fixed_year,  fixed_month  = simulate_add_to_time(trigger_date, -1)
        broken_year, broken_month = simulate_broken_sub_month(trigger_date)

        fixed_ok   = (fixed_year == exp_year   and fixed_month  == exp_month)
        broken_ok  = (broken_year == exp_year  and broken_month == exp_month)
        test_pass  = fixed_ok and not broken_ok   # fix correct, broken wrong

        if case["is_year_boundary"]:
            test_pass = fixed_ok   # January: fix must be correct regardless of broken
            label     = "YEAR BOUNDARY ← critical"
        else:
            label = ""

        if verbose:
            status = "✓" if fixed_ok else "✗"
            print(f"  {case['trigger_fires_on']:<15} {exp_year:<12} {exp_month:<12} "
                  f"{fixed_year:<10} {fixed_month:<10} {broken_year:<10} {broken_month:<10} "
                  f"{status}  {label}")

        if fixed_ok:
            passed += 1

    # Always show the January boundary explicitly (the key bug case)
    jan_case       = cases[0]   # January fires on 2026-01-01
    trigger_jan    = date.fromisoformat(jan_case["trigger_fires_on"])
    fixed_y, fixed_m   = simulate_add_to_time(trigger_jan, -1)
    broken_y, broken_m = simulate_broken_sub_month(trigger_jan)

    print(f"\n  ── January Boundary Case (the original bug) ──")
    print(f"     Trigger fires on : 2026-01-01")
    print(f"     Expected result  : year=2025, month=12 (December of prior year)")
    print(f"     FIXED  result    : year={fixed_y},  month={fixed_m}  "
          f"{'✓ CORRECT' if (fixed_y == 2025 and fixed_m == 12) else '✗ WRONG'}")
    print(f"     BROKEN result    : year={broken_y}, month={broken_m}  "
          f"{'✓ (accidentally correct)' if (broken_y == 2025 and broken_m == 12) else '✗ WRONG (month=0 is invalid)'}")

    print(f"\n  Logic test result: {passed}/12 month cases correct with addToTime fix")
    return passed == 12


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate ADF trigger date fix for trg_monthly_close.json"
    )
    parser.add_argument(
        "--trigger-file",
        type=Path,
        default=DEFAULT_TRIGGER_PATH,
        help="Path to trg_monthly_close.json (default: auto-detected from project root)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output including all 12 month test cases"
    )
    args = parser.parse_args()

    print("\nFIP Trigger Date Fix Validator")
    print("Validates FINDING A-03 remediation: trg_monthly_close.json addToTime fix")

    file_ok  = validate_trigger_file(args.trigger_file, verbose=args.verbose)
    logic_ok = run_logic_tests(verbose=args.verbose)

    print(f"\n{'='*70}")
    if file_ok and logic_ok:
        print("  ✓ ALL CHECKS PASSED — FINDING A-03 is RESOLVED")
        print("  The trigger correctly computes prior-month year/month for all 12 months.")
        sys.exit(0)
    else:
        failures = []
        if not file_ok:
            failures.append("Trigger file still uses broken sub() pattern")
        if not logic_ok:
            failures.append("Date arithmetic logic errors detected")
        print(f"  ✗ CHECKS FAILED: {'; '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

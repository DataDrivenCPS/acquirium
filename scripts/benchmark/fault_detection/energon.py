#14

IC_START = "2024-01-01T00:00:00Z"
IC_END   = "2025-01-01T00:00:00Z"
TEST_START = "2025-01-01T00:00:00Z"
TEST_END   = "2025-02-01T00:00:00Z"

ENERGON_QUERY = """
SELECT ChlorinationBasin(B) * Data(B)
FROM Building B
WHERE B.Source = 'Benicia'
FILTER Data.Unit IN ('MilliGM-PER-L')
"""

def run_fault_isolation(ic_df, test_df):
    return fault_isolation_pipeline(ic_df, test_df)

def main():
    ic_df = fetch(ENERGON_QUERY, start=IC_START, end=IC_END, cast_value="float")
    test_df = fetch(ENERGON_QUERY, start=TEST_START, end=TEST_END, cast_value="float")

    results = run_fault_isolation(ic_df, test_df)

    report = format_report(results)
    persist_report(report)

if __name__ == "__main__":
    main()

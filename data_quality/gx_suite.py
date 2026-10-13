"""Run a Great Expectations suite on a pandas DataFrame (the course 2.7 notebook pattern).

Same helper that passed 14 rules on 10 Oct 2026, with three small additions:
  labels  -> prints which quality dimension each rule belongs to (Michael's 14)
  halt    -> True: raise an Exception on failure (a pipeline gate)
             False: report only (a warning suite)
  returns -> True / False, so Dagster or a notebook can read the verdict
"""
import great_expectations as gx
from google.cloud import bigquery

PROJECT = "friendly-autumn-509313-n3"      # where query jobs run and are billed


def bq_df(sql):
    """Run SQL in BigQuery (US) and return the result as a pandas DataFrame."""
    client = bigquery.Client(project=PROJECT)
    return client.query(sql, location="US").to_dataframe()


def run_suite(df, suite_name, expectations, labels=None, halt=True):
    labels = labels or {}
    context = gx.get_context()                                    # 1 an in-memory workspace
    source = context.data_sources.add_or_update_pandas(name=suite_name + "_source")
    asset = source.add_dataframe_asset(name=suite_name + "_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(suite_name + "_batch")
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=suite_name))
    for expectation in expectations:                              # 2 the rules
        suite.add_expectation(expectation)
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(data=batch_definition, suite=suite,
                                name=suite_name + "_validation"))
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name=suite_name + "_checkpoint",
        validation_definitions=[validation_definition]))
    results = checkpoint.run(batch_parameters={"dataframe": df})   # 3 data injected at run time

    for validation in results.run_results.values():                # 4 one line per rule
        for r in validation.results:
            kw = r.expectation_config.kwargs
            col = kw.get("column", "")
            seen = r.result.get("observed_value", r.result.get("unexpected_count", ""))
            tag = labels.get(col, "")
            print(" " + ("PASS" if r.success else "FAIL"),
                  r.expectation_config.type, col, seen, ("[" + tag + "]") if tag else "")
    print(suite_name + ":", "PASSED" if results.success else "FAILED")
    if not results.success and halt:                               # 5 the gate
        raise Exception("Data quality check failed (" + suite_name + ") - pipeline halted.")
    return results.success

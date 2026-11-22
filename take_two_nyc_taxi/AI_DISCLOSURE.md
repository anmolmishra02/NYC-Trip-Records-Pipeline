# AI Usage Disclosure

AI tooling was used during this take-home exercise. The candidate remains responsible for every submitted line of code and every analytical decision.

## Tool used
- ChatGPT (OpenAI)

## Areas of assistance
- Debugging PySpark and Delta Lake behaviour, including timestamp handling and Delta `timestampNtz` feature enablement.
- Reviewing/refining Silver standardization and data-quality logic.
- Developing and reviewing portions of Gold airport and congestion-pricing aggregation logic.
- Generating diagnostic SQL and interpreting query results.
- Reviewing assumptions, edge cases and caveats.
- Drafting README, DQ specification, data dictionary and architecture documentation.

## Approximate contribution
Complete these estimates based on the final ZIP before submission:
- Approximate AI-generated or AI-substantially-drafted code: **[candidate to estimate]**
- Approximate code written directly by the candidate: **[candidate to estimate]**

Do not submit the placeholders. Estimate the final submitted code, not suggestions that were rejected or never used.

## Examples of review and correction
- Timestamp semantics were investigated after a discrepancy in pre-effective-date fee counts. The source Parquet fields were identified as `TIMESTAMP_NTZ`; Silver was changed to preserve local wall-clock timestamps instead of converting them to timezone-aware timestamps.
- An earlier 7,590 pre-effective-date fee count was not supported by final Bronze/Silver validation. The count was corrected to four after querying Bronze with `TIMESTAMP_NTZ` boundaries.
- Airport aggregation was updated to filter on the airport event timestamp: pickup time for FROM airport and drop-off time for TO airport.
- Negative financial values and congestion-fee adjustments were retained and surfaced as warnings rather than silently overwritten or automatically quarantined.

## Ownership and verification
The candidate executed the pipeline in Databricks, inspected Bronze/Silver/Gold outputs, and is responsible for understanding and defending the implementation, assumptions, limitations and submitted artefacts. AI suggestions were not treated as authoritative without runtime or data validation.

# AI Usage Disclosure

AI tools were used in a limited supporting capacity during this take-home exercise, primarily for debugging assistance, reviewing implementation approaches, refining documentation, and improving code readability.

The pipeline design, implementation, data transformations, business logic, and analytical decisions were driven and reviewed by the candidate. AI suggestions were treated as recommendations, evaluated against the actual requirements and execution results, and modified where necessary.

The candidate takes full ownership of the submitted code, understands the implementation, and is responsible for explaining and defending all technical decisions.

## Tool used
- ChatGPT

## Areas of assistance
- Debugging PySpark and Delta Lake behaviour, including timestamp handling and Delta `timestampNtz` feature enablement.
- Reviewing/refining Silver standardization and data-quality logic.
- Reviewing portions of Gold airport and congestion-pricing aggregation logic.
- Reviewing assumptions, edge cases and caveats.
- Drafting README, DQ specification, data dictionary and architecture documentation.

AI-generated suggestions were reviewed and validated against the implementation and actual execution results. Final code changes, implementation choices and analytical decisions were reviewed and owned by the candidate.

## Approximate contribution

- Approximate AI-generated or AI-substantially-drafted code: **[30%]**
- Approximate code written directly by the candidate: **[70%]**

## Examples of review and correction
- Timestamp semantics were investigated after a discrepancy in pre-effective-date fee counts. The source Parquet fields were identified as `TIMESTAMP_NTZ`; Silver was changed to preserve local wall-clock timestamps instead of converting them to timezone-aware timestamps.
- An earlier 7,590 pre-effective-date fee count was not supported by final Bronze/Silver validation. The count was corrected to four after querying Bronze with `TIMESTAMP_NTZ` boundaries.
- Airport aggregation was updated to filter on the airport event timestamp: pickup time for FROM airport and drop-off time for TO airport.
- Negative financial values and congestion-fee adjustments were retained and surfaced as warnings rather than silently overwritten or automatically quarantined.

## Ownership and verification
The candidate executed the pipeline in Databricks, inspected Bronze/Silver/Gold outputs, and is responsible for understanding and defending the implementation, assumptions, limitations and submitted artefacts. AI suggestions were not treated as authoritative without runtime or data validation.

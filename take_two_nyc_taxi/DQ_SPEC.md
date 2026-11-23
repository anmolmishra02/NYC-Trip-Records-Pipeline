# Data Quality Check Specification

## Principles
- Quarantine records that cannot be reliably interpreted for trip-level analytics.
- Warn and retain unusual financial values when they may represent source adjustments.
- Preserve source values and lineage; do not silently coerce anomalies.
- Reconcile input rows to valid plus quarantined rows.

## Implemented checks

| Check | Assertion | Severity | Failure action | Rationale |
|---|---|---|---|---|
| `MISSING_PICKUP_DATETIME` | Pickup timestamp is present | Critical | Quarantine | Cannot derive trip start/date |
| `MISSING_DROPOFF_DATETIME` | Drop-off timestamp is present | Critical | Quarantine | Cannot determine trip completion/date |
| `DROPOFF_BEFORE_PICKUP` | Drop-off is not earlier than pickup | Critical | Quarantine | Chronologically invalid trip |
| `MISSING_PICKUP_LOCATION` | Pickup location ID is present | High | Quarantine | Cannot assign pickup borough |
| `MISSING_DROPOFF_LOCATION` | Drop-off location ID is present | High | Quarantine | Cannot classify airport/CBD destination |
| `MISSING_TOTAL_AMOUNT` | Total amount is present | High | Quarantine | Required for collected-revenue analysis |
| `INVALID_SOURCE_MONTH` | `source_month` is valid YYYY-MM | High | Quarantine | Cannot validate source period |
| `OUTSIDE_SOURCE_PERIOD` | Pickup/drop-off timestamps are within source month with one-day tolerance | Medium | Quarantine | Detects misplaced/anomalous records while allowing cross-month trips |
| `NEGATIVE_FARE_AMOUNT` | Fare is non-negative | Warning | Log and retain | Could represent adjustment/refund-like data |
| `NEGATIVE_TOTAL_AMOUNT` | Total amount is non-negative | Warning | Log and retain | Could represent reversals or adjustments |
| `NEGATIVE_CBD_CONGESTION_FEE` | CBD fee is non-negative | Warning | Log and retain | Negative source adjustments are preserved |
| `CBD_FEE_BEFORE_EFFECTIVE_DATE` | Positive CBD fee is not present before 2025-01-05 | Warning | Log and retain | Flags potential source inconsistency without rewriting facts |

## Observed outcomes
- Bronze trip rows: 10,870,081.
- Silver valid rows: 10,869,523.
- Quarantined rows: 558.
- Reconciliation: 10,869,523 + 558 = 10,870,081.
- Four positive `cbd_congestion_fee` records were observed with drop-off before January 5, 2025. They were retained and flagged.
- Negative fare, total amount and congestion-fee values were observed and retained with warning metadata.
- December 2024 does not provide `cbd_congestion_fee`; the unified value is NULL.


## Pipeline-level failure policy
- Critical row-level failures route the record to quarantine with a failure reason.
- Failed reconciliation or unreadable/incompatible input schema should fail the pipeline rather than publish incomplete Gold outputs.
- Warning-level checks are logged and surfaced in monitoring; they do not stop processing.
- Monitor quarantine rate, warning counts by source month/service, null rates, schema drift and reconciliation.


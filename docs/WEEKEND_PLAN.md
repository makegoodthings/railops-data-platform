# 12–16 hour weekend plan

The goal is not merely a green pipeline. At each milestone, explain the engineering decision in
your own words and commit the result.

| Time | Milestone | Deliverable | Production idea |
|---:|---|---|---|
| 0.5h | 0. Environment | Docker and repo ready | Reproducible runtime |
| 1.0h | 1. Contract first | Source schema + grains | Model before code |
| 2.0h | 2. Incremental ingest | Bronze + watermark | No loss, safe rerun |
| 2.0h | 3. Transform | Silver + rejects | Trusted canonical data |
| 1.5h | 4. Serve | Gold tables | Consumer-oriented modeling |
| 1.5h | 5. Quality | Blocking checks | Correctness as code |
| 1.5h | 6. Recovery | Failure drill + backfill | Operability |
| 2.0h | 7. Orchestrate | Airflow DAG | Dependencies/retries |
| 1.5h | 8. Ship | Tests, CI, README | Maintainability |
| 1.0h | 9. Interview | Architecture story | Communicate ownership |

## Required Git history

Use one commit after each acceptance test:

```text
chore: bootstrap local data platform
feat: define source data contract and seed generator
feat: add idempotent incremental bronze ingestion
feat: add silver normalization and quarantine
feat: publish consumer-ready gold models
test: enforce data quality contracts
feat: support bounded historical backfills
feat: orchestrate railops batch pipeline
ci: validate pipeline on pull requests
docs: document architecture and operational runbook
```

## Rules for learning

1. Before running code, predict what each table's row count should be.
2. Run every pipeline twice and compare results.
3. Deliberately break one source row and one task.
4. Never fix bad data by deleting the evidence.
5. At the end, you must answer: what prevents loss, duplication, stale data, and silent corruption?

## Definition of done

- A new developer can run it from README only.
- The second identical run changes no business rows.
- A source correction reaches Silver and Gold.
- Invalid records are visible with rejection reasons.
- A bounded backfill does not move the main watermark.
- Tests and lint run in GitHub Actions.
- The README includes a diagram, tradeoffs, and incident recovery steps.


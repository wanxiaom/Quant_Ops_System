# DAG test tasks

`dag_test_task.py` is a lightweight task used to verify Pipeline isolation,
queue ordering, log collection, success propagation, and failure blocking. It
does not read or write business data.

The test fixtures are installed by `db/migrations/005_dag_test_tasks.sql`:

- `test_pipeline_alpha`: alpha step 1 -> step 2 -> step 3
- `test_pipeline_beta`: beta step 1 -> step 2 -> step 3

Both pipelines run on `node_001`. Start the Linux Agent before triggering a
pipeline.

Direct script check:

```bash
python3 src/scripts/test/dag_test_task.py \
  --queue alpha --step step-1 --sleep-seconds 1 --emit-lines 2
```

Trigger through Manager after publishing or installing the fixtures:

```bash
curl -X POST \
  -H "Authorization: Bearer quant_ops_secret_2026" \
  -H "Content-Type: application/json" \
  -d '{"pipeline_id":"test_pipeline_alpha"}' \
  http://-/api/tasks/task_test_alpha_1/run
```

To test failure handling, edit one task's `default_params` and set
`"fail": true`. Its downstream task should not be queued.

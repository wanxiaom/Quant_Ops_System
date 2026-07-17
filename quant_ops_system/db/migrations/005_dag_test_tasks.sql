USE quant_ops;

-- Lightweight tasks for testing multiple independent DAG queues.
INSERT INTO tasks
    (task_id, name, node_type, script_path, cron_expr, timeout_sec, enabled, target_node_id, default_params)
VALUES
    ('task_test_alpha_1', 'DAG Test Alpha - Step 1', 'python',
     'src/scripts/test/dag_test_task.py', '', 60, 1, 'node_001',
     '{"queue":"alpha","step":"step-1","sleep_seconds":2,"emit_lines":2}'),
    ('task_test_alpha_2', 'DAG Test Alpha - Step 2', 'python',
     'src/scripts/test/dag_test_task.py', '', 60, 1, 'node_001',
     '{"queue":"alpha","step":"step-2","sleep_seconds":2,"emit_lines":2}'),
    ('task_test_alpha_3', 'DAG Test Alpha - Step 3', 'python',
     'src/scripts/test/dag_test_task.py', '', 60, 1, 'node_001',
     '{"queue":"alpha","step":"step-3","sleep_seconds":2,"emit_lines":2}'),
    ('task_test_beta_1', 'DAG Test Beta - Step 1', 'python',
     'src/scripts/test/dag_test_task.py', '', 60, 1, 'node_001',
     '{"queue":"beta","step":"step-1","sleep_seconds":3,"emit_lines":3}'),
    ('task_test_beta_2', 'DAG Test Beta - Step 2', 'python',
     'src/scripts/test/dag_test_task.py', '', 60, 1, 'node_001',
     '{"queue":"beta","step":"step-2","sleep_seconds":3,"emit_lines":3}'),
    ('task_test_beta_3', 'DAG Test Beta - Step 3', 'python',
     'src/scripts/test/dag_test_task.py', '', 60, 1, 'node_001',
     '{"queue":"beta","step":"step-3","sleep_seconds":3,"emit_lines":3}')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    node_type = VALUES(node_type),
    script_path = VALUES(script_path),
    cron_expr = VALUES(cron_expr),
    timeout_sec = VALUES(timeout_sec),
    enabled = VALUES(enabled),
    target_node_id = VALUES(target_node_id),
    default_params = VALUES(default_params);

INSERT INTO pipelines
    (pipeline_id, name, description, enabled, canvas, nodes, edges, entry_task_ids, schedule)
VALUES
    ('test_pipeline_alpha', 'DAG Test Queue Alpha', 'Three-step lightweight DAG test queue', 1,
     '{"width":900,"height":420}',
     '[{"task_id":"task_test_alpha_1","x":80,"y":140},{"task_id":"task_test_alpha_2","x":340,"y":140},{"task_id":"task_test_alpha_3","x":600,"y":140}]',
     '[{"from":"task_test_alpha_1","to":"task_test_alpha_2"},{"from":"task_test_alpha_2","to":"task_test_alpha_3"}]',
     '["task_test_alpha_1"]', '{}'),
    ('test_pipeline_beta', 'DAG Test Queue Beta', 'Three-step independent DAG test queue', 1,
     '{"width":900,"height":420}',
     '[{"task_id":"task_test_beta_1","x":80,"y":140},{"task_id":"task_test_beta_2","x":340,"y":140},{"task_id":"task_test_beta_3","x":600,"y":140}]',
     '[{"from":"task_test_beta_1","to":"task_test_beta_2"},{"from":"task_test_beta_2","to":"task_test_beta_3"}]',
     '["task_test_beta_1"]', '{}')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    description = VALUES(description),
    enabled = VALUES(enabled),
    canvas = VALUES(canvas),
    nodes = VALUES(nodes),
    edges = VALUES(edges),
    entry_task_ids = VALUES(entry_task_ids),
    schedule = VALUES(schedule);

DELETE FROM dag_edges WHERE pipeline_id IN ('test_pipeline_alpha', 'test_pipeline_beta');

INSERT INTO dag_edges (parent_task_id, child_task_id, pipeline_id) VALUES
    ('task_test_alpha_1', 'task_test_alpha_2', 'test_pipeline_alpha'),
    ('task_test_alpha_2', 'task_test_alpha_3', 'test_pipeline_alpha'),
    ('task_test_beta_1', 'task_test_beta_2', 'test_pipeline_beta'),
    ('task_test_beta_2', 'task_test_beta_3', 'test_pipeline_beta');

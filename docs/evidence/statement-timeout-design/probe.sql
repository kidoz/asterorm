\set ON_ERROR_STOP on
SELECT current_setting('server_version') AS server_version;
SELECT setting AS original_timeout FROM pg_catalog.pg_settings
WHERE name = 'statement_timeout' \gset
SELECT unit, min_val, max_val, context FROM pg_catalog.pg_settings
WHERE name = 'statement_timeout';

SELECT pg_catalog.set_config('statement_timeout', '500ms', false);
SET LOCAL statement_timeout = '40ms';
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 500;
END $$;
\echo PASS autocommit_local_has_no_effect

BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '200ms', true);
SELECT pg_catalog.set_config('statement_timeout', '100ms', true);
SELECT pg_catalog.set_config('statement_timeout', '200ms', true);
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 200;
END $$;
SELECT pg_catalog.set_config('statement_timeout', '500ms', true);
COMMIT;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 500;
END $$;
\echo PASS nested_local_restore

BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '350ms', false);
ROLLBACK;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 500;
END $$;
BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '350ms', false);
SELECT pg_catalog.set_config('statement_timeout', '100ms', true);
COMMIT;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 350;
END $$;
SELECT pg_catalog.set_config('statement_timeout', '500ms', false);
\echo PASS session_set_is_transactional_and_local_does_not_replace_it

BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '200ms', true);
SAVEPOINT before_inner;
SELECT pg_catalog.set_config('statement_timeout', '100ms', true);
ROLLBACK TO before_inner;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 200;
END $$;
ROLLBACK;
\echo PASS earlier_savepoint_undoes_local_override

BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '200ms', true);
SELECT pg_catalog.set_config('statement_timeout', '100ms', true);
SAVEPOINT inside_inner;
SELECT pg_catalog.set_config('statement_timeout', '200ms', true);
ROLLBACK TO inside_inner;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 100;
END $$;
ROLLBACK;
\echo PASS later_savepoint_can_resurrect_inner_override

BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '100ms', true);
COMMIT;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 500;
END $$;
BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '100ms', true);
ROLLBACK;
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 500;
END $$;
\echo PASS transaction_completion_restores_session_value

BEGIN;
SELECT pg_catalog.set_config('statement_timeout', '40ms', true);
\set ON_ERROR_STOP off
SELECT pg_sleep(0.2);
\set cancellation_state :SQLSTATE
SELECT pg_catalog.set_config('statement_timeout', '500ms', true);
\set restoration_state :SQLSTATE
\set ON_ERROR_STOP on
ROLLBACK;
SELECT :'cancellation_state' = '57014' AND :'restoration_state' = '25P02' AS expected_states \gset
\if :expected_states
\echo PASS cancellation_and_failed_transaction_restore
\else
DO $$ BEGIN RAISE EXCEPTION 'Unexpected cancellation/restoration SQLSTATE'; END $$;
\endif
\echo cancellation_state :cancellation_state restoration_state :restoration_state
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 500;
END $$;

SELECT pg_catalog.set_config('statement_timeout', '0ms', false);
SELECT pg_catalog.set_config('statement_timeout', '2147483647ms', false);
DO $$ BEGIN
    ASSERT (SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout') = 2147483647;
END $$;
\set ON_ERROR_STOP off
SELECT pg_catalog.set_config('statement_timeout', '-1ms', false);
\set negative_state :SQLSTATE
SELECT pg_catalog.set_config('statement_timeout', '2147483648ms', false);
\set overflow_state :SQLSTATE
\set ON_ERROR_STOP on
SELECT :'negative_state' = '22023' AND :'overflow_state' = '22023' AS expected_states \gset
\if :expected_states
\echo PASS zero_maximum_and_invalid_bounds
\else
DO $$ BEGIN RAISE EXCEPTION 'Unexpected duration SQLSTATE'; END $$;
\endif
\echo negative_state :negative_state overflow_state :overflow_state
SELECT pg_catalog.set_config('statement_timeout', :'original_timeout' || 'ms', false);
\echo PASS original_session_value_restored

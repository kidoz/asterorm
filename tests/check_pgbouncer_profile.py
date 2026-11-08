"""Verify the opt-in pinned proxy configuration, then run its scoped native integration."""
import argparse
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--integration', type=Path, required=True)
arguments = parser.parse_args()
root = Path(__file__).resolve().parents[1]
compose = ['docker', 'compose', '-f', str(root / 'compose.yaml'), '-f',
           str(root / 'compose.pgbouncer.yaml'), '--profile', 'pgbouncer']
def admin(sql):
    return subprocess.check_output([*compose, 'exec', '-T', '-e', 'PGPASSWORD=orm_test',
                                    'pgbouncer', 'psql', '-h', '127.0.0.1', '-p', '5432',
                                    '-U', 'orm_test', '-d', 'pgbouncer', '-At', '-F', '\t',
                                    '-c', sql], text=True)
version = admin('SHOW VERSION').strip()
assert version == 'PgBouncer 1.24.1', version
settings = {fields[0]: fields[1] for line in admin('SHOW CONFIG').splitlines()
            if len(fields := line.split('\t')) >= 2}
for name, expected in [('pool_mode', 'transaction'), ('max_prepared_statements', '0'),
                       ('default_pool_size', '2'), ('server_round_robin', '1')]:
    assert settings[name] == expected, (name, settings.get(name))
    print(name, '=', expected, flush=True)
print(version, flush=True)
environment = os.environ.copy()
environment['ASTERORM_PGBOUNCER_TEST_CONNINFO'] = (
    'host=127.0.0.1 port=6432 dbname=orm_test user=orm_test password=orm_test sslmode=disable')
subprocess.run([str(arguments.integration.resolve()), '[pgbouncer]'], env=environment, check=True)

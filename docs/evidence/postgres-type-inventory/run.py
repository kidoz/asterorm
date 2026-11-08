"""Capture current PostgreSQL type gaps; this historical inventory is not feature acceptance."""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--build-root', type=Path, default=Path('buildDir'))
parser.add_argument('--output', type=Path)
parser.add_argument('--sanitize', action='store_true', help='Instrument the probe and inline codecs only')
arguments = parser.parse_args()
root = Path(__file__).resolve().parents[3]
folder = Path(__file__).resolve().parent
build = arguments.build_root.resolve()
workspace = Path(tempfile.mkdtemp(prefix='asterorm-type-inventory-'))
entries = json.loads((build / 'compile_commands.json').read_text())
entry = next(item for item in entries if item['file'].endswith('/integration/test_sql.cpp'))
original = entry.get('arguments') or shlex.split(entry['command'])
base = []
iterator = iter(original)
for token in iterator:
    if token in ('-o', '-MF', '-MQ', '-MT'):
        next(iterator)
    elif token not in ('-c', '-MD', '-MMD', entry['file']):
        base.append(token)
sanitizer_flags = ['-fsanitize=address,undefined'] if arguments.sanitize else []
base.extend(sanitizer_flags)
source = folder / 'inventory.cpp'
object_file = workspace / 'inventory.o'
compile_command = [*base, '-c', str(source), '-o', str(object_file)]
targets = json.loads(subprocess.check_output(['meson', 'introspect', str(build), '--targets'], text=True))
libraries = [next(target['filename'][0] for target in targets if target['name'] == name)
             for name in ('asterorm_pg', 'asterorm_core')]
executable = workspace / 'inventory'
link = [base[0], str(object_file), *libraries, '-pthread', *sanitizer_flags,
        *['-Wl,-rpath,' + str(path) for path in sorted({Path(name).parent for name in libraries})],
        '-o', str(executable)]
print('Inventory workspace:', workspace, flush=True)
subprocess.run(compile_command, cwd=entry['directory'], check=True)
subprocess.run(link, cwd=entry['directory'], check=True)
completed = subprocess.run([str(executable)], cwd=root, text=True, capture_output=True, timeout=45)
(workspace / 'native.stdout').write_text(completed.stdout)
(workspace / 'native.stderr').write_text(completed.stderr)
if completed.returncode:
    raise RuntimeError(completed.stderr)
observations = {}
for line in completed.stdout.splitlines():
    name, value = line.split('\t', 1)
    assert name not in observations, name
    observations[name] = bytes.fromhex(value).decode()
assert observations['cleanup.restored'] == 'true'
assert observations['server.0.0'].startswith('16.'), observations['server.0.0']
assert observations['range.discrete.0.0'] == '[1,4)'
assert observations['json.path_rows.rows'] == '2'
assert observations['network.cidr_hostbits.sqlstate'] == '22P02'
for name in ('range','json','inet','interval','mac','hstore'):
    assert 'Index' in observations['index.plan.'+name+'.0.0'], name
controls = {}
probe = folder / 'compile_probe.cpp'
database = [dict(directory=entry['directory'], file=str(source), arguments=compile_command)]
for number, name, diagnostic in [
    (0,'existing values and custom codec',None),
    (1,'range',r"no (?:template|type|member) named 'range'"), (2,'interval',r"no (?:template|type|member) named 'interval'"), (3,'network',r"no (?:template|type|member) named 'network'"),
    (4,'macaddr',r"no (?:template|type|member) named 'macaddr'"), (5,'hstore alias',r"no (?:type|member) named 'hstore'"),
    (6,'map hstore codec','Unsupported type for encoding'),
    (7,'recursive entity codec','Unsupported type for encoding'),
    (8,'runtime JSON extraction',r"no member named 'json_get'"), (9,'typed JSON extraction',r"no member named 'json_get'"),
    (10,'JSON path set-returning builder',r"no member named 'jsonb_path_query'")]:
    command = [*base, '-fsyntax-only', '-fdiagnostics-color=never', '-DTYPE_CASE='+str(number), str(probe)]
    result = subprocess.run(command, cwd=entry['directory'], text=True, capture_output=True)
    (workspace / ('compile-'+str(number)+'.log')).write_text(result.stdout+result.stderr)
    if number == 0:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0 and re.search(diagnostic, result.stderr), (name, result.stderr)
    controls[name] = dict(exit_code=result.returncode, diagnostic=diagnostic)
    print('compile',name,':',result.returncode,flush=True)
database.append(dict(directory=entry['directory'],file=str(probe),arguments=[*base,'-DTYPE_CASE=0','-c',str(probe),'-o',str(workspace/'compile_probe.o')]))
(workspace / 'compile_commands.json').write_text(json.dumps(database,indent=2)+'\n')
(workspace / 'commands.json').write_text(json.dumps(dict(compile=compile_command,link=link,run=[str(executable)]),indent=2)+'\n')
report = dict(native_exit=completed.returncode, sanitizers=['address','undefined'] if arguments.sanitize else [], compile_controls=controls,observations=observations)
output = arguments.output or workspace/'results.json'
output.write_text(json.dumps(report,indent=2)+'\n')
print('Native observations:',len(observations),'compile controls:',len(controls),flush=True)
print('Results:',output,flush=True)

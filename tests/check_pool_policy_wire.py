"""Reject named Parse packets while exercising the real libpq cached-call entry point."""
import socket
import subprocess
import sys
import threading
from check_pool_health_wire import peer

for mode in ('off', 'zero', 'profile', 'cached-control'):
    observations, errors = [], []
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        listener.settimeout(8)
        worker = threading.Thread(target=peer, args=(listener, 'success', observations, errors))
        worker.start()
        completed = subprocess.run([sys.argv[1], str(listener.getsockname()[1]), mode],
                                   capture_output=True, text=True, timeout=10)
        worker.join(9)
        assert not worker.is_alive(), (mode, 'peer did not stop')
        if mode == 'cached-control':
            assert completed.returncode == 2 and any('named preparations' in error for error in errors), (mode, completed.returncode, errors)
            assert observations == [], observations
        else:
            assert completed.returncode == 0, (mode, completed.returncode, completed.stderr, errors)
            assert not errors and observations == ['SELECT 41'], (mode, observations, errors)
        print('PASS:', mode, flush=True)
print('3 unnamed real-libpq policy scenarios and named-Parse negative control passed')

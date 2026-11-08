"""Drive real libpq against a controlled loopback PostgreSQL wire peer."""
import socket
import struct
import subprocess
import sys
import threading
import time


def message(tag, payload=b''):
    return tag + struct.pack('!I', len(payload) + 4) + payload


def exact(connection, size):
    output = b''
    while len(output) < size:
        chunk = connection.recv(size - len(output))
        if not chunk:
            raise EOFError()
        output += chunk
    return output


def rows(value):
    description = struct.pack('!H', 1) + b'?column?\0' + struct.pack('!IhIhih', 0, 0, 23, 4, -1, 0)
    data = struct.pack('!HI', 1, len(value)) + value
    return message(b'1') + message(b'2') + message(b'T', description) + message(b'D', data) + message(b'C', b'SELECT 1\0')


def peer(listener, scenario, observations, errors):
    try:
        connection, _ = listener.accept()
        with connection:
            connection.settimeout(8)
            size = struct.unpack('!I', exact(connection, 4))[0]
            assert 8 <= size <= 4096
            startup = exact(connection, size - 4)
            assert startup[:4] == struct.pack('!I', 196608)
            connection.sendall(message(b'R', struct.pack('!I', 0)) +
                               message(b'S', b'server_version\00016.14\0') +
                               message(b'S', b'client_encoding\0UTF8\0') +
                               message(b'K', struct.pack('!II', 1234, 5678)) + message(b'Z', b'I'))
            while True:
                tag = exact(connection, 1)
                size = struct.unpack('!I', exact(connection, 4))[0]
                assert 4 <= size <= 4096
                payload = exact(connection, size - 4)
                if tag == b'X':
                    return
                if tag == b'P':
                    name, query, _ = payload.split(b'\0', 2)
                    assert not name, 'health must not use named preparations'
                    observations.append(query.decode())
                if tag != b'S':
                    continue
                assert observations[-1] in ('SELECT 1', 'SELECT 41')
                if scenario == 'disconnect':
                    return
                if scenario in ('error', 'fatal'):
                    prefix = b'SFATAL\0C57P01\0' if scenario == 'fatal' else b'SERROR\0C23505\0'
                    fields = (prefix + b'Mhealth sentinel\0Ddetail sentinel\0Hhint sentinel\0'
                              b'thealth_table\0chealth_column\0nhealth_constraint\0\0')
                    connection.sendall(message(b'E', fields) + (b'' if scenario == 'fatal' else message(b'Z', b'I')))
                    if scenario == 'fatal':
                        return
                    continue
                if scenario == 'timeout':
                    while connection.recv(1024):
                        pass
                    return
                value = b'2' if scenario == 'wrong' else b'41' if observations[-1] == 'SELECT 41' else b'1'
                response = rows(value)
                if scenario != 'drain_timeout':
                    response += message(b'Z', b'I')
                # Force partial-frame delivery and repeated consume/busy cycles.
                for offset in range(0, len(response), 7):
                    connection.sendall(response[offset:offset + 7])
                    time.sleep(0.001)
                if scenario == 'drain_timeout':
                    while connection.recv(1024):
                        pass
                    return
    except (EOFError, ConnectionResetError, BrokenPipeError):
        if scenario == 'success':
            errors.append('successful peer lost connection before termination')
    except Exception as error:
        errors.append(repr(error))


def main():
    for scenario in ('success', 'timeout', 'drain_timeout', 'error', 'fatal', 'wrong', 'disconnect'):
        for mode in (0, 1):
            observations, errors = [], []
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                listener.listen(1)
                listener.settimeout(8)
                worker = threading.Thread(target=peer, args=(listener, scenario, observations, errors))
                worker.start()
                completed = subprocess.run([sys.argv[1], scenario, str(listener.getsockname()[1]), str(mode)],
                                           capture_output=True, text=True, timeout=10)
                worker.join(9)
                assert not worker.is_alive(), (scenario, 'peer did not stop')
                assert completed.returncode == 0, (scenario, mode, completed.returncode, completed.stderr)
                assert not errors, (scenario, errors)
                expected = ['SELECT 1', 'SELECT 41', 'SELECT 1'] if scenario == 'success' else ['SELECT 1']
                assert observations == expected, (scenario, observations)
                print('PASS:', scenario, 'initial nonblocking mode', mode, flush=True)
    print('14 real-libpq wire scenarios passed')


if __name__ == '__main__':
    main()

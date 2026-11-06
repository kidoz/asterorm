"""Exercise migration locking with independent PostgreSQL client processes."""
import queue
import subprocess
import sys
import threading
import uuid


class Worker:
    def __init__(self, executable, schema, operation, label):
        self.process = subprocess.Popen(
            [executable, schema, operation, label],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True,
        )
        self.lines = queue.Queue()

        def read_lines():
            for line in self.process.stdout:
                self.lines.put(line.strip())
            self.lines.put(None)

        threading.Thread(target=read_lines, daemon=True).start()

    def line(self):
        value = self.lines.get(timeout=10)
        if value is None:
            raise AssertionError(self.process.stderr.read())
        return value

    def send(self, command):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def finish(self):
        status = self.process.wait(timeout=10)
        assert status == 0, self.process.stderr.read()

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)


def run_pair(executable, first_operation, second_operation):
    schema = "migration_serial_" + uuid.uuid4().hex[:16]
    workers = []
    try:
        gate = Worker(executable, schema, "gate", "gate")
        workers.append(gate)
        assert gate.line() == "ready"
        first = Worker(executable, schema, first_operation, "one")
        second = Worker(executable, schema, second_operation, "two")
        workers.extend([first, second])
        gate.send("wait")
        assert gate.line() == "blocked"
        gate.send("release")
        gate.finish()
        results = [first.line(), second.line()]
        first.finish()
        second.finish()
        status = Worker(executable, schema, "status", "check")
        workers.append(status)
        final = status.line()
        status.finish()
        if first_operation == second_operation == "apply":
            assert sorted(results) == ["2 0", "2 2"], results
            assert final == "2 2 1", final
            # A subsequent rollback process must see the committed history.
            rollback = Worker(executable, schema, "rollback", "undo")
            workers.append(rollback)
            assert rollback.line() == "0 2"
            rollback.finish()
        elif second_operation == "rollback":
            assert results[0] == "2 2", results
            assert results[1] in ("0 0", "0 2"), results
            expected = "2 2 1" if results[1] == "0 0" else "0 0 0"
            assert final == expected, (results, final)
        else:
            assert results[0] == "2 2", results
            assert results[1] in ("0", "2"), results
            assert final == "2 2 1", final
    finally:
        for worker in workers:
            worker.stop()
        cleanup = Worker(executable, schema, "cleanup", "cleanup")
        try:
            cleanup.finish()
        finally:
            cleanup.stop()


if __name__ == "__main__":
    for operations in [("apply", "apply"), ("apply", "rollback"), ("apply", "current")]:
        run_pair(sys.argv[1], *operations)
        print("Passed:", "/".join(operations))

# Leader election ring

`myleprocess.py` implements leader election for the two-neighbor asynchronous
ring in the assignment. Each process listens for its predecessor and connects
to its successor. The first line of `config.txt` is the local listening
endpoint. The second line is the endpoint of the successor.

Messages are JSON objects with the required fields `uuid` and `flag`. A
newline separates messages on the persistent TCP connection. A flag of `0`
means that the election is still running. A flag of `1` announces the leader.
The process forwards only candidate UUIDs greater than its own, then forwards
the leader announcement once.

## Run a local three-process ring

Create one config file for each process. For ports `5001`, `5002`, and `5003`,
use these endpoint pairs:

```text
# process 1
127.0.0.1,5001
127.0.0.1,5002

# process 2
127.0.0.1,5002
127.0.0.1,5003

# process 3
127.0.0.1,5003
127.0.0.1,5001
```

Start each process from its own terminal. The `--uuid` option makes a demo
repeatable, but it is optional because the default is `uuid.uuid4()`.

```powershell
& "C:\Users\skarm\AppData\Local\Microsoft\WinGet\Links\uv.exe" run python myleprocess.py --config config1.txt --log log1.txt --uuid 00000000-0000-0000-0000-000000000001
& "C:\Users\skarm\AppData\Local\Microsoft\WinGet\Links\uv.exe" run python myleprocess.py --config config2.txt --log log2.txt --uuid 00000000-0000-0000-0000-000000000003
& "C:\Users\skarm\AppData\Local\Microsoft\WinGet\Links\uv.exe" run python myleprocess.py --config config3.txt --log log3.txt --uuid 00000000-0000-0000-0000-000000000002
```

Each terminal prints the same result:

```text
leader is 00000000-0000-0000-0000-000000000003
```

## Tests

Run the tests with the requested `uv` executable:

```powershell
& "C:\Users\skarm\AppData\Local\Microsoft\WinGet\Links\uv.exe" run python -m unittest discover -s tests -v
```

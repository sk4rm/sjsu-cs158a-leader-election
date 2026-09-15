# Run the leader election program

Open three terminals in the project directory. Run one command in each
terminal, then press Enter after all three programs have started.

```powershell
# Terminal 1
uv run python myleprocess.py --config config.txt --log log.txt

# Terminal 2
uv run python myleprocess.py --config config2.txt --log log2.txt

# Terminal 3
uv run python myleprocess.py --config config3.txt --log log3.txt
```

Example execution:

```text
Terminal 1
[server] server started at port 5001
Press Enter to start client...
[client] client connected to 127.0.0.1:5002
leader is c5711d54-4ecc-43a3-adf5-595818b03a86

Terminal 2
[server] server started at port 5002
Press Enter to start client...
[client] client connected to 127.0.0.1:5003
leader is c5711d54-4ecc-43a3-adf5-595818b03a86

Terminal 3
[server] server started at port 5003
Press Enter to start client...
[client] client connected to 127.0.0.1:5001
leader is c5711d54-4ecc-43a3-adf5-595818b03a86
```

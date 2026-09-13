from __future__ import annotations

import json
import socket
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Node:
    config: Config
    log_path: str = "log.txt"
    id: uuid.UUID | None = None

    _client_thread: threading.Thread | None = None
    _server_thread: threading.Thread | None = None

    def __post_init__(self):
        self.id = uuid.uuid4()

        self._clear_log()
        self._log(f"My id: {self.id}")

        self._client_thread = threading.Thread(
            target=self._connect,
            args=(
                self.id,
                self.config,
            ),
        )

        self._server_thread = threading.Thread(
            target=self._listen,
            args=(self.config,),
        )

    def _clear_log(self):
        with open(self.log_path, mode="w"):
            pass

    def _log(self, message: str):
        with open(self.log_path, mode="a") as log_file:
            log_file.write(message + "\n")

    def _connect(self, id: uuid.UUID, config: Config):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((config.server_host, config.server_port))

            message = Message(id, 0)
            client_socket.sendall(message.encode())

    def _listen(self, config: Config):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("", config.client_port))
            server_socket.listen(1)

            while True:
                (_client_socket, (_client_host, _client_port)) = server_socket.accept()
                self._log(f"Received: {_client_socket} {_client_host} {_client_port}")

    def start_client(self):
        if not self._client_thread:
            raise RuntimeError("client thread not initialized")

        input("Press Enter to start client")
        self._client_thread.start()

    def start_server(self):
        if not self._server_thread:
            raise RuntimeError("server thread not initialized")

        self._server_thread.start()


@dataclass
class Message:
    uuid: uuid.UUID
    flag: int

    def serialize(self):
        return json.dumps(self)

    def encode(self, encoding: str = "utf-8") -> bytes:
        return str(self.uuid).encode(encoding)


@dataclass
class Config:
    client_host: str
    client_port: int
    server_host: str
    server_port: int

    @classmethod
    def from_file(cls, path: str | Path):
        path = Path(path)

        (
            (first_host, first_port),
            (second_host, second_port),
        ) = [
            line.strip().split(sep=",", maxsplit=1)
            for line in path.read_text().splitlines()
            if not line.startswith("#")
        ]

        return cls(
            first_host,
            int(first_port),
            second_host,
            int(second_port),
        )


def main():
    config = Config.from_file("config.txt")

    node = Node(config)
    node.start_server()
    node.start_client()


if __name__ == "__main__":
    raise SystemExit(main())

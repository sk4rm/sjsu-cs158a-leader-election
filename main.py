from __future__ import annotations

import json
import socket
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Node:
    config: Config
    log_path: str = "log.txt"

    _id: uuid.UUID = field(default_factory=uuid.uuid4)
    _state: int = 0
    _leader: uuid.UUID | None = None

    _id_lock: threading.Lock = field(default_factory=threading.Lock)
    _state_lock: threading.Lock = field(default_factory=threading.Lock)
    _leader_lock: threading.Lock = field(default_factory=threading.Lock)

    _client_thread: threading.Thread | None = None
    _server_thread: threading.Thread | None = None
    _buffer_size: int = 4096

    def __post_init__(self):
        self._clear_log()
        self._log(f"My id: {self._id}")

        self._client_thread = threading.Thread(
            target=self._connect,
            args=(
                self._id,
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
            self._log(f"Sent: uuid={message.uuid}, flag={message.flag}")

    def _listen(self, config: Config):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("", config.client_port))
            server_socket.listen(1)

            while True:
                (client_socket, (client_host, client_port)) = server_socket.accept()

                with client_socket:
                    while True:
                        payload = client_socket.recv(self._buffer_size)
                        if not payload:
                            print(f"client {client_host}:{client_port} disconnected")
                            break

                        message = Message.decode(payload)

                        if self._id > message.uuid:
                            comparison = "greater"

                            with self._state_lock:
                                self._state = 1

                                with self._leader_lock:
                                    self._leader = message.uuid

                                self._log(
                                    f"Received: uuid={message.uuid}, flag={message.flag}, {comparison}, {self._state}"
                                )
                        elif self._id < message.uuid:
                            self._log(
                                f"Received: uuid={message.uuid}, flag={message.flag}, less"
                            )
                        else:
                            self._log(
                                f"Ignored: uuid={message.uuid}, flag={message.flag}"
                            )

    def start_client(self):
        if not self._client_thread:
            raise RuntimeError("client thread not initialized")

        input("Press Enter to start client\n")
        self._client_thread.start()

    def start_server(self):
        if not self._server_thread:
            raise RuntimeError("server thread not initialized")

        self._server_thread.start()


@dataclass
class Message:
    uuid: uuid.UUID
    flag: int

    def encode(self) -> bytes:
        data = {
            "uuid": str(self.uuid),
            "flag": self.flag,
        }
        return json.dumps(data).encode("utf-8")

    @staticmethod
    def decode(data: bytes) -> Message:
        obj = json.loads(data.decode("utf-8"))
        return Message(
            uuid.UUID(obj["uuid"]),
            obj["flag"],
        )


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

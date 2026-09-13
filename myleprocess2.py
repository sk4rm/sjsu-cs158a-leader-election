"""Leader election process for a unidirectional TCP ring.

Each process has one listening socket and one outgoing connection.  The
configuration file describes those two endpoints.  Messages use compact JSON
objects and are separated by whitespace on the TCP stream, so the receiver
can handle both one-message-per-send and multiple messages in one ``recv``.

The election is the Chang-Roberts algorithm.  Every process sends its own
UUID once.  A process forwards only UUIDs greater than its own.  When the
largest UUID returns to its owner, that process sends a flag-1 announcement
around the ring.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO, Union
from uuid import UUID

JsonValue = Union[str, int, dict[str, Any]]


class ProtocolError(ValueError):
    """Raised when a peer sends a message that does not match the protocol."""


class MessageEncoder(json.JSONEncoder):
    """Encode ``Message`` and ``UUID`` values for callers using ``json.dumps``."""

    def default(self, o: Any) -> Any:
        if isinstance(o, Message):
            return o.to_dict()
        if isinstance(o, UUID):
            return str(o)
        return super().default(o)


@dataclass
class Message:
    """A leader-election message.

    The public member names intentionally match the assignment: ``uuid`` and
    ``flag``.  UUIDs are encoded as strings because JSON has no UUID type.
    """

    uuid: UUID
    flag: int = 0

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.uuid, UUID):
                self.uuid = UUID(str(self.uuid))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid uuid: {self.uuid!r}") from exc

        if (
            not isinstance(self.flag, int)
            or isinstance(self.flag, bool)
            or self.flag not in (0, 1)
        ):
            raise ValueError("flag must be 0 or 1")

    def to_dict(self) -> dict[str, JsonValue]:
        """Return the JSON-compatible representation of this message."""

        return {"uuid": str(self.uuid), "flag": self.flag}

    def to_json(self) -> str:
        """Serialize this message as a JSON object."""

        return json.dumps(self, cls=MessageEncoder, separators=(",", ":"))

    def serialize(self) -> str:
        """Alias for :meth:`to_json` for simple callers."""

        return self.to_json()

    @classmethod
    def from_dict(cls, value: Any) -> Message:
        """Build a message from a decoded JSON object."""

        if not isinstance(value, dict):
            raise ProtocolError("message must be a JSON object")
        if "uuid" not in value or "flag" not in value:
            raise ProtocolError("message must contain uuid and flag")
        try:
            return cls(value["uuid"], value["flag"])
        except (TypeError, ValueError) as exc:
            raise ProtocolError(str(exc)) from exc

    @classmethod
    def from_json(cls, value: str | bytes | bytearray) -> Message:
        """Deserialize a message from JSON text or UTF-8 bytes."""

        if isinstance(value, (bytes, bytearray)):
            try:
                value = bytes(value).decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProtocolError("message is not valid UTF-8") from exc
        try:
            decoded = json.loads(value)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ProtocolError("message is not valid JSON") from exc
        return cls.from_dict(decoded)

    @classmethod
    def deserialize(cls, value: str | bytes | bytearray) -> Message:
        """Alias for :meth:`from_json` for simple callers."""

        return cls.from_json(value)


@dataclass(frozen=True)
class NodeConfig:
    """The two endpoints used by one process in the ring."""

    server_host: str
    server_port: int
    client_host: str
    client_port: int

    @property
    def server_address(self) -> tuple[str, int]:
        return self.server_host, self.server_port

    @property
    def client_address(self) -> tuple[str, int]:
        return self.client_host, self.client_port

    @classmethod
    def from_file(cls, path: str | Path) -> NodeConfig:
        """Read the server endpoint and client endpoint from ``path``.

        The file format is two non-empty lines.  Each line contains
        ``host,port``.  Blank lines and lines beginning with ``#`` are ignored
        so a local configuration can include a short comment.
        """

        config_path = Path(path)
        try:
            raw_lines = config_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise ValueError(f"cannot read config file {config_path}: {exc}") from exc

        lines = [
            line.strip()
            for line in raw_lines
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if len(lines) != 2:
            raise ValueError(
                "config.txt must contain exactly two non-empty lines: "
                "server host,port followed by client host,port"
            )

        server_host, server_port = _parse_endpoint(lines[0], "server")
        client_host, client_port = _parse_endpoint(lines[1], "client")
        return cls(server_host, server_port, client_host, client_port)


def _parse_endpoint(line: str, label: str) -> tuple[str, int]:
    host_part, separator, port_part = line.partition(",")
    host = host_part.strip()
    port_text = port_part.strip()
    if not separator or not host or not port_text or "," in port_text:
        raise ValueError(f"invalid {label} endpoint {line!r}; expected host,port")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"invalid {label} port {port_text!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"invalid {label} port {port}; expected 1 through 65535")
    return host, port


def read_config(path: str | Path = "config.txt") -> NodeConfig:
    """Compatibility helper for callers that prefer a function API."""

    return NodeConfig.from_file(path)


class LeaderElectionNode:
    """One process in the leader-election ring."""

    def __init__(
        self,
        config: NodeConfig,
        node_uuid: UUID | str | None = None,
        log_path: str | Path | None = "log.txt",
        connect_timeout: float = 30.0,
        retry_delay: float = 0.25,
    ) -> None:
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")
        if retry_delay <= 0:
            raise ValueError("retry_delay must be positive")

        self.config = config
        if node_uuid is None:
            self.uuid = uuid.uuid4()
        else:
            self.uuid = (
                node_uuid if isinstance(node_uuid, UUID) else UUID(str(node_uuid))
            )
        self.leader_id: UUID | None = None
        self.state = 0

        self.log_path = Path(log_path) if log_path is not None else None
        self.connect_timeout = connect_timeout
        self.retry_delay = retry_delay

        self._lifecycle_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._socket_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._log_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._terminated = threading.Event()
        self._server_ready = threading.Event()
        self._incoming_ready = threading.Event()
        self._outgoing_ready = threading.Event()
        self._connections_ready = threading.Event()

        self._server_socket: socket.socket | None = None
        self._incoming_socket: socket.socket | None = None
        self._outgoing_socket: socket.socket | None = None
        self._server_thread: threading.Thread | None = None
        self._startup_error: BaseException | None = None
        self._log_file: TextIO | None = None
        self._started = False
        self._initial_message_sent = False
        self._receive_buffer = ""
        self._json_decoder = json.JSONDecoder()

    @property
    def flag(self) -> int:
        """Return the node's current election state as a protocol flag."""

        with self._state_lock:
            return self.state

    def start(self, timeout: float | None = None) -> None:
        """Bind, accept, and connect the two persistent ring connections."""

        with self._lifecycle_lock:
            if self._connections_ready.is_set():
                return
            if self._started:
                self._raise_startup_error()
                raise RuntimeError("node startup is already in progress")
            self._started = True
            self._open_log()
            self._log(f"Process id: {self.uuid}")
            self._server_thread = threading.Thread(
                target=self._accept_connection,
                name=f"leader-election-server-{self.uuid}",
                daemon=True,
            )
            self._server_thread.start()

        deadline = time.monotonic() + (
            self.connect_timeout if timeout is None else timeout
        )
        try:
            self._wait_for_event(self._server_ready, deadline, "server socket")
            self._raise_startup_error()
            self._connect_to_server(deadline)
            self._wait_for_event(self._incoming_ready, deadline, "incoming connection")
            self._raise_startup_error()
            self._connections_ready.set()
        except BaseException:
            self.stop()
            raise

    def run(self) -> UUID:
        """Run the election and return the UUID chosen as leader."""

        try:
            self.start()
            self.send_initial_message()
            self._receive_loop()
            with self._state_lock:
                leader_id = self.leader_id
            if leader_id is None:
                raise RuntimeError("connections ended before a leader was elected")
            print(f"leader is {leader_id}")
            return leader_id
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop background work and release local sockets and the log file."""

        self._stop_event.set()
        with self._socket_lock:
            sockets = [
                self._server_socket,
                self._incoming_socket,
                self._outgoing_socket,
            ]
            self._server_socket = None
            self._incoming_socket = None
            self._outgoing_socket = None

        for sock in sockets:
            if sock is None:
                continue
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

        server_thread = self._server_thread
        if (
            server_thread is not None
            and server_thread.is_alive()
            and server_thread is not threading.current_thread()
        ):
            server_thread.join(timeout=1.0)

        self._close_log()

    def send_initial_message(self) -> None:
        """Send this process's UUID once, without comparing it first."""

        if not self._connections_ready.is_set():
            raise RuntimeError("connections are not ready")
        with self._lifecycle_lock:
            if self._initial_message_sent:
                return
            self._initial_message_sent = True
        self._send_message(Message(self.uuid, 0))

    def process_message(self, message: Message) -> bool:
        """Process one received message and return whether it was forwarded."""

        if not isinstance(message, Message):
            if isinstance(message, dict):
                message = Message.from_dict(message)
            else:
                message = Message.from_json(message)

        with self._state_lock:
            current_state = self.state
            current_leader = self.leader_id

        comparison = _compare_uuids(message.uuid, self.uuid)
        received = (
            f"Received: uuid={message.uuid}, flag={message.flag}, {comparison}, "
            f"state={current_state}"
        )
        if current_leader is not None:
            received += f", leader_id={current_leader}"
        self._log(received)

        if message.flag == 0:
            if current_state != 0:
                self._log(
                    f"Ignored: uuid={message.uuid}, flag=0, leader is already known"
                )
                return False

            if comparison == "greater":
                self._send_message(message)
                return True

            if comparison == "same":
                with self._state_lock:
                    self.state = 1
                    self.leader_id = self.uuid
                self._log(f"Leader is decided to {self.leader_id}.")
                self._send_message(Message(self.uuid, 1))
                return True

            self._log(
                f"Ignored: uuid={message.uuid}, flag=0, "
                "candidate is less than this process"
            )
            return False

        # A flag-1 message announces a leader.  Normally every node sees the
        # same UUID here.  Comparing announcements as well makes a node keep
        # the largest announcement if a peer sends messages in an unusual
        # order, while still forwarding each accepted announcement once.
        with self._state_lock:
            known_leader = self.leader_id
            if known_leader is None or message.uuid > known_leader:
                self.state = 1
                self.leader_id = message.uuid
                accepted_new_leader = True
            else:
                accepted_new_leader = message.uuid == known_leader
            selected_leader = self.leader_id

        if message.uuid != selected_leader:
            self._log(
                f"Ignored: uuid={message.uuid}, flag=1, "
                f"leader_id={selected_leader} is already known"
            )
            return False

        if accepted_new_leader:
            self._log(f"Leader is decided to {selected_leader}.")

        if message.uuid == self.uuid:
            self._terminated.set()
            self._log(
                f"Ignored: uuid={message.uuid}, flag=1, "
                "leader announcement returned to its owner"
            )
            return False

        self._send_message(message)
        self._terminated.set()
        return True

    # A descriptive alias makes the state-machine entry point easy to find.
    handle_message = process_message

    def _accept_connection(self) -> None:
        server_socket: socket.socket | None = None
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(self.config.server_address)
            server_socket.listen(1)
            server_socket.settimeout(0.5)
            with self._socket_lock:
                self._server_socket = server_socket
            self._server_ready.set()

            while not self._stop_event.is_set():
                try:
                    incoming, _ = server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._stop_event.is_set():
                        return
                    raise

                incoming.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with self._socket_lock:
                    self._incoming_socket = incoming
                self._incoming_ready.set()
                return
        except BaseException as exc:  # noqa: BLE001
            if not self._stop_event.is_set():
                self._set_startup_error(exc)
                self._server_ready.set()
        finally:
            if server_socket is not None:
                try:
                    server_socket.close()
                except OSError:
                    pass
                with self._socket_lock:
                    if self._server_socket is server_socket:
                        self._server_socket = None

    def _connect_to_server(self, deadline: float) -> None:
        last_error: BaseException | None = None
        while not self._stop_event.is_set():
            self._raise_startup_error()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            outgoing: socket.socket | None = None
            try:
                outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                outgoing.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                outgoing.settimeout(min(1.0, remaining))
                outgoing.connect(self.config.client_address)
                outgoing.settimeout(None)
                with self._socket_lock:
                    self._outgoing_socket = outgoing
                self._outgoing_ready.set()
                return
            except OSError as exc:
                last_error = exc
                if outgoing is not None:
                    try:
                        outgoing.close()
                    except OSError:
                        pass
                if deadline - time.monotonic() <= 0:
                    break
                self._stop_event.wait(
                    min(self.retry_delay, max(0.0, deadline - time.monotonic()))
                )

        if self._stop_event.is_set():
            raise RuntimeError("node stopped during startup")
        raise TimeoutError(
            f"could not connect to {self.config.client_host}:{self.config.client_port}"
        ) from last_error

    def _receive_loop(self) -> None:
        while not self._stop_event.is_set() and not self._terminated.is_set():
            message = self._receive_message()
            if message is None:
                if self._terminated.is_set() or self._stop_event.is_set():
                    return
                raise ConnectionError("peer closed the incoming connection")
            self.process_message(message)

    def _receive_message(self) -> Message | None:
        while not self._stop_event.is_set():
            self._receive_buffer = self._receive_buffer.lstrip()
            if self._receive_buffer:
                try:
                    decoded, end = self._json_decoder.raw_decode(self._receive_buffer)
                except json.JSONDecodeError:
                    decoded = None
                    end = 0
                else:
                    self._receive_buffer = self._receive_buffer[end:]
                    return Message.from_dict(decoded)

            if len(self._receive_buffer.encode("utf-8")) > 65536:
                raise ProtocolError("message exceeds 64 KiB")

            sock = self._incoming_socket
            if sock is None:
                raise RuntimeError("incoming connection is not available")
            try:
                data = sock.recv(4096)
            except OSError:
                if self._stop_event.is_set():
                    return None
                raise
            if not data:
                if self._receive_buffer.strip():
                    raise ProtocolError("peer closed with an incomplete JSON message")
                return None
            try:
                self._receive_buffer += data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProtocolError("message is not valid UTF-8") from exc

        return None

    def _send_message(self, message: Message) -> None:
        payload = (message.to_json() + "\n").encode("utf-8")
        sock = self._outgoing_socket
        if sock is None:
            raise RuntimeError("outgoing connection is not available")
        with self._send_lock:
            sock.sendall(payload)
        self._log(f"Sent: uuid={message.uuid}, flag={message.flag}")

    def _open_log(self) -> None:
        if self.log_path is None:
            return
        self._log_file = self.log_path.open("w", encoding="utf-8")

    def _log(self, line: str) -> None:
        with self._log_lock:
            if self._log_file is not None:
                self._log_file.write(line + "\n")
                self._log_file.flush()

    def _close_log(self) -> None:
        with self._log_lock:
            if self._log_file is not None:
                self._log_file.close()
                self._log_file = None

    def _set_startup_error(self, error: BaseException) -> None:
        with self._lifecycle_lock:
            if self._startup_error is None:
                self._startup_error = error

    def _raise_startup_error(self) -> None:
        if self._startup_error is not None:
            raise RuntimeError("could not start server socket") from self._startup_error

    def _wait_for_event(
        self, event: threading.Event, deadline: float, name: str
    ) -> None:
        while not event.is_set():
            self._raise_startup_error()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"timed out waiting for {name}")
            if event.wait(min(0.05, remaining)):
                break


def _compare_uuids(left: UUID, right: UUID) -> str:
    if left > right:
        return "greater"
    if left < right:
        return "less"
    return "same"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run leader election in a TCP ring")
    parser.add_argument(
        "--config",
        default="config.txt",
        help="two-line host,port configuration file (default: config.txt)",
    )
    parser.add_argument(
        "--log",
        default="log.txt",
        help="log file path (default: log.txt)",
    )
    parser.add_argument(
        "--uuid",
        dest="node_uuid",
        help="optional UUID, useful for repeatable local demonstrations",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=30.0,
        help="seconds to retry the outgoing connection (default: 30)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=0.25,
        help="seconds between outgoing connection attempts (default: 0.25)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = NodeConfig.from_file(args.config)
        node_uuid = UUID(args.node_uuid) if args.node_uuid else None
        node = LeaderElectionNode(
            config,
            node_uuid=node_uuid,
            log_path=args.log,
            connect_timeout=args.connect_timeout,
            retry_delay=args.retry_delay,
        )
        node.run()
    except KeyboardInterrupt:
        return 130
    except (
        OSError,
        ValueError,
        RuntimeError,
        TimeoutError,
        ConnectionError,
        ProtocolError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import io
import json
import socket
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from uuid import UUID

from myleprocess import LeaderElectionNode, Message, MessageEncoder, NodeConfig


class MessageTests(unittest.TestCase):
    def test_message_round_trip_uses_required_field_names(self) -> None:
        identifier = UUID("00000000-0000-0000-0000-000000000123")
        message = Message(identifier, 1)

        self.assertEqual(message.to_dict(), {"uuid": str(identifier), "flag": 1})
        self.assertEqual(Message.from_json(message.to_json()).uuid, identifier)
        self.assertEqual(Message.from_json(message.to_json()).flag, 1)
        self.assertEqual(json.loads(json.dumps(message, cls=MessageEncoder)), message.to_dict())

    def test_message_rejects_invalid_flags(self) -> None:
        with self.assertRaises(ValueError):
            Message(UUID(int=1), 2)
        with self.assertRaises(ValueError):
            Message(UUID(int=1), True)


class ConfigTests(unittest.TestCase):
    def test_config_reads_server_then_client_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.txt"
            path.write_text(
                "127.0.0.1,5001\n127.0.0.1,5002\n",
                encoding="utf-8",
            )

            config = NodeConfig.from_file(path)

        self.assertEqual(config.server_address, ("127.0.0.1", 5001))
        self.assertEqual(config.client_address, ("127.0.0.1", 5002))

    def test_config_rejects_missing_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.txt"
            path.write_text("127.0.0.1,5001\n", encoding="utf-8")

            with self.assertRaises(ValueError):
                NodeConfig.from_file(path)


class ElectionIntegrationTests(unittest.TestCase):
    def test_three_nodes_choose_the_largest_uuid(self) -> None:
        ports = []
        reservations = []
        try:
            for _ in range(3):
                reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                reservation.bind(("127.0.0.1", 0))
                reservations.append(reservation)
                ports.append(reservation.getsockname()[1])
        finally:
            for reservation in reservations:
                reservation.close()

        identifiers = [
            UUID("00000000-0000-0000-0000-000000000001"),
            UUID("00000000-0000-0000-0000-000000000003"),
            UUID("00000000-0000-0000-0000-000000000002"),
        ]

        with tempfile.TemporaryDirectory() as directory:
            nodes = []
            for index in range(3):
                config = NodeConfig(
                    "127.0.0.1",
                    ports[index],
                    "127.0.0.1",
                    ports[(index + 1) % 3],
                )
                nodes.append(
                    LeaderElectionNode(
                        config,
                        node_uuid=identifiers[index],
                        log_path=Path(directory) / f"log{index + 1}.txt",
                        connect_timeout=5.0,
                        retry_delay=0.02,
                    )
                )

            results = [None, None, None]
            errors = []

            def run_node(index: int) -> None:
                try:
                    results[index] = nodes[index].run()
                except BaseException as exc:  # surface worker failures in the test
                    errors.append(exc)

            threads = [
                threading.Thread(target=run_node, args=(index,), daemon=True)
                for index in range(3)
            ]
            output = io.StringIO()
            with redirect_stdout(output):
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=8.0)

            for node in nodes:
                node.stop()

            self.assertFalse(errors, errors)
            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(results, [identifiers[1]] * 3)
            self.assertTrue(all(node.state == 1 for node in nodes))
            self.assertTrue(all(node.leader_id == identifiers[1] for node in nodes))

            logs = []
            for index in range(3):
                log = (Path(directory) / f"log{index + 1}.txt").read_text(
                    encoding="utf-8"
                )
                logs.append(log)
                self.assertIn(f"Process id: {identifiers[index]}", log)
                self.assertIn("Received:", log)
                self.assertIn("Sent:", log)
                self.assertIn("flag=1", log)
            self.assertIn("Ignored:", "\n".join(logs))


if __name__ == "__main__":
    unittest.main()

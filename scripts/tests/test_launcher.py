"""Configuration-only launcher regression checks; never bind ports or start nodes."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class LauncherTest(unittest.TestCase):
    def run_launcher(self, directory, nodes):
        return subprocess.run(
            [str(ROOT / 'scripts/run_cluster.sh'), '--nodes', str(nodes),
             '--run-dir', str(directory), '--generate-only'],
            capture_output=True, text=True,
        )

    def test_five_symmetric_voters_and_unique_endpoints(self):
        with tempfile.TemporaryDirectory(prefix='raftkv-config-') as directory:
            result = self.run_launcher(directory, 5)
            self.assertEqual(result.returncode, 0, result.stderr)
            for node in range(1, 6):
                config = (Path(directory) / f'node{node}.toml').read_text()
                self.assertEqual(config.count('[[peers]]'), 5)
                self.assertIn(f'ui_listen = "127.0.0.1:{8079+node}"', config)
                self.assertIn(f'client_listen = "127.0.0.1:{6378+node}"', config)
                for peer in range(1, 6):
                    self.assertIn(f'raft_addr = "http://127.0.0.1:{7000+peer}"', config)

    def test_membership_change_preserves_old_configuration(self):
        with tempfile.TemporaryDirectory(prefix='raftkv-config-') as directory:
            self.assertEqual(self.run_launcher(directory, 3).returncode, 0)
            path = Path(directory) / 'node1.toml'
            original = path.read_bytes()
            result = self.run_launcher(directory, 5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('fresh', result.stderr)
            self.assertEqual(path.read_bytes(), original)

    def test_invalid_size_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='raftkv-config-') as directory:
            for count in (0, 32, 'invalid'):
                self.assertNotEqual(self.run_launcher(directory, count).returncode, 0)
            self.assertEqual(list(Path(directory).glob('node*.toml')), [])


if __name__ == '__main__':
    unittest.main()

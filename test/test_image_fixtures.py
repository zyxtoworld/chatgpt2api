from __future__ import annotations

import struct
import tempfile
import unittest
from pathlib import Path

from test.fixtures.image_inputs import FIXTURE_NAMES, image_fixture_bytes


class ImageFixtureTests(unittest.TestCase):
    def test_image_fixtures_are_deterministic_valid_pngs_and_read_back(self) -> None:
        png_signature = b"\x89PNG\r\n\x1a\n"
        with tempfile.TemporaryDirectory() as temp_dir:
            for name in FIXTURE_NAMES:
                payload = image_fixture_bytes(name)
                self.assertTrue(payload.startswith(png_signature))
                self.assertEqual(payload[12:16], b"IHDR")
                width, height, bit_depth, color_type = struct.unpack(">IIBB", payload[16:26])
                self.assertEqual((width, height), (16, 16))
                self.assertEqual((bit_depth, color_type), (8, 2))

                path = Path(temp_dir) / name
                path.write_bytes(payload)
                self.assertEqual(path.read_bytes(), payload)
                self.assertEqual(image_fixture_bytes(name), payload)

from __future__ import annotations

import unittest
from unittest import mock

import utils.image_tokens as image_tokens
from utils.image_tokens import (
    count_image_input_tokens,
    count_image_output_tokens,
)
from utils.helper import MAX_JSON_IMAGE_BYTES


class ImageTokenTests(unittest.TestCase):
    def test_patch_token_examples_match_openai_docs(self):
        self.assertEqual(count_image_input_tokens(1024, 1024, "gpt-4.1-mini", "high"), 1659)
        self.assertEqual(count_image_input_tokens(1800, 2400, "gpt-4.1-mini", "high"), 2353)

    def test_image_input_tokens_force_gpt_54_mini(self):
        expected = count_image_input_tokens(1024, 1024, "gpt-5.4-mini", "low")
        self.assertEqual(expected, 415)
        self.assertEqual(count_image_input_tokens(1024, 1024, "gpt-4o", "low"), expected)
        self.assertEqual(count_image_input_tokens(1024, 1024, "gpt-image-2", "low"), expected)

    def test_image_dimensions_do_not_coerce_non_integer_values(self):
        malformed = [
            {"type": "image", "width": 1.5, "height": 1024},
            {"type": "image", "width": True, "height": 1024},
        ]

        self.assertEqual(image_tokens.count_image_content_tokens(malformed, "gpt-5-mini"), 0)

    def test_image_output_tokens_scale_by_count_and_size(self):
        single = count_image_output_tokens("1024x1024", "auto", 1)
        self.assertGreater(single, 0)
        self.assertEqual(count_image_output_tokens("1024x1024", "auto", 2), single * 2)

    def test_oversized_data_url_is_rejected_before_base64_decode(self):
        encoded_limit = ((MAX_JSON_IMAGE_BYTES + 2) // 3) * 4
        content = [{
            "type": "image_url",
            "image_url": {"url": "data:image/png;base64," + "A" * (encoded_limit + 1)},
        }]

        with mock.patch.object(image_tokens.base64, "b64decode", return_value=b"") as decode:
            self.assertEqual(image_tokens.count_image_content_tokens(content, "gpt-5-mini"), 0)

        decode.assert_not_called()

    def test_unpadded_payload_at_character_limit_is_rejected_before_decode(self):
        encoded_limit = ((MAX_JSON_IMAGE_BYTES + 2) // 3) * 4
        value = "data:image/png;base64," + "A" * encoded_limit

        with mock.patch.object(image_tokens.base64, "b64decode", return_value=b"") as decode:
            self.assertIsNone(image_tokens.image_size_from_data_url(value))

        decode.assert_not_called()

    def test_exact_max_payload_with_padding_is_allowed_to_decode(self):
        encoded_limit = ((MAX_JSON_IMAGE_BYTES + 2) // 3) * 4
        value = "data:image/png;base64," + "A" * (encoded_limit - 2) + "=="

        with mock.patch.object(image_tokens.base64, "b64decode", return_value=b"") as decode:
            self.assertIsNone(image_tokens.image_size_from_data_url(value))

        decode.assert_called_once()

    def test_bounded_base64_rejects_invalid_shape_and_predicted_overflow_before_decode(self):
        cases = (
            ("padding-zero exact limit", "QUJD", 3, True),
            ("padding-one exact limit", "QUI=", 2, True),
            ("padding-two exact limit", "QQ==", 1, True),
            ("predicted limit plus one", "QUJDRA==", 3, False),
            ("non-four-byte length", "A" * 7, 4, False),
        )

        for label, encoded, max_bytes, accepted in cases:
            with self.subTest(label=label):
                with mock.patch.object(
                    image_tokens.base64,
                    "b64decode",
                    wraps=image_tokens.base64.b64decode,
                ) as decode:
                    result = image_tokens._decode_bounded_base64(encoded, max_bytes=max_bytes)
                    if accepted:
                        self.assertIsNotNone(result)
                        decode.assert_called_once()
                    else:
                        self.assertIsNone(result)
                        decode.assert_not_called()

    def test_output_image_decoder_uses_bounded_fifty_megabyte_contract(self):
        with mock.patch.object(image_tokens, "_decode_bounded_base64", return_value=None) as decode:
            image_tokens.count_image_output_items_tokens([{"b64_json": "QUJD"}])

        decode.assert_called_once_with(
            "QUJD",
            max_bytes=image_tokens._MAX_OUTPUT_IMAGE_BYTES,
        )

    def test_valid_data_url_still_contributes_image_tokens(self):
        value = (
            "data:image/png;base64,"
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
        )
        content = [{"type": "image_url", "image_url": {"url": value}}]

        self.assertGreater(image_tokens.count_image_content_tokens(content, "gpt-5-mini"), 0)


if __name__ == "__main__":
    unittest.main()

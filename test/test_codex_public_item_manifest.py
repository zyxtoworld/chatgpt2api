from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import unittest

from services.protocol import openai_v1_response
from services.protocol import responses_websocket
from utils.helper import responses_sse_stream


MANIFEST_PATH = Path(__file__).parents[1] / "services" / "protocol" / "codex_public_item_manifest.json"


class CodexPublicItemManifestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    @staticmethod
    def _positive_item(item_type: str, fields: list[str]) -> dict[str, Any]:
        values: dict[str, Any] = {
            "action": {},
            "approve": True,
            "content": [],
            "environment": {},
            "operation": {},
            "output": {"computer_call_output": {}, "shell_call_output": []}.get(
                item_type,
                "value",
            ),
            "outputs": [],
            "pending_safety_checks": [],
            "queries": [],
            "status": "completed",
            "tools": [],
        }
        return {
            "type": item_type,
            "id": f"{item_type}-id",
            **{field: values.get(field, "value") for field in fields if field not in {"type", "id"}},
        }

    def test_python_public_item_fields_match_shared_manifest(self) -> None:
        expected = {
            item_type: set(fields)
            for item_type, fields in self.manifest["items"].items()
        }
        self.assertEqual(openai_v1_response._PUBLIC_CODEX_ITEM_FIELDS_BY_TYPE, expected)

    def test_python_nested_projectors_match_manifest_and_cover_all_types(self) -> None:
        canary = "codex-cross-type-item-canary"
        for item_type, fields in self.manifest["items"].items():
            with self.subTest(item_type=item_type):
                item = self._positive_item(item_type, fields)
                projected = openai_v1_response._project_public_codex_response_item(item)
                self.assertEqual(set(projected), set(fields))
                self.assertNotIn(canary, json.dumps(projected, ensure_ascii=False))

                for other_type, other_fields in self.manifest["items"].items():
                    foreign = next((field for field in other_fields if field not in fields), None)
                    if foreign is None:
                        continue
                    poisoned = dict(item)
                    poisoned[foreign] = canary
                    foreign_projected = openai_v1_response._project_public_codex_response_item(
                        poisoned
                    )
                    self.assertNotIn(foreign, foreign_projected)
                    self.assertNotIn(canary, json.dumps(foreign_projected, ensure_ascii=False))

    def test_manifest_items_project_in_terminal_and_active_json_events(self) -> None:
        for item_type, fields in self.manifest["items"].items():
            item = self._positive_item(item_type, fields)
            with self.subTest(item_type=item_type):
                terminal = openai_v1_response.project_public_codex_response_event(
                    {
                        "type": "response.completed",
                        "response": {
                            "id": "response-id",
                            "status": "completed",
                            "output": [item],
                        },
                    }
                )
                self.assertEqual(terminal["response"]["output"][0]["type"], item_type)

                active = openai_v1_response.project_public_codex_response_event(
                    {
                        "type": "response.in_progress",
                        "response": {
                            "id": "response-id",
                            "status": "in_progress",
                            "output": [item],
                        },
                    }
                )
                self.assertEqual(active["response"]["output"][0]["type"], item_type)

                malformed = dict(item)
                malformed["type"] = None
                with self.assertRaisesRegex(RuntimeError, "malformed"):
                    openai_v1_response._project_public_codex_response_item(malformed)

    def test_manifest_nested_projectors_drop_recursive_secrets(self) -> None:
        canary = "codex-nested-projector-secret"
        values = {
            "environment": {"type": "container", "id": "container-id", "secret": canary},
            "item_output": {"type": "result", "text": "ok", "secret": canary},
            "operation": {"type": "patch", "path": "README.md", "secret": canary},
            "output_object": {"type": "result", "text": "ok", "secret": canary},
            "outputs": [{"type": "output_text", "text": "ok", "secret": canary}],
            "safety_checks": [{"type": "safety", "reason": "review", "secret": canary}],
        }
        for item_type, projector_fields in self.manifest["nested_projectors"].items():
            for field, projector in projector_fields.items():
                with self.subTest(item_type=item_type, field=field):
                    item = {
                        "type": item_type,
                        "id": f"{item_type}-id",
                        field: values[projector],
                    }
                    projected = openai_v1_response._project_public_codex_response_item(item)
                    self.assertNotIn(canary, json.dumps(projected, ensure_ascii=False))

    def test_manifest_items_use_the_same_json_sse_and_websocket_projection(self) -> None:
        for item_type, fields in self.manifest["items"].items():
            with self.subTest(item_type=item_type):
                item = self._positive_item(item_type, fields)
                event = {
                    "type": "response.completed",
                    "response": {
                        "id": "response-id",
                        "status": "completed",
                        "output": [item],
                    },
                }
                json_projected = openai_v1_response.project_public_codex_response_event(event)
                websocket_projected = responses_websocket.project_public_codex_response_event(event)
                self.assertEqual(websocket_projected, json_projected)
                sse = "".join(responses_sse_stream([json_projected]))
                self.assertIn("event: response.completed\n", sse)
                self.assertIn(f'"type": "{item_type}"', sse)

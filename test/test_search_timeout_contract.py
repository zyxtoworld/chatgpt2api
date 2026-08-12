from __future__ import annotations

from unittest import TestCase, mock

from services.openai_backend_api import OpenAIBackendAPI, SEARCH_TIMEOUT_SECS, SearchTimeoutError


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value


class SearchTimeoutContractTests(TestCase):
    def test_search_uses_one_clamped_budget_across_all_stages(self) -> None:
        self.assertLess(SEARCH_TIMEOUT_SECS, 120.0)

        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "fixture-token"
        clock = _FakeClock()
        stage_budgets: list[float] = []

        def prepare(*args, **kwargs):
            stage_budgets.append(kwargs["timeout_secs"])
            clock.value += 20.0
            return "conduit"

        def bootstrap(*args, **kwargs):
            stage_budgets.append(kwargs["timeout_secs"])
            clock.value += 20.0

        def run(*args, **kwargs):
            stage_budgets.append(kwargs["timeout_secs"])
            clock.value += 20.0
            return "conversation"

        with (
            mock.patch("services.openai_backend_api.time.monotonic", side_effect=clock.monotonic),
            mock.patch.object(backend, "_prepare_search_conversation", side_effect=prepare) as prepare_call,
            mock.patch.object(backend, "_bootstrap", side_effect=bootstrap) as bootstrap_call,
            mock.patch.object(backend, "_run_search_conversation", side_effect=run) as run_call,
            mock.patch.object(backend, "_wait_search_result", return_value={"answer": "ok"}) as wait,
        ):
            result = backend.search("fixture query", timeout_secs=300.0, poll_interval_secs=1.0)

        self.assertEqual(result, {"answer": "ok"})
        self.assertEqual(stage_budgets, [90.0, 70.0, 50.0])
        self.assertEqual(prepare_call.call_count, 1)
        self.assertEqual(bootstrap_call.call_count, 1)
        self.assertEqual(run_call.call_count, 1)
        self.assertEqual(wait.call_args.kwargs["timeout_secs"], 30.0)
        self.assertIn("deadline", wait.call_args.kwargs)

    def test_expired_stage_stops_before_calling_later_stages(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "fixture-token"
        clock = _FakeClock()

        def prepare(*args, **kwargs):
            clock.value = SEARCH_TIMEOUT_SECS
            return "conduit"

        with (
            mock.patch("services.openai_backend_api.time.monotonic", side_effect=clock.monotonic),
            mock.patch.object(backend, "_prepare_search_conversation", side_effect=prepare) as prepare_call,
            mock.patch.object(backend, "_bootstrap") as bootstrap,
            mock.patch.object(backend, "_run_search_conversation") as run,
            mock.patch.object(backend, "_wait_search_result") as wait,
        ):
            with self.assertRaises(SearchTimeoutError):
                backend.search("fixture query", timeout_secs=300.0)

        self.assertEqual(prepare_call.call_count, 1)
        bootstrap.assert_not_called()
        run.assert_not_called()
        wait.assert_not_called()

    def test_blocking_search_stream_is_closed_at_deadline(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        response = mock.Mock()
        response.closed = False

        def close() -> None:
            response.closed = True

        response.close.side_effect = close

        class _ImmediateTimer:
            def __init__(self, interval, callback) -> None:
                self.interval = interval
                self.callback = callback

            def start(self) -> None:
                self.callback()

            def cancel(self) -> None:
                pass

        with (
            mock.patch("services.openai_backend_api.time.monotonic", return_value=0.0),
            mock.patch("services.openai_backend_api.threading.Timer", _ImmediateTimer),
            mock.patch("services.openai_backend_api.iter_sse_payloads", return_value=iter(())),
        ):
            with self.assertRaises(SearchTimeoutError):
                list(backend._iter_search_sse_until(response, 1.0))

        self.assertTrue(response.closed)

    def test_watchdog_close_exception_is_projected_to_search_timeout(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        response = mock.Mock()

        class _ImmediateTimer:
            def __init__(self, interval, callback) -> None:
                self.interval = interval
                self.callback = callback

            def start(self) -> None:
                self.callback()

            def cancel(self) -> None:
                pass

        with (
            mock.patch("services.openai_backend_api.time.monotonic", return_value=0.0),
            mock.patch("services.openai_backend_api.threading.Timer", _ImmediateTimer),
            mock.patch(
                "services.openai_backend_api.iter_sse_payloads",
                side_effect=RuntimeError("response closed"),
            ),
        ):
            with self.assertRaises(SearchTimeoutError):
                list(backend._iter_search_sse_until(response, 1.0))

        response.close.assert_called_once()

    def test_incomplete_last_result_does_not_return_after_deadline(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)

        with (
            mock.patch("services.openai_backend_api.time.monotonic", side_effect=[0.0, 0.0, 5.0]),
            mock.patch.object(backend, "_get_search_conversation", return_value={}) as getter,
            mock.patch.object(
                backend,
                "_extract_search_result",
                return_value={"answer": "partial", "status": "running"},
            ) as extractor,
        ):
            with self.assertRaises(SearchTimeoutError):
                backend._wait_search_result("conversation", 5.0, 30.0, deadline=5.0)

        getter.assert_called_once()
        extractor.assert_called_once_with("conversation", {})

    def test_poll_sleep_is_bounded_by_remaining_budget(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        with (
            mock.patch("services.openai_backend_api.time.monotonic", side_effect=[0.0, 0.0, 4.0, 5.0]),
            mock.patch("services.openai_backend_api.time.sleep") as sleep,
            mock.patch.object(backend, "_get_search_conversation", return_value={}),
            mock.patch.object(
                backend,
                "_extract_search_result",
                return_value={"answer": "partial", "status": "running"},
            ),
        ):
            with self.assertRaises(SearchTimeoutError):
                backend._wait_search_result("conversation", 5.0, 30.0, deadline=5.0)

        sleep.assert_called_once_with(1.0)

import json
import logging

from gnmi_server.logging_config import JsonFormatter


def test_json_formatter_emits_stable_context() -> None:
    record = logging.LogRecord(
        name="gnmi-server",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="configuration accepted",
        args=(),
        exc_info=None,
    )
    record.service = "gnmi-server"
    record.request_id = "request-1"
    record.result = "accepted"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["severity"] == "INFO"
    assert payload["message"] == "configuration accepted"
    assert payload["service"] == "gnmi-server"
    assert payload["request_id"] == "request-1"
    assert payload["result"] == "accepted"
    assert payload["timestamp"].endswith("+00:00")

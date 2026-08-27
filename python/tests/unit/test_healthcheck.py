from gnmi_server.healthcheck import _heartbeat_ready


def test_heartbeat_requires_online_fresh_nonfuture_timestamp() -> None:
    now_ns = 10_000_000_000
    assert _heartbeat_ready(
        {"status": "ONLINE", "last_seen_ns": "9000000000"}, now_ns, 2.0
    )
    assert not _heartbeat_ready(
        {"status": "OFFLINE", "last_seen_ns": "9000000000"}, now_ns, 2.0
    )
    assert not _heartbeat_ready(
        {"status": "ONLINE", "last_seen_ns": "7000000000"}, now_ns, 2.0
    )
    assert not _heartbeat_ready(
        {"status": "ONLINE", "last_seen_ns": "11000000000"}, now_ns, 2.0
    )
    assert not _heartbeat_ready(
        {"status": "ONLINE", "last_seen_ns": "malformed"}, now_ns, 2.0
    )

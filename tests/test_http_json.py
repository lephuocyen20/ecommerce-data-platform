import io
import json

import pytest

from common.http_json import HttpJsonError, LocalHttpJsonClient, trino_query


class Response:
    def __init__(self, payload):
        self.payload = io.BytesIO(json.dumps(payload).encode())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, *args):
        return self.payload.read(*args)


def test_json_client_serializes_body_and_preserves_timeout():
    calls = []

    def opener(request, timeout):
        calls.append((request, timeout))
        return Response({"ok": True})

    result = LocalHttpJsonClient(opener=opener).request_json(
        "http://127.0.0.1:8182/v1/test",
        method="POST",
        body={"x": 1},
        headers={"Accept": "application/json"},
        timeout=20,
    )

    assert result == {"ok": True}
    assert calls[0][1] == 20
    assert calls[0][0].data == b'{"x":1}'


def test_trino_query_follows_next_uri_with_pagination_headers():
    calls = []
    responses = iter([{"data": [[1]], "nextUri": "http://127.0.0.1:18085/v1/next"}, {"data": [[2]]}])

    def opener(request, timeout):
        calls.append((request, timeout))
        return Response(next(responses))

    rows = trino_query(
        "http://127.0.0.1:18085",
        "lakehouse",
        "gold_schema",
        "SELECT 1",
        client=LocalHttpJsonClient(opener=opener),
    )

    assert rows == [[1], [2]]
    assert calls[0][0].get_header("X-trino-user") == "metabase"
    assert calls[0][0].get_header("X-trino-catalog") == "lakehouse"
    assert calls[0][0].data == b"SELECT 1"
    assert calls[1][0].full_url == "http://127.0.0.1:18085/v1/next"
    assert calls[1][1] == 30


def test_json_client_exposes_http_status_and_bounded_detail():
    from urllib.error import HTTPError

    def opener(request, timeout):
        raise HTTPError(request.full_url, 409, "conflict", {}, io.BytesIO(b"x" * 600))

    with pytest.raises(HttpJsonError) as caught:
        LocalHttpJsonClient(opener=opener).request_json("http://127.0.0.1:8182/v1/test", timeout=20)

    assert caught.value.status_code == 409
    assert len(str(caught.value).split(": ", 1)[1]) == 500
    assert len(caught.value.detail) <= 500

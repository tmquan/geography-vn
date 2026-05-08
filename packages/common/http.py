"""HTTP client with retries, on-disk caching, and graceful error wrapping.

Tailored for the ``sapnhap.bando.com.vn`` PHP API surface, which:

* uses **POST** with ``application/x-www-form-urlencoded`` bodies,
* serves JSON with a non-JSON ``Content-Type`` header (PHP's default ``text/html``),
* occasionally injects an HTML warning preamble (``<br /><b>Warning</b>:``)
  when QGIS Server is mid-restart.

We tolerate all three: JSON parse is content-type-agnostic, the cache is keyed
by ``(method, path, sorted-form-fields)``, and 5xx + transient transport errors
get exponential-backoff retries via ``tenacity``.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from packages.common.logging import get_logger
from packages.common.paths import ensure_dir

log = get_logger(__name__)


class HttpError(RuntimeError):
    """Raised when an HTTP request ultimately fails (after retries)."""


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    headers: dict[str, str]
    json: Any
    text: str
    from_cache: bool


# PHP warning preamble we strip before JSON parse — server occasionally leaks
# stack frames as HTML before the actual JSON body.
_PHP_WARN_RE = re.compile(r"<br\s*/?>\s*<b>(?:Warning|Notice|Fatal error)</b>.*?<br\s*/?>",
                          re.DOTALL | re.IGNORECASE)


def _try_parse_json(text: str) -> Any:
    """Best-effort JSON parse: trim whitespace, peel off PHP warnings, then
    json.loads. Raises ``ValueError`` if nothing parses.
    """
    s = text.strip()
    if not s:
        raise ValueError("empty body")
    # Quick path: it really is JSON.
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    # Strip the PHP warning preamble and retry.
    s2 = _PHP_WARN_RE.sub("", s).strip()
    if s2 and s2 != s:
        return json.loads(s2)
    raise ValueError(f"not JSON; starts with: {s[:120]!r}")


class HttpClient:
    """Thin wrapper around :class:`httpx.Client` with caching + retries."""

    def __init__(
        self,
        *,
        base_url: str = "",
        user_agent: str = "geography-vn/0.1",
        verify_ssl: bool = True,
        timeout_s: float = 30.0,
        retries: int = 3,
        retry_backoff_s: float = 1.5,
        delay_between_requests_s: float = 0.0,
        cache_dir: str | Path | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json, text/plain, */*",
            },
            verify=verify_ssl,
            timeout=timeout_s,
            follow_redirects=True,
        )
        self._retries = max(1, retries)
        self._retry_backoff_s = retry_backoff_s
        self._delay = delay_between_requests_s
        self._cache_dir = Path(ensure_dir(cache_dir)) if cache_dir else None

    # ----- public API ------------------------------------------------------
    def post_json(
        self,
        path: str,
        data: dict[str, Any] | None = None,
        *,
        use_cache: bool = True,
    ) -> HttpResponse:
        """POST form-encoded ``data`` to ``path``; parse response as JSON."""
        return self._request("POST", path, data=data, use_cache=use_cache)

    def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        use_cache: bool = True,
    ) -> HttpResponse:
        return self._request("GET", path, params=params, use_cache=use_cache)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ----- internals -------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        use_cache: bool,
    ) -> HttpResponse:
        cache_path = (
            self._cache_path(method, path, params or data or {})
            if (use_cache and self._cache_dir)
            else None
        )

        if cache_path is not None and cache_path.exists():
            try:
                with cache_path.open("r", encoding="utf-8") as fh:
                    cached = json.load(fh)
                return HttpResponse(
                    status_code=cached["status_code"],
                    headers=cached["headers"],
                    json=cached["json"],
                    text=cached.get("text", ""),
                    from_cache=True,
                )
            except Exception as exc:
                log.warning("cache read failed (%s); refetching", exc)

        try:
            resp = self._fetch(method, path, params=params, data=data)
        except RetryError as exc:
            raise HttpError(f"{method} {path} failed after {self._retries} attempts: {exc}") from exc
        except httpx.HTTPError as exc:
            raise HttpError(f"{method} {path} failed: {exc}") from exc

        if cache_path is not None:
            payload = {
                "status_code": resp.status_code,
                "headers": resp.headers,
                "json": resp.json,
                "text": resp.text[:5000],  # keep diagnostic snippet, not full body
            }
            try:
                cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            except OSError as exc:  # pragma: no cover
                log.warning("cache write failed: %s", exc)

        if self._delay:
            time.sleep(self._delay)

        return resp

    def _fetch(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
    ) -> HttpResponse:
        @retry(
            stop=stop_after_attempt(self._retries),
            wait=wait_exponential(multiplier=self._retry_backoff_s, min=1, max=30),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        )
        def _do() -> HttpResponse:
            log.debug("%s %s params=%s data=%s", method, path, params, data)
            r = self._client.request(method, path, params=params, data=data)
            if 500 <= r.status_code < 600:
                raise httpx.HTTPStatusError(
                    f"{r.status_code} {r.reason_phrase}", request=r.request, response=r
                )
            r.raise_for_status()
            try:
                parsed = _try_parse_json(r.text)
            except ValueError as exc:
                raise HttpError(
                    f"{method} {path}: response not parseable as JSON ({exc})"
                ) from exc
            return HttpResponse(
                status_code=r.status_code,
                headers=dict(r.headers),
                json=parsed,
                text=r.text,
                from_cache=False,
            )

        return _do()

    def _cache_path(self, method: str, path: str, body: dict[str, Any]) -> Path:
        assert self._cache_dir is not None
        qs = urlencode(sorted(body.items()), doseq=True)
        key = f"{method.upper()} {path}?{qs}".encode()
        digest = hashlib.sha1(key).hexdigest()[:16]
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", path.strip("/")) or "root"
        return self._cache_dir / f"{slug}__{digest}.json"

"""HTTP transport; HTTP-specific failures stay outside the sync engine."""

import math
import time
from collections.abc import Callable
from email.utils import parsedate_to_datetime

import httpx
from pydantic import TypeAdapter

from ..models import ChangeBatch, Mutation, MutationResult
from ..ports import RetryableError


class HTTPTransport:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        prefix: str = "/v1/sync",
        clock: Callable[[], float] = time.time,
    ):
        self.client, self.prefix, self.clock = client, prefix.rstrip("/"), clock

    def _retry_after(self, response: httpx.Response) -> float:
        value = response.headers.get("Retry-After", "")
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = parsedate_to_datetime(value).timestamp() - self.clock()
            except (ValueError, TypeError, OverflowError):
                return 0
        return max(0, delay) if math.isfinite(delay) else 0

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            response = await self.client.request(method, self.prefix + path, **kwargs)
            response.raise_for_status()
            return response
        except httpx.TransportError as exc:
            raise RetryableError(str(exc)) from exc
        except httpx.HTTPStatusError as exc:
            if (
                exc.response.status_code in (408, 429)
                or exc.response.status_code >= 500
            ):
                raise RetryableError(
                    str(exc), retry_after=self._retry_after(exc.response)
                ) from exc
            raise

    async def push(self, mutations: list[Mutation]) -> list[MutationResult]:
        response = await self._request(
            "POST", "/mutations", json=[m.model_dump(mode="json") for m in mutations]
        )
        return TypeAdapter(list[MutationResult]).validate_python(response.json())

    async def pull(self, cursor: int, limit: int) -> ChangeBatch:
        response = await self._request(
            "GET", "/changes", params={"cursor": cursor, "limit": limit}
        )
        return ChangeBatch.model_validate(response.json())

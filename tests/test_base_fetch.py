import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from app.BaseFetchClass import BaseFetchClass


@pytest.mark.asyncio
async def test_fetch_data_success():
    fetcher = BaseFetchClass()

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json.return_value = {"prices": [[123, 456]]}

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__.return_value = mock_response

    result = await fetcher._fetch_data(mock_session, "http://api.com", {})

    assert result == {"prices": [[123, 456]]}
    mock_session.get.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_data_rate_limit_429():
    """Check rate limit functionality"""

    fetcher = BaseFetchClass()

    mock_response = AsyncMock()
    mock_response.status = 429
    mock_response.text.return_value = "Rate limit exceeded"

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__.return_value = mock_response

    result = await fetcher._fetch_data(mock_session, "http://api.com", {})

    assert result == {}


@pytest.mark.asyncio
async def test_semaphore_limits_concurrency(monkeypatch):
    limit = 2
    fetcher = BaseFetchClass(max_concurrent=limit)

    current_active = 0
    max_observed = 0

    async def mocked_response(*args, **kwargs):
        nonlocal current_active, max_observed
        current_active += 1
        max_observed = max(max_observed, current_active)
        await asyncio.sleep(0.1)
        current_active -= 1

        mock_res = AsyncMock()
        mock_res.status = 200
        mock_res.json.return_value = {}
        return mock_res

    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = mocked_response

    tasks = [fetcher._fetch_data(mock_session, f"url_{i}", {}) for i in range(5)]
    await asyncio.gather(*tasks)

    assert max_observed == limit

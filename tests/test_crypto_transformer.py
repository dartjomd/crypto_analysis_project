import copy
import pandas as pd
import pytest

from app.CryptoTransformer import CryptoTransformer


@pytest.fixture
def transformer():
    return CryptoTransformer()


@pytest.fixture
def mock_api_data():
    return [
        {
            "prices": [[1704067200000, 42000.1234]],
            "total_volumes": [[1704067200000, 1000.555]],
            "market_caps": [[1704067200000, 800000000.777]],
        }
    ]


@pytest.fixture
def mock_coins_data():
    return [("bitcoin", "usd")]


# tests
def test_normalize_success(
    transformer: CryptoTransformer,
    mock_api_data: list[dict[str, list[list]]],
    mock_coins_data: list[tuple[str, str]],
):
    """Check that data is normalized correctly and rounded properly"""

    transformer.normalize_crypto_data(data=mock_api_data, coins_data=mock_coins_data)
    df = transformer.get_normalized_crypto()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert len(df) == 1

    # check columns
    expected_columns = {
        "price",
        "volume",
        "capitalization",
        "date_key",
        "coin_name",
        "currency",
    }
    assert expected_columns.issubset(df.columns)

    # check round
    assert df.iloc[0]["price"] == 42000.12

    # check date key
    assert df.iloc[0]["date_key"] == 20240101


def test_normalize_empty_input(transformer: CryptoTransformer):
    """Check correct work with empty lists"""

    transformer.normalize_crypto_data(data=[], coins_data=[])
    df = transformer.get_normalized_crypto()
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_normalize_mismatch_length(
    transformer: CryptoTransformer, mock_api_data: list[dict[str, list[list]]]
):
    """Check correct work when there is more/less data than actual coin/currency pairs"""

    wrong_coins = [("bitcoin", "usd"), ("ethereum", "usd")]
    transformer.normalize_crypto_data(data=mock_api_data, coins_data=wrong_coins)
    df = transformer.get_normalized_crypto()

    assert df.empty


def test_normalize_empty_column(
    transformer: CryptoTransformer,
    mock_api_data: list[dict[str, list[list]]],
    mock_coins_data: list[tuple[str, str]],
):
    """Check correct work when data has empty column"""

    # go through every column
    for column in ["prices", "total_volumes", "market_caps"]:
        copied_mock_data = copy.deepcopy(mock_api_data)
        copied_mock_data[0][column] = []

        transformer.normalize_crypto_data(
            data=copied_mock_data, coins_data=mock_coins_data
        )
        df = transformer.get_normalized_crypto()

        assert df.empty

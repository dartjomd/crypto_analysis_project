import pytest
import pandas as pd
from unittest.mock import MagicMock
from app.CryptoAnalyzer import CryptoAnalyzer
from app.enums.ColumnsToAnalyzeEnum import ColumnsToAnalyzeEnum
from app.enums.OrderEnum import OrderEnum


@pytest.fixture
def mock_db():
    """Get mock database instance"""
    return MagicMock()


@pytest.fixture
def analyzer(mock_db):
    """Get mock crypto analyzer"""
    return CryptoAnalyzer(db=mock_db, table_name="test_crypto_table")


def test_get_spikes_query_generation(analyzer, mock_db):
    """Test if SQL string is correct and pandas dataframe is created"""

    mock_db.execute_query.return_value = [
        {"coin_name": "bitcoin", "date_key": 20240101, "price": 42000, "price_rank": 1}
    ]

    df = analyzer.get_spikes(
        up_to_rank=3,
        column=ColumnsToAnalyzeEnum.price.value,
        order=OrderEnum.descending.value,
        coin_name="bitcoin",
        currency="usd",
        start_date_key="20240101",
        end_date_key="20240131",
    )

    called_sql = mock_db.execute_query.call_args[1]["query"]

    assert "FROM test_crypto_table" in called_sql
    assert "WHERE coin_name = 'bitcoin'" in called_sql
    assert "DENSE_RANK()" in called_sql
    assert "price_rank <= 3" in called_sql

    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["price"] == 42000


def test_get_volatility_empty_result(analyzer, mock_db):
    """Test if result is correct if database is empty"""

    mock_db.execute_query.return_value = []

    df = analyzer.get_volatility(
        column=ColumnsToAnalyzeEnum.price.value,
        lag_to_row=1,
        coin_name="unknown",
        currency="usd",
    )

    assert isinstance(df, pd.DataFrame)
    assert df.empty

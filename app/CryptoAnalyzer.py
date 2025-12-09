from typing import Literal
import pandas as pd
from DatabaseLoader import DatabaseLoader

COLUMNS_TYPE = Literal["price", "volume"]


class CryptoAnalyzer:
    """Class for executing MySQL queries to extract analyzed data from table and return it as DataFrame"""

    def __init__(self, db: DatabaseLoader, table_name: str):
        self.db = db
        self._table_name = table_name

    def get_spikes(
        self,
        up_to_rank: int,
        column: COLUMNS_TYPE,
        order: Literal["DESC", "ASC"],
        coin_name: str,
        currency: str,
        start_date_key: str,
        end_date_key: str,
    ) -> pd.DataFrame:
        """
        Get days where price or volume for each (coin, currency) was either the biggest or smallest

        :param up_to_rank: amount of days
        :param column: which column to rank
        :param order: in which column order to get data
        :param coin_name: coin name to retrieve data for
        :param currency: currency in which retrieve data in
        :param start_date_key: YYYYMMDD format string for defining starting date for getting spikes
        :param end_date_key: YYYYMMDD format string for defining ending date for getting spikes
        """

        SQL_WHERE_CLAUSE = f"WHERE coin_name = '{coin_name}' AND currency = '{currency}' AND date_key BETWEEN {start_date_key} AND {end_date_key}"

        SQL_QUERY = f"""
            WITH ranked_data AS (
                SELECT coin_name, date_key, currency, {column},
                DENSE_RANK() OVER (
                    PARTITION BY coin_name, currency
                    ORDER BY {column} {order}
                ) as {column}_rank
                FROM {self._table_name}
                {SQL_WHERE_CLAUSE}
            )
            SELECT * FROM ranked_data WHERE {column}_rank <= {up_to_rank};
        """

        data = self.db.execute_query(query=SQL_QUERY)
        return pd.DataFrame(data)

    def get_moving_average(
        self,
        column: COLUMNS_TYPE,
        preceding_days: int,
        following_days: int,
        coin_name: str,
        currency: str,
    ) -> pd.DataFrame:
        """
        Get moving average for price or volume for each (coin, currency)

        :param coin_name: coin name to retrieve data for
        :type coin_name: str
        :param currency: currency in which retrieve data in
        :type currency: str
        """

        SQL_WHERE_CLAUSE = (
            f"WHERE coin_name = '{coin_name}' AND currency = '{currency}'"
        )

        SQL_QUERY = f"""
            SELECT coin_name, currency, date_key, {column}, AVG({column}) OVER (
                PARTITION BY coin_name, currency
                ORDER BY date_key
                ROWS BETWEEN {preceding_days} PRECEDING AND {following_days} FOLLOWING
            ) AS moving_avg_{column} 
            FROM {self._table_name}
            {SQL_WHERE_CLAUSE};
        """

        data = self.db.execute_query(query=SQL_QUERY)
        return pd.DataFrame(data)

    def get_volatility(
        self, column: COLUMNS_TYPE, lag_to_row: int, coin_name: str, currency: str
    ) -> pd.DataFrame:
        """
        Get volatility by days for (coin, currency) pair

        :param column: columns to analyze
        :type column: price | volume
        :param lag_to_row: how many days to LAG back
        :type lag_to_row: int
        :param coin_name: coin name to retrieve data for
        :type coin_name: str
        :param currency: currency in which retrieve data in
        :type currency: str
        """

        SQL_WHERE_CLAUSE = (
            f"WHERE coin_name = '{coin_name}' AND currency = '{currency}'"
        )

        SQL_QUERY = f"""
            WITH LaggedData AS (
                SELECT LAG({column}, {lag_to_row}) OVER (
                    PARTITION BY coin_name, currency
                    ORDER BY date_key
                ) AS previous,
                coin_name,
                date_key, 
                currency,
                {column}
                FROM {self._table_name}
                {SQL_WHERE_CLAUSE}
            )
            SELECT 
                ROUND(({column} - previous)/previous*100, 5) AS {column}_growth,
                coin_name,
                date_key,
                currency
            FROM LaggedData
            WHERE previous IS NOT NULL;
        """

        data = self.db.execute_query(query=SQL_QUERY)
        return pd.DataFrame(data)

    def get_monthly_analysis(self, coin_name: str, currency: str) -> pd.DataFrame:
        """
        Get monthly analysis of price and volume for (coin, currency) pair

        :param coin_name: coin name to retrieve data for
        :type coin_name: str
        :param currency: currency in which retrieve data in
        :type currency: str
        """

        SQL_WHERE_CLAUSE = (
            f"WHERE coin_name = '{coin_name}' AND currency = '{currency}'"
        )

        SQL_QUERY = f"""
            WITH DataByMonth AS (
                SELECT
                    coin_name,
                    currency,
                    price,
                    volume,
                    DATE_FORMAT(STR_TO_DATE(date_key, '%Y%m%d'), '%Y-%m') AS year_month_key
                FROM crypto_data
                {SQL_WHERE_CLAUSE}
            )
            SELECT
                AVG(price) AS avg_price,
                AVG(volume) AS avg_volume,
                year_month_key,
                coin_name, 
                currency 
            FROM DataByMonth
            GROUP BY year_month_key, coin_name, currency;
        """
        data = self.db.execute_query(query=SQL_QUERY)
        return pd.DataFrame(data)

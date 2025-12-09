import asyncio
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
import pandas as pd
from CryptoExtracter import CryptoExtracter
from CryptoTransformer import CryptoTransformer
from CryptoVisualizer import CryptoVisualizer
from DatabaseLoader import DatabaseLoader
from CryptoAnalyzer import CryptoAnalyzer

load_dotenv()

DAYS_OF_HISTORY = 100
COINS = ["solana", "ethereum"]
CURRENCY = ["usd", "eur"]
TABLE_NAME = os.getenv("TABLE_NAME")


def get_coins_data(
    coins_list: list[str], currency_list: list[str]
) -> list[tuple[str, str]]:
    # get coins data by generating cortesion product of coin name and currency
    return [(coin, currency) for coin in coins_list for currency in currency_list]


async def main():
    # get coins data for extracting and transforming data correctly
    coins_data = get_coins_data(coins_list=COINS, currency_list=CURRENCY)
    end_point_timestamp = int(time.time())
    start_date = datetime.now() - timedelta(days=DAYS_OF_HISTORY)
    start_timestamp = int(start_date.timestamp())

    # extract data using API
    extracter = CryptoExtracter()
    crypto_data = await extracter.get_retrospective_data(
        starting_from_timestamp=start_timestamp,
        up_to_timestamp=end_point_timestamp,
        coins_data=coins_data,
    )

    # transform data to DataFrame
    transformer = CryptoTransformer()
    transformer.normalize_crypto_data(data=crypto_data, coins_data=coins_data)
    df_crypto = transformer.get_normalized_crypto()

    # initialize database
    db_loader = DatabaseLoader()

    # save data to database
    await asyncio.to_thread(
        db_loader.load_dataframe, df=df_crypto, table_name=TABLE_NAME
    )

    # analyse data
    analyzer = CryptoAnalyzer(db=db_loader, table_name=TABLE_NAME)

    # go through every (coin_name, currency) pair and save visualised data as images
    for coin, currency in coins_data:

        # visualize general information about price and volume
        CryptoVisualizer.plot_general_info(
            df=df_crypto, coin_name=coin, currency=currency
        )

        # visualize monthly statistics for price and volume
        df_monthly_data = analyzer.get_monthly_analysis(
            coin_name=coin, currency=currency
        )
        CryptoVisualizer.plot_monthly_analysis(df=df_monthly_data, column="avg_price")
        CryptoVisualizer.plot_monthly_analysis(df=df_monthly_data, column="avg_volume")

        # visualize monthly share of volume
        CryptoVisualizer.plot_monthly_volume_share(df=df_monthly_data, total_months=12)

        # visualize spikes
        start_date_key = "20251010"
        end_date_key = "20251025"
        df_spikes_data = analyzer.get_spikes(
            up_to_rank=5,
            order="DESC",
            column="price",
            coin_name=coin,
            currency=currency,
            start_date_key=start_date_key,
            end_date_key=end_date_key,
        )
        CryptoVisualizer.plot_spikes(
            df=df_spikes_data,
            column="price",
            start_date_key=start_date_key,
            end_date_key=end_date_key,
        )

        # visualize moving average
        preceding_days = 3
        following_days = 3
        total_day_span = preceding_days + following_days + 1
        df_moving_average_data = analyzer.get_moving_average(
            preceding_days=preceding_days,
            following_days=following_days,
            column="price",
            coin_name=coin,
            currency=currency,
        )
        CryptoVisualizer.plot_moving_average(
            df=df_moving_average_data, column="price", total_day_span=total_day_span
        )

        # visualize growth
        days_to_lag = 3
        df_volatility_data = analyzer.get_volatility(
            column="price", lag_to_row=days_to_lag, coin_name=coin, currency=currency
        )
        CryptoVisualizer.plot_volatility(
            df=df_volatility_data, column="price", days_to_lag=days_to_lag
        )


if __name__ == "__main__":
    asyncio.run(main())


# limit error handling
# no coin error handling

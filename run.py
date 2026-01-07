import asyncio
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

from app.CryptoExtracter import CryptoExtracter
from app.CryptoTransformer import CryptoTransformer
from app.CryptoVisualizer import CryptoVisualizer
from app.DatabaseLoader import DatabaseLoader
from app.CryptoAnalyzer import CryptoAnalyzer
from app.enums.ColumnsToVisualizeEnum import ColumnsToVisualizeEnum
from app.enums.OrderEnum import OrderEnum
from app.enums.ColumnsToAnalyzeEnum import ColumnsToAnalyzeEnum

load_dotenv()

TABLE_NAME = os.getenv("TABLE_NAME")


def get_coins_data(
    coins_list: list[str], currency_list: list[str]
) -> list[tuple[str, str]]:
    # get coins data by generating cortesion product of coin name and currency
    return [(coin, currency) for coin in coins_list for currency in currency_list]


async def main(days_of_history: int, coins: list[str], currency: list[str]):
    # get coins data for extracting and transforming data correctly
    coins_data = get_coins_data(coins_list=coins, currency_list=currency)
    end_point_timestamp = int(time.time())
    start_date = datetime.now() - timedelta(days=days_of_history)
    start_timestamp = int(start_date.timestamp())

    # extract data using API
    extracter = CryptoExtracter()
    crypto_data = await extracter.get_retrospective_data(
        starting_from_timestamp=start_timestamp,
        up_to_timestamp=end_point_timestamp,
        coins_data=coins_data,
    )

    # check if every requested dataset is empty
    if all(not d for d in crypto_data):
        print("No data to analyse")
        return

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

        # visualize spikes
        start_date_key = "20251110"
        end_date_key = "20251125"
        df_spikes_data = analyzer.get_spikes(
            up_to_rank=5,
            order=OrderEnum.descending.value,
            column=ColumnsToAnalyzeEnum.capitalization.value,
            coin_name=coin,
            currency=currency,
            start_date_key=start_date_key,
            end_date_key=end_date_key,
        )
        CryptoVisualizer.plot_spikes(
            df=df_spikes_data,
            column=ColumnsToVisualizeEnum.capitalization.value,
            start_date_key=start_date_key,
            end_date_key=end_date_key,
        )

        # visualize general information about price and volume
        CryptoVisualizer.plot_general_info(
            df=df_crypto, coin_name=coin, currency=currency
        )

        # visualize monthly statistics for price and volume
        df_monthly_data = analyzer.get_monthly_analysis(
            coin_name=coin, currency=currency
        )
        CryptoVisualizer.plot_monthly_analysis(
            df=df_monthly_data, column=ColumnsToVisualizeEnum.average_price.value
        )
        CryptoVisualizer.plot_monthly_analysis(
            df=df_monthly_data, column=ColumnsToVisualizeEnum.average_volume.value
        )
        CryptoVisualizer.plot_monthly_analysis(
            df=df_monthly_data,
            column=ColumnsToVisualizeEnum.average_capitalization.value,
        )

        # visualize monthly share of volume
        CryptoVisualizer.plot_monthly_volume_share(df=df_monthly_data, total_months=12)

        # visualize moving average
        preceding_days = 3
        following_days = 3
        total_day_span = preceding_days + following_days + 1
        df_moving_average_data = analyzer.get_moving_average(
            preceding_days=preceding_days,
            following_days=following_days,
            column=ColumnsToAnalyzeEnum.price.value,
            coin_name=coin,
            currency=currency,
        )
        CryptoVisualizer.plot_moving_average(
            df=df_moving_average_data,
            column=ColumnsToVisualizeEnum.price.value,
            total_day_span=total_day_span,
        )

        # visualize growth
        days_to_lag = 3
        df_volatility_data = analyzer.get_volatility(
            column=ColumnsToAnalyzeEnum.price.value,
            lag_to_row=days_to_lag,
            coin_name=coin,
            currency=currency,
        )
        CryptoVisualizer.plot_volatility(
            df=df_volatility_data,
            column=ColumnsToVisualizeEnum.price.value,
            days_to_lag=days_to_lag,
        )
        continue


if __name__ == "__main__":
    asyncio.run(
        main(
            days_of_history=100,
            coins=["bitcoin", "non_existing_coin"],
            currency=["usd", "non_existing_currency"],
        )
    )

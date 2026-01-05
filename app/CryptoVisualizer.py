from datetime import datetime
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib

from consts import OUTPUT_DIR

matplotlib.use("Agg")


class CryptoVisualizer:

    @staticmethod
    def plot_general_info(
        df: pd.DataFrame,
        coin_name: str,
        currency: str,
        title: str = "Price and Volume Dynamics",
    ):
        """
        Draw volume and price chart for (coin_name, currency) pair

        :param df: DataFrame containing daily crypto data.
        :type df: pd.DataFrame
        :param coin_name: The cryptocurrency to filter and plot.
        :type coin_name: str
        :param currency: The fiat/crypto currency to filter and plot against.
        :type currency: str
        :param title: The base title for the chart.
        :type title: str
        """

        # copy df not to modify the original DataFrame
        df = df.copy()

        if df.empty:
            print("DataFrame is empty. Unable to draw a general info chart.")
            return

        # filter data down to a single pair
        data = df[(df["coin_name"] == coin_name) & (df["currency"] == currency)].copy()

        if data.empty:
            print(f"No data found for pair {coin_name.upper()}/{currency.upper()}.")
            return

        # transform date_key column into datetime object and set as index
        data["date"] = pd.to_datetime(data["date_key"], format="%Y%m%d")
        data = data.set_index("date")

        fig, ax1 = plt.subplots(figsize=(14, 7))
        ax2 = ax1.twinx()  # Create a second Y-axis

        # settings and labels
        label_base = f"{coin_name.upper()} ({currency.upper()})"
        ax1.set_title(f"{title} - {label_base}", fontsize=16)
        ax1.set_xlabel("Date", fontsize=12)

        COLOR_PRICE = "tab:blue"
        COLOR_VOLUME = "tab:orange"

        # Y axis labels
        ax1.set_ylabel(f"Price ({currency.upper()})", color=COLOR_PRICE, fontsize=12)
        ax2.set_ylabel("Volume", color=COLOR_VOLUME, fontsize=12)

        # plot price
        (line1,) = ax1.plot(
            data.index,
            data["price"],
            label=f"Price {label_base}",
            color=COLOR_PRICE,
            alpha=0.9,
            linewidth=2,
            marker="o",
            markersize=3,
        )

        # plot volume
        line2 = ax2.bar(
            data.index,
            data["volume"],
            label=f"Volume {label_base}",
            color=COLOR_VOLUME,
            alpha=0.3,
            width=0.8,
        )

        # formatting and display settings
        ax1.tick_params(axis="y", labelcolor=COLOR_PRICE)
        ax2.tick_params(axis="y", labelcolor=COLOR_VOLUME)

        # format scientific number to comprehensive
        volume_formatter = mticker.ScalarFormatter(useOffset=False, useMathText=False)
        ax2.yaxis.set_major_formatter(volume_formatter)

        # date format
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45)

        # form legend
        lines = [line1, line2]
        labels = [f"Price {label_base}", f"Volume {label_base}"]
        ax1.legend(lines, labels, loc="upper left")

        plt.grid(True)
        plt.tight_layout()

        # save as image
        filename_base = f"{coin_name}_{currency}_daily_info"
        CryptoVisualizer.save_as_png(
            filename_base=filename_base, category_dir="general_info", fig=fig
        )

    @staticmethod
    def plot_monthly_analysis(
        df: pd.DataFrame,
        column: str = "avg_price",
    ):
        """
        Build a bar diagram for selected column

        :param df: DataFrame with monthly data (must contain year_month_key, coin_name, avg_price).
        :type df: pd.DataFrame
        :param column: columnt to visualize ('avg_price' or 'avg_volume' or 'avg_capitalization').
        :type column: str
        """
        if df.empty or column not in df.columns:
            print(
                f"DataFrame is empty or '{column}' is absent. Unable to draw a monthly analysis chart."
            )
            return

        # get coin name and currency
        coin = df["coin_name"].iloc[0].capitalize()
        currency_code = df["currency"].iloc[0].upper()

        # sort data by date key
        subset = df.sort_values(by="year_month_key")

        # configure labels
        title_map = {
            "avg_price": "Average Price",
            "avg_volume": "Average Volume",
            "avg_capitalization": "Average Capitalization",
        }
        y_label_map = {
            "avg_price": f"Price ({currency_code})",
            "avg_volume": "Volume",
            "avg_capitalization": "Capitalization",
        }

        fig, ax = plt.subplots(figsize=(10, 6))

        metric_title = title_map.get(column, column)

        # build bar diagram
        bars = ax.bar(
            subset["year_month_key"], subset[column], color="tab:orange", alpha=0.8
        )

        # set up axes and title
        ax.set_title(f"{metric_title} for {coin} ({currency_code})")
        ax.set_xlabel("Year-Month")
        ax.set_ylabel(y_label_map.get(column, column))
        ax.grid(axis="y", linestyle="--", alpha=0.6)
        ax.tick_params(axis="x", rotation=45)

        # add labels
        for bar in bars:
            yval = bar.get_height()
            text_val = f"{yval:,.2f}" if column == "avg_price" else f"{yval:,.0f}"
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                yval + (yval * 0.01),
                text_val,
                ha="center",
                va="bottom",
                fontsize=8,
            )

        plt.tight_layout()

        filename_base = f"{coin}_{currency_code}_monthly_{column}"
        CryptoVisualizer.save_as_png(
            filename_base=filename_base, category_dir="monthly", fig=fig
        )

    @staticmethod
    def plot_spikes(
        df: pd.DataFrame, column: str, start_date_key: str, end_date_key: str
    ):
        """
        Draw a graph for top N days in ASCENDING or DESCENGING order.

        :param df: DataFrame containing ranked spike data
        :param column: The column that was ranked
        """
        if df.empty:
            print(f"DataFrame for spikes is empty. Unable to draw a spikes graph")
            return

        # format starting/ending dates
        def format_date_key(date: str) -> str:
            return f"{date[:4]}-{date[4:6]}-{date[6:8]}"

        formatted_start_date = format_date_key(str(start_date_key))
        formatted_end_date = format_date_key(str(end_date_key))

        # get coin and currency
        coin = df["coin_name"].iloc[0].upper()
        currency = df["currency"].iloc[0].upper()

        # get order type
        order_type = "Highest" if df.iloc[0][f"{column}_rank"] == 1 else "Lowest"
        color = "darkgreen" if order_type == "Highest" else "darkred"

        # prepare data
        dates = pd.to_datetime(df["date_key"].astype(str), format="%Y%m%d").dt.strftime(
            "%Y-%m-%d"
        )
        values = df[column]

        # create plot
        fig, ax = plt.subplots(figsize=(12, 6))

        color = "darkgreen" if order_type == "Highest" else "darkred"
        bars = ax.bar(dates, values, color=color, alpha=0.7)

        # labels and title
        ax.set_title(
            f"{column.capitalize()} {order_type} Spikes - {coin}/{currency}, {formatted_start_date} - {formatted_end_date}",
            fontsize=14,
        )
        ax.set_xlabel("Date")
        ax.set_ylabel(f"{column.capitalize()} ({currency})")
        plt.xticks(rotation=35)
        ax.grid(axis="y", linestyle="--", alpha=0.5)

        # add values on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                height,
                f"{height:,.2f}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

        plt.tight_layout()

        # save chart
        filename_base = f"{coin}_{currency}_{order_type}_spikes_{column}_{formatted_start_date}--{formatted_end_date}"
        CryptoVisualizer.save_as_png(
            filename_base=filename_base, category_dir="spikes", fig=fig
        )

    @staticmethod
    def plot_moving_average(df: pd.DataFrame, column: str, total_day_span: int):
        """
        Draw daily price and moving average price on a graph

        :param df: DataFrame with moving average
        :param column: column used for calculating average
        :param total_day_span: total amount of days used to calculate moving average
        """
        if df.empty:
            print(
                f"DataFrame for moving average is empty. Unable to draw a moving average graph."
            )
            return

        # form data
        coin = df["coin_name"].iloc[0].upper()
        currency = df["currency"].iloc[0].upper()

        # sort by date
        df["date"] = pd.to_datetime(df["date_key"], format="%Y%m%d")
        df = df.sort_values(by="date").set_index("date")

        # define column names
        avg_col = f"moving_avg_{column}"
        metric_title = column.capitalize()

        fig, ax = plt.subplots(figsize=(14, 7))

        # plot 1, actual data (price or volume)
        ax.plot(
            df.index,
            df[column],
            label=f"Actual {metric_title}",
            color="gray",
            alpha=0.6,
            linewidth=1.5,
        )

        # plot 2, moving average
        ax.plot(
            df.index,
            df[avg_col],
            label="Moving Average",
            color="red",
            alpha=0.9,
            linewidth=2.5,
        )

        # configuration
        ax.set_title(
            f"{metric_title} and Moving Average({str(total_day_span)} days) for {coin}/{currency}",
            fontsize=14,
        )
        ax.set_xlabel("Date")
        ax.set_ylabel(f"{metric_title} ({currency})")

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45)

        ax.legend(loc="upper left")
        ax.grid(True, linestyle="--", alpha=0.6)

        # Save chart
        filename_base = (
            f"{coin}_{currency}_moving_avg_{column}_{str(total_day_span)}days"
        )
        CryptoVisualizer.save_as_png(
            filename_base=filename_base, category_dir="moving_average", fig=fig
        )

    @staticmethod
    def plot_volatility(df: pd.DataFrame, column: str, days_to_lag: int):
        """
        Draws a graph of the period-over-period percentage change for a specific metric.

        :param df: DataFrame containing percentage change data
        :param column: the column (price or volume) that was compared
        :param days_to_lag: days back to calculate volatility
        """
        if df.empty:
            print(
                f"DataFrame for volatility is empty. Unable to draw a volatility graph."
            )
            return

        # prepare data and context
        coin = df["coin_name"].iloc[0].upper()
        currency = df["currency"].iloc[0].upper()

        # sort by date
        df["date"] = pd.to_datetime(df["date_key"], format="%Y%m%d")
        df = df.sort_values(by="date").set_index("date")

        growth_col = f"{column}_growth"
        metric_title = column.capitalize()

        fig, ax = plt.subplots(figsize=(14, 7))

        # define colors (red and green)
        colors = ["green" if x >= 0 else "red" for x in df[growth_col]]

        # plot bars
        ax.bar(df.index, df[growth_col], color=colors, alpha=0.7)

        # add zero line for reference
        ax.axhline(0, color="black", linewidth=0.8)

        # configuration
        ax.set_title(
            f"Volatility by {str(days_to_lag)} day(s) ({metric_title} % Change) for {coin}/{currency}",
            fontsize=14,
        )
        ax.set_xlabel("Date")
        ax.set_ylabel("Percentage Change (%)")

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45)

        ax.grid(True, axis="y", linestyle="--", alpha=0.6)

        # save chart
        filename_base = f"{coin}_{currency}_{str(days_to_lag)}days_volatility_{column}"
        CryptoVisualizer.save_as_png(
            filename_base=filename_base, category_dir="volatility", fig=fig
        )

    @staticmethod
    def plot_monthly_volume_share(df: pd.DataFrame, total_months: int):
        """
        Build a pie chart for share of coin volume by month

        :param df: DataFrame with monthly aggregation (must contain 'year_month_key' and 'avg_volume')
        :total_months: amount of months to consider when making chart
        """

        def format_label(pct):
            """Format volume percentage to actual volume"""

            absolute_volume = (pct / 100.0) * float(total_volume)
            volume_formatted = f"{absolute_volume:,.0f}"
            return f"{pct:.1f}%\n({volume_formatted})"

        if df.empty or "avg_volume" not in df.columns:
            print(
                "DataFrame is empty or 'avg_volume' column is missing. Unable to draw a monthly volume share graph."
            )
            return

        # copy df for maximum 12 months
        subset = df[0:total_months].copy()

        # prepare data and context
        coin_name = df["coin_name"].iloc[0].upper()
        currency = df["currency"].iloc[0].upper()

        # calculate the total volume for the period
        total_volume = subset["avg_volume"].sum()
        if total_volume == 0:
            print("Total volume is zero, cannot plot share.")
            return

        # get display names
        coin_display = subset["coin_name"].iloc[0].capitalize()
        currency_code = subset["currency"].iloc[0].upper()

        fig, ax = plt.subplots(figsize=(10, 10))

        formatted_labels = [
            f"{ym[:4]}-{ym[4:6]}" for ym in subset["year_month_key"].astype(str)
        ]
        sizes = subset["avg_volume"]

        wedges, texts, autotexts = ax.pie(
            sizes,
            labels=formatted_labels,
            autopct=format_label,
            startangle=90,
            wedgeprops={
                "edgecolor": "black",
                "linewidth": 0.8,
            },
            pctdistance=0.65,
        )

        # title and appearance
        ax.axis("equal")
        ax.set_title(
            f"Monthly Volume Share for {coin_display} (in {currency_code})", fontsize=16
        )

        # adjust percentage text size and color
        for autotext in autotexts:
            autotext.set_color("white")
            autotext.set_fontsize(10)

        # saving the chart
        filename_base = f"{coin_name}_{currency}_monthly_volume_share"
        CryptoVisualizer.save_as_png(
            filename_base=filename_base, category_dir="volume_analysis", fig=fig
        )

    @staticmethod
    def save_as_png(filename_base: str, category_dir: str, fig):
        # get current date
        current_date = datetime.now()
        formatted_date = current_date.strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_base}_{formatted_date}.png".lower()

        target_directory = Path(OUTPUT_DIR) / category_dir
        target_directory.mkdir(parents=True, exist_ok=True)
        full_path = target_directory / filename

        plt.savefig(full_path.as_posix())
        plt.close(fig)

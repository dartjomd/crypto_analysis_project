import pandas as pd


class CryptoTransformer:

    def __init__(self):
        self._normalized_df: pd.DataFrame | None = None

    def get_normalized_crypto(self):
        if self._normalized_df is None or self._normalized_df.empty:
            return pd.DataFrame([])
        else:
            return self._normalized_df

    def normalize_crypto_data(
        self, data: list[dict[str, any]], coins_data: list[tuple[str, str]]
    ) -> pd.DataFrame:

        # define final DataFrame
        df_final = pd.DataFrame([])

        # check if every coin has its fetched data
        if len(coins_data) != len(data):
            print("Error. Not every coin has its data.")
            return df_final

        for i, coin_data in enumerate(data):
            df_prices = pd.DataFrame(
                coin_data["prices"], columns=["timestamp", "price"]
            )
            df_total_volumes = pd.DataFrame(
                coin_data["total_volumes"], columns=["timestamp", "total_volumes"]
            )

            df_data = df_prices.merge(
                on="timestamp", how="inner", right=df_total_volumes
            )

            # change timestamp to INT type in YYYYMMDD format
            df_data["date"] = pd.to_datetime(
                df_data["timestamp"], unit="ms", errors="coerce"
            ).dt.normalize()
            df_data["date_key"] = df_data["date"].dt.strftime(("%Y%m%d")).astype(int)

            # drop unnecesary columns
            df_data.drop(columns=["timestamp", "date"], inplace=True)

            # rename columns
            df_data.rename(
                columns={
                    "total_volumes": "volume",
                },
                inplace=True,
            )

            df_data["coin_name"] = coins_data[i][0]
            df_data["currency"] = coins_data[i][1]

            # append particular coin data to final result
            df_final = pd.concat([df_final, df_data])

        # round float values
        df_final = df_final.round(2)

        # change types
        df_final = df_final.astype({"coin_name": "category", "currency": "category"})

        # drop duplicates
        df_final = df_final.drop_duplicates()

        self._normalized_df = df_final

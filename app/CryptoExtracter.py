from app.BaseFetchClass import BaseFetchClass
from app.consts import BASE_URL


class CryptoExtracter(BaseFetchClass):
    """
    Class for extracting cryptocurrency data from CoinGecko.com
    """

    async def get_retrospective_data(
        self,
        starting_from_timestamp: int,
        up_to_timestamp: int,
        coins_data: list[tuple[str, str]],
    ) -> list[dict[str, any]]:
        """
        Fetch retrospective cryptocurrency data based on time and coin types.

        :param starting_from_timestamp: starting from what time get data
        :param up_to_timestamp: up to what time get data
        :param coins: list of coins to fetch
        :param currency: desired currency to output
        :return: fetched data
        """

        urls = CryptoExtracter.calculate_retrospective_url_params(
            starting_from=starting_from_timestamp,
            up_to=up_to_timestamp,
            coins_data=coins_data,
        )

        return await self.gather_data(urls)

    @staticmethod
    def calculate_retrospective_url_params(
        coins_data: list[tuple[str, str]], starting_from: int, up_to: int
    ) -> list[tuple[str, dict]]:
        """
        Calculate list with urls(baseurl, params)

        :param starting_from_timestamp: starting from what time get data
        :param up_to_timestamp: up to what time get data
        :param coins: list of coins to fetch
        :param currency: desired currency to output
        :return: Description
        """
        urls = []

        # generate urls for extracting data by cortesion product of coin name and currency
        for coin_name, currency in coins_data:
            url = f"{BASE_URL}/coins/{coin_name}/market_chart/range"
            params = {"vs_currency": currency, "from": starting_from, "to": up_to}
            urls.append((url, params))

        return urls

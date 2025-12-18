import asyncio
import aiohttp


class BaseFetchClass:
    """
    Base class for fetching data using aiohttp.
    """

    def __init__(self, max_concurrent: int = 3):
        # create semaphore for excessive requests handling
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def gather_data(self, urls: list[tuple[str, dict]]) -> list[dict[str, any]]:
        """
        Gathers all data from list of urls given as an argument.

        :param urls: list of tuple with full url in dictionary.
        :return: all fetched data.
        """

        result = []

        # open aiohttp connection
        async with aiohttp.ClientSession() as session:
            tasks = []

            # go through urls and get all aiohttp tasks
            for url, params in urls:
                task = asyncio.create_task(
                    self._fetch_data(session=session, base_url=url, params=params)
                )
                tasks.append(task)

            # prepare tasks for execution
            result = await asyncio.gather(*tasks)

            return result

    async def _fetch_data(
        self, session: aiohttp.ClientSession, base_url: str, params: dict
    ) -> dict[str, any]:
        """
        Fetches data from one URL given as an argument.

        :param session: aiohttp session instance
        :param base_url: base url
        :param params: dictionary with keys and values of url params
        :return: fetched result
        """

        async with self.semaphore:
            timeout = aiohttp.ClientTimeout(total=15)
            try:
                # get response
                async with session.get(
                    base_url, params=params, timeout=timeout
                ) as response:
                    # check if response is received
                    if response.status == 200:
                        res = await response.json()

                        # check if response contains errors
                        if isinstance(res, dict) and "error" in res:
                            print(f"API error for {response.url}: {res['error']}")
                            return {}

                        # return response if everything is correct
                        return res

                    # check if response has rate limit error
                    elif response.status == 429:
                        print(f"Rate limit (429) for {response.url}")
                        return {}
                    # check if response has other types of error
                    else:
                        print(
                            f"Error. Unable to fetch data from {response.url}. Error code: {response.status}."
                        )
                        return {}
            except aiohttp.ClientError as e:
                print(f"Critical aiohttp error. {e}")
                return {}
            except asyncio.TimeoutError as e:
                print(f"Timeout for {base_url}")
                return {}

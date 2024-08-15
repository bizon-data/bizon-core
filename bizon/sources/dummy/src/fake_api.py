# Function emulating an API call to a source endpoint
def fake_api_call(url: str, cursor: str = None) -> dict:
    if url == "https://api.dummy.com/v1/creatures":
        return fake_api_call_creatures(cursor)

    if url == "https://api.dummy.com/v1/plants":
        return fake_api_call_plants(cursor)

    raise NotImplementedError(f"URL {url} not implemented for Dummy")


def fake_api_call_creatures(cursor: str = None) -> dict:
    """Function to fake an API source endpoint"""
    # Here, let's imagine we made our API call to our Dummy Source
    # response = self.session.call(method="GET", url=self.url_entity)

    # Our API contains 3 pages, so we need 3 API calls to reach end of data

    # If no pagination is passed, we return first page of entitie
    if not cursor:
        return {
            "results": [{"id": 9898, "name": "bizon", "age": 26}, {"id": 88787, "name": "croco", "age": 23}],
            # The API provides a next cursor to use as query param
            # To retrieve data from next page
            "next": {"cursor": "vfvfvuhfefpeiduzhihxb"},
        }

    # If we reached the last page:
    if cursor == "final-cursor":
        return {
            "results": [{"id": 56565, "name": "poulpo", "age": 26}],
            # In this case the cursor could be empty for example
            "next": {"cursor": ""},
        }

    # Here we are pulling the second page, leading after to the last page
    return {
        "results": [{"id": 98, "name": "froggy", "age": 26}, {"id": 3333, "name": "turtle", "age": 23}],
        # In this case the cursor lead to the last page with its value: "final-cursor"
        "next": {"cursor": "final-cursor"},
    }


def fake_api_call_plants(cursor: str = None) -> dict:
    """Function to fake an API source endpoint"""
    # Here, let's imagine we made our API call to our Dummy Source
    # response = self.session.call(method="GET", url=self.url_entity)

    # Our API contains 3 pages, so we need 3 API calls to reach end of data

    # If no pagination is passed, we return first page of entitie
    if not cursor:
        return {
            "results": [{"id": 9898, "name": "tree", "age": 26}, {"id": 88787, "name": "flower", "age": 23}],
            # The API provides a next cursor to use as query param
            # To retrieve data from next page
            "next": {"cursor": "vfvfvuhfefpeiduzhihxb"},
        }

    # If we reached the last page:
    if cursor == "final-cursor":
        return {
            "results": [{"id": 56565, "name": "rose", "age": 26}],
            # In this case the cursor could be empty for example
            "next": {"cursor": ""},
        }

    # Here we are pulling the second page, leading after to the last page
    return {
        "results": [{"id": 98, "name": "cactus", "age": 26}, {"id": 3333, "name": "bamboo", "age": 23}],
        # In this case the cursor lead to the last page with its value: "final-cursor"
        "next": {"cursor": "final-cursor"},
    }

import asyncio
import orjson
import polars as pl
import logging
from pathlib import Path
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions


# https://www.reddit.com/r/news.json?limit=100
async def grab_reddit_json(subreddit: str, amount_of_posts: int) -> str:
    """
    Opens up a chromium driver in the sub-reddit source page and returns it is json text.

    Args:
        subreddit: Base urls for the subreddit.
        amount_of_posts: Number of posts to fetch per subreddit.

    Returns:
        A loadable json string.
    """
    options = ChromiumOptions()
    options.binary_location = "/usr/bin/chromium"
    options.add_argument("--disable-blink-features=AutomationControlled")

    async with Chrome(options=options) as browser:
        tab = await browser.start()
        await tab.go_to(f"{subreddit}.json?limit={amount_of_posts}")

        json_holder = await tab.find(tag_name="pre")
        text = await json_holder.text

        if not text or not text.strip():
            raise ValueError("Reddit JSON response is empty")

        logging.debug("Ran the chromium driver and grabbed json data")
        return text


async def fetch_all_reddit_json(subreddits: list[str], limit: int) -> list[dict]:
    """
    Fetch Reddit JSON listings concurrently.

    Args:
        subreddits: List of subreddit base URLs.
        limit: Number of posts to fetch per subreddit.

    Returns:
        A list of dicts that were previously jsons.
    """
    results = await asyncio.gather(
        *[grab_reddit_json(url, limit) for url in subreddits]
    )
    return [orjson.loads(r) for r in results]


def extract_meme_data_reddit(loaded_jsons: list[dict]) -> pl.DataFrame:
    """
    Intakes a list of dicts, and extract only the needed data.

    Args:
        loaded_jsons: The reddit jsons that were loaded

    Returns:
        A dataframe object
    """
    posts = []

    for loaded_json in loaded_jsons:
        children = loaded_json.get("data", {}).get("children", [])
        if not children:
            continue

        for child in children:
            data = child["data"]
            posts.append(
                {
                    "title": data.get("title"),
                    "score": data.get("score"),
                    "upvotes": data.get("ups"),
                    "num_comments": data.get("num_comments"),
                    # Remember create downvote call from this godamm fuzzy beards
                    "upvote_ratio": data.get("upvote_ratio"),
                    "is_created_from_ads_ui": data.get("is_created_from_ads_ui"),
                    "total_awards_received": data.get("total_awards_received"),
                    "num_reports": data.get("num_reports"),
                    "image_url": data.get("url_overridden_by_dest"),
                }
            )

    return pl.DataFrame(posts)


def append_to_parquet(df: pl.DataFrame, path: str):
    """
    Append new meme data to a Parquet dataset.

    If the dataset already exists, new rows are concatenated and
    duplicates are removed based on title and image URL.

    Args:
        df: New meme data.
        path: Path to the Parquet file.
    """
    path = Path(path)

    if path.exists():
        df = pl.read_parquet(path).vstack(df).unique(subset=["title", "image_url"])

    df.write_parquet(path)


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        filename="reddit_scraper.log",
        filemode="w",
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logging.getLogger("pydoll").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)


if __name__ == "__main__":
    setup_logging()

    SUBREDDITS = [
        "https://www.reddit.com/r/dankmemes/",
        "https://www.reddit.com/r/memes/",
        "https://www.reddit.com/r/Memes_Of_The_Dank/",
    ]

    loaded_jsons = asyncio.run(fetch_all_reddit_json(SUBREDDITS, limit=1))
    df = extract_meme_data_reddit(loaded_jsons)
    append_to_parquet(df, "data/base.parquet")

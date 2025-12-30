import polars as pl
import logging
import requests
from pathlib import Path
from datetime import datetime


def fetch_all_reddit_json(subreddits: list[str], limit: int) -> list[dict]:
    """
    Fetch Reddit JSON listings concurrently.

    Args:
        subreddits: List of subreddit base URLs.
        limit: Number of posts to fetch per subreddit.

    Returns:
        A list of dicts that were previously jsons.
    """

    # Headers to ofuscate identification
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    results = [
        requests.get(f"{url}/.json?limit={limit}", headers=headers).json()
        for url in subreddits
    ]

    return results


def extract_meme_data_reddit(loaded_jsons: list[dict]) -> pl.DataFrame:
    """
    Intakes a list of dicts and extracts only the needed meme data.

    Args:
        loaded_jsons: Parsed Reddit JSON responses.

    Returns:
        Polars DataFrame containing normalized meme metadata.
    """
    posts: list[dict] = []

    for loaded_json in loaded_jsons:
        children = loaded_json.get("data", {}).get("children", [])
        if not children:
            logger.warning("Empty Reddit listing encountered")
            continue

        for child in children:
            data = child["data"]
            posts.append(
                {
                    "created_utc": data.get("created_utc"),
                    "title": data.get("title"),
                    "score": data.get("score"),
                    "upvotes": data.get("ups"),
                    "num_comments": data.get("num_comments"),
                    "upvote_ratio": data.get("upvote_ratio"),
                    "is_created_from_ads_ui": data.get("is_created_from_ads_ui"),
                    "total_awards_received": data.get("total_awards_received"),
                    "num_reports": data.get("num_reports"),
                    "image_url": data.get("url_overridden_by_dest"),
                    "is_video": data.get("is_video"),
                    "sub_reddit": data.get("subreddit")
                }
            )

    df = (
        pl.DataFrame(posts)
        .with_columns(pl.from_epoch("created_utc", time_unit="s").alias("created_at"))
        .with_columns(
            [
                pl.col("created_utc").cast(pl.Int64),
                pl.col("score").cast(pl.Int64),
                pl.col("upvotes").cast(pl.Int64),
                pl.col("num_comments").cast(pl.Int64),
                pl.col("upvote_ratio").cast(pl.Float64),
                pl.col("total_awards_received").cast(pl.Int64),
            ]
        )
    )

    logging.info(f"Extracted %d memes", df.height)
    logging.debug("Sample data:\n%s", df.head())

    return df


def setup_logging(time: str):
    logging.basicConfig(
        level=logging.DEBUG,
        filename=f"logs/scraper_run_{formatted}.log",
        filemode="w",
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logging.getLogger("pydoll").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)


if __name__ == "__main__":
    now = datetime.now()
    formatted = now.strftime("%Y_%m_%d%_H:%M:%S.%f")[:-3]
    setup_logging(time=formatted)
    SUBREDDITS = [
        "https://www.reddit.com/r/dankmemes/",
        "https://www.reddit.com/r/memes/",
        "https://www.reddit.com/r/Memes_Of_The_Dank/",
    ]
    # Extract
    loaded_jsons = fetch_all_reddit_json(SUBREDDITS, limit=200)
    # Slight Transform
    df = extract_meme_data_reddit(loaded_jsons)
    # Dump
    df.write_parquet(f"data/reddit_data_{formatted}.parquet")

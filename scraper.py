import polars as pl
import logging
import time
import easyocr
import requests
import torch
from pathlib import Path
from datetime import datetime


# https://roundproxies.com/blog/reddit/
def get_reddit_jsons(
    subreddits: list[str], sort="hot", pages=5, limit=100, timeframe="all"
) -> list[dict]:
    """
    Fetch Reddit JSON listings

    Args:
        subreddits: List of subreddit base URLs.
        limit: Number of posts to fetch per subreddit.Caps at 100
        pages: The amount of pages to scrap. If at 100, 5 pages equal 500 posts
        sort: The sort type for posts

    Returns:
        A list of dicts.
    """

    # Headers to ofuscate identification
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    params = {"limit": limit, "t": timeframe}

    all_posts = []
    after = None
    for subreddit in subreddits:
        for page in range(pages):
            if after:
                logging.debug("Grabbing new page data")
                params["after"] = after

            url = f"{subreddit}/{sort}.json"
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                posts = data["data"]["children"]
                all_posts.extend(posts)

                after = data["data"].get("after")
                if not after:
                    break

                time.sleep(1.0)

    logging.debug(f"Sample json:{all_posts[0]}")
    return all_posts


def extract_meme_data_reddit(all_posts: list[dict]) -> pl.DataFrame:
    """
    Intakes a list of dicts and extracts only the needed meme data.

    Args:
        loaded_jsons: Parsed Reddit JSON responses.

    Returns:
        Polars DataFrame containing normalized meme metadata.
    """
    infos: list[dict] = []

    for post in all_posts:
        data = post["data"]
        infos.append(
            {
                "created_utc": data.get("created_utc"),
                "title": data.get("title"),
                "upvotes": data.get("ups"),
                "num_comments": data.get("num_comments"),
                "upvote_ratio": data.get("upvote_ratio"),
                "is_created_from_ads_ui": data.get("is_created_from_ads_ui"),
                "total_awards_received": data.get("total_awards_received"),
                "num_reports": data.get("num_reports"),
                "image_url": data.get("url_overridden_by_dest"),
                "is_video": data.get("is_video"),
                "sub_reddit": data.get("subreddit"),
            }
        )

    df = (
        pl.DataFrame(infos)
        .with_columns(pl.from_epoch("created_utc", time_unit="s").alias("created_at"))
        .with_columns(
            [
                pl.col("created_utc").cast(pl.Int64),
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


def transform_texts_from_images(df: pl.DataFrame) -> pl.DataFrame:
    """
    Intakes the df gets it is image_urls and extracts the text if no text is available append none

    Args:
        loaded_jsons: Parsed Reddit JSON responses.

    Returns:
        Polars DataFrame containing normalized meme metadata.
    """
    meme_text = []

    for url in df["image_url"]:
        try:
            r = requests.get(url, timeout=5)
            r.raise_for_status()

            img = cv2.imdecode(np.frombuffer(r.content, np.uint8), cv2.IMREAD_COLOR)

            if img is None:
                meme_text.append(None)
                continue

            result = reader.readtext(img, detail=0, paragraph=True)
            logg
            meme_text.append(result)

        except Exception:
            meme_text.append(None)

    return df.with_columns(pl.Series("meme_text", meme_text))


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
    # Logging setup
    now = datetime.now()
    formatted = now.strftime("%Y_%m_%d%_H:%M:%S.%f")[:-3]
    setup_logging(time=formatted)

    SUBREDDITS = [
        "https://www.reddit.com/r/dankmemes/",
        "https://www.reddit.com/r/memes/",
        "https://www.reddit.com/r/Memes_Of_The_Dank/",
    ]
    # Extract
    all_posts = get_reddit_jsons(SUBREDDITS, pages=1)
    # Transform to dataframe
    df = extract_meme_data_reddit(all_posts)
    # Dump
    df.write_parquet(f"data/reddit_data_{formatted}.parquet")

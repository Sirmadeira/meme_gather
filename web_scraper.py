import asyncio
import orjson
import polars as pl
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions
from logging import basicConfig,info,DEBUG,debug,getLogger,WARNING


# https://www.reddit.com/r/news.json?limit=100
async def grab_reddit_json(amount_of_posts:int = 1):
    options = ChromiumOptions()
    options.binary_location = "/usr/bin/chromium"
    options.add_argument("--disable-blink-features=AutomationControlled")

    async with Chrome(options=options) as browser:
        tab = await browser.start()
        await tab.go_to(f"https://www.reddit.com/r/dankmemes/.json?limit={amount_of_posts}")

        json_holder = await tab.find(tag_name="pre")
        text = await json_holder.text

        if not text or not text.strip():
            raise ValueError("Reddit JSON response is empty")

        debug("Ran the chromium driver and grabbed json data")
        return text


def extract_meme_data_reddit(loaded_json: dict) -> list[dict]:
    # Amount of posts queried
    children = loaded_json["data"]["children"]

    if len(children) == 0:
        raise RuntimeError("No posts returned from API")


    posts = []
    for child in children:
        data = child["data"]
        meme = {
            "title":data.get("title"),
            "score":data.get("score"),
            "upvotes":data.get("ups"),
            "num_comments":data.get("num_comments"),
            # Remember create downvote call from this godamm fuzzy beards
            "upvote_ratio":data.get("upvote_ratio"),
            "is_created_from_ads_ui":data.get("is_created_from_ads_ui"),
            "total_awards_received":data.get("total_awards_received"),
            "num_reports": data.get("num_reports"),
            "image_url": data.get("url_overridden_by_dest")
        }
        debug(meme)
        posts.append(meme)

    return posts








if __name__ == "__main__":
    # Logging configs
    basicConfig(
        level=DEBUG,
        filename="z.log",
        filemode="w",
        format="%(asctime)s %(levelname)s  %(name)s: %(message)s",
    )
    getLogger("pydoll").setLevel(WARNING)
    getLogger("websockets.client").setLevel(WARNING)

    # Grabs json
    raw_json = asyncio.run(grab_reddit_json(1))
    # Loads it fast
    loaded_json = orjson.loads(raw_json)
    # Cleans it
    extract_meme_data_reddit(loaded_json=loaded_json)

        


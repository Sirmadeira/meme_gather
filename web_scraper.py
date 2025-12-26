import asyncio
import orjson
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions

# https://www.reddit.com/r/news.json?limit=100
async def main():
    options = ChromiumOptions()
    options.binary_location = "/usr/bin/chromium"
    options.add_argument('--disable-blink-features=AutomationControlled')

    async with Chrome(options=options) as browser:
        tab = await browser.start()
        await tab.go_to("https://www.reddit.com/r/news.json?limit=1")
        json_holder = await tab.find(tag_name="pre")
        print(await json_holder.text)
        print(orjson.loads(await json_holder.text))
        

if __name__ == "__main__":
    raw_json = asyncio.run(main())



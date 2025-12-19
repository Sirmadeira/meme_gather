import asyncio
from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions

async def main():
    options = ChromiumOptions()
    options.binary_location = "/usr/bin/chromium"
    options.add_argument('--disable-blink-features=AutomationControlled')

    async with Chrome(options=options) as browser:
        tab = await browser.start()
        await tab.go_to('https://www.reddit.com/r/dankmemes/best/')
        await tab.go_to('https://www.reddit.com/r/dankmemes/best/')
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())

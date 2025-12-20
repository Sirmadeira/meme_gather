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
        element = await tab.find(class_name="post-background-image-filter")
        image = element.get_attribute("src")        
        print(image)

        await tab.execute_script("window.scrollBy(0, 1000)")

        await asyncio.sleep(1000000)



        browser.close()


if __name__ == "__main__":
    asyncio.run(main())


# absolute top-1/2 -translate-y-1/2 left-0 w-full h-full opacity-30 object-cover scale-[1.2] post-background-image-filter
# Class for images
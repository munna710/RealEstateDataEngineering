import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
SBR_WS_CDP = 'wss://brd-customer-hl_ac47e1cf-zone-real_estate_browser:v563l1hogsq6@brd.superproxy.io:9222'
BASE_URL = "https://zoopla.co.uk"
LOCATION = "London"


def extract_pictures(pictures_section):
    picture_sources = []
    for picture in pictures_section.find_all('picture'):
        for source in picture.find_all('source'):
            source_type = source.get("type","").split("/")[1]
            pic_url = source.get("srcset","").split(",")[0].split(" ")[0]

            if source_type == "webp" and '1024' in pic_url:
                picture_sources.append(pic_url)

    return picture_sources

async def run(pw):
    print('Connecting to Scraping Browser...')
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    try:
        page = await browser.new_page()
        print(f'Connected! Navigating to {BASE_URL}')
        await page.goto(BASE_URL)
        await page.fill('input[name="autosuggest-input"]', LOCATION)
        await page.keyboard.press('Enter')
        print("waiting for search results...")
        # await page.wait_for_load_state("Load")
        await page.wait_for_load_state("networkidle")
        content = await page.inner_html('div[data-testid="regular-listings"]')
        
        soup = BeautifulSoup(content, 'html.parser')
        for idx, div in enumerate(soup.find_all('div',class_ = "dkr2t82")):
            link = div.find('a')['href']
            data = {
                "address" : div.find('address').text,
                "title" : div.find("h2").text,
                "link" : BASE_URL + link
            }
            print("Navigating to listing page...",link)
            await page.goto(data['link'])
            await page.wait_for_load_state("networkidle")
            content = await page.inner_html('div[data-testid="listing-details-page"]')
            soup = BeautifulSoup(content, 'html.parser')
            pictures_section = soup.find("section",{"aria-labelledby":"listing-gallery-heading"})
            pictures = extract_pictures(pictures_section)
            data["pictures"] = pictures
            print(data)
            break

        
        
        print('Navigated! Scraping page content...')
        
    finally:
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)


if __name__ == '__main__':
    asyncio.run(main())

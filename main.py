import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from openai import OpenAI
from kafka import KafkaProducer


SBR_WS_CDP = 'wss://brd-customer-hl_ac47e1cf-zone-real_estate_browser:mnov563l1hogsq6@brd.superproxy.io:9222'
BASE_URL = "https://zoopla.co.uk"
LOCATION = "London"

client = OpenAI(api_key="***************************")

def extract_property_details(input_command):
    print("extracting property details...")
     
    command =  """
                extract information about the apartment into json.
                {input_command}
                this is the final json structure expected:
                {{
                    "price" : "",
                    "address" : "",
                    "bedrooms" : "",
                    "bathrooms" : "",
                    "receptions" : "",
                    "EPC_Rating" : "",
                    "tenure": "",
                    "time_remaining_on_lease" : "",
                    "sevice_charge" : "",
                    "council_tax_band" : "",
                    "ground_rent" : ""

                }}
    """.format(input_command=input_command)
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": command}
        ]
    )
    res = response.choices[0].message.content
    json_data = json.loads(res)
    return json_data


def extract_pictures(pictures_section):
    picture_sources = []
    for picture in pictures_section.find_all('picture'):
        for source in picture.find_all('source'):
            source_type = source.get("type","").split("/")[1]
            pic_url = source.get("srcset","").split(",")[0].split(" ")[0]

            if source_type == "webp" and '1024' in pic_url:
                picture_sources.append(pic_url)

    return picture_sources

def extract_floor_plan(soup):
    print("Extracting floor plan")
    plan = {}
    floor_plan = soup.find('div',{"data-testid": "floorplan-thumbnail-0"})
    if floor_plan:
        floor_plan_src = floor_plan.find("picture").find("source")["srcset"]
        plan["floor_plan"] = floor_plan_src.split(' ')[0]
    return plan
         


async def run(pw,producer):
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
        content = await page.inner_html('div[data-testid="regular-listings"]',timeout=60000)
        
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

            property_details = soup.select_one('div[class="_14bi3x33z _14bi3x32f"]')
            property_details = extract_property_details(property_details)
            
            floor_plan = extract_floor_plan(soup)
            data.update(floor_plan)
            data.update(property_details)
            print(data)
            break
            
            print("sending data to kafka...")
            producer.send('properties', value.json.dumps(data).encode('utf-8'))
             

            break

        
        
        print('Navigated! Scraping page content...')
        
    finally:
        await browser.close()


async def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    async with async_playwright() as playwright:
        await run(playwright)


if __name__ == '__main__':
    asyncio.run(main())

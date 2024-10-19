import json
import asyncio
import aiohttp
import pandas as pd

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def request_data():
    federal_debt_url = r'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/schedules_fed_debt?'
    params = '&page[number]={{ page_number }}&page[size]=100'
    url = federal_debt_url + params
    async with aiohttp.ClientSession() as session:
        # get total pages
        total_pages = await fetch(session, url.replace('{{ page_number }}', '1'))
        total_pages = json.loads(total_pages).get('meta').get('total-pages')

        tasks = []
        for i in range(1, total_pages + 1):
            tasks.append(fetch(session, url.replace('{{ page_number }}', str(i))))

        responses = await asyncio.gather(*tasks)

    return [json.loads(response).get('data') for response in responses]


if __name__ == '__main__':
    vals = asyncio.run(request_data())
    flattened_data = [item for sublist in vals for item in sublist]
    df = pd.DataFrame(flattened_data)
    print('pause')

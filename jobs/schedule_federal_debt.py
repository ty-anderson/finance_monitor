import json
import requests
import pandas as pd


federal_debt_url = r'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/schedules_fed_debt?'
params = '&page[number]={{ page }}&page[size]=100'
url = federal_debt_url + params

result = requests.get(url.replace('{{ page }}', '1'))
json_result = json.loads(result.text)
total_pages = json_result.get('meta').get('total-pages')

main_df = pd.DataFrame()
for i in range(1, total_pages + 1):
    print(f'Page {i} of {total_pages}')
    result = requests.get(url.replace('{{ page }}', str(i)))
    json_result = json.loads(result.text)
    main_df = pd.concat([main_df, pd.DataFrame(json_result.get('data'))])
    # df = pd.DataFrame(json_result.get('data'))

print('pause')

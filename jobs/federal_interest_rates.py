import json
import requests
import pandas as pd


interest_rate_url = r'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates'
result = requests.get(interest_rate_url)
json_result = json.loads(result.text)
df = pd.DataFrame(json_result.get('data'))

print('pause')

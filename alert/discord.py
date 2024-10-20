import os
import requests
from dotenv import load_dotenv
load_dotenv()


def webhook_finance_monkey(msg: str):
    data = {"content": msg}
    response = requests.post(os.getenv('DISCORD_WEBHOOK_FINANCE_MONKEY'), json=data)
    print('Message sent')
    print(f'Response: {response.status_code}')


if __name__ == '__main__':
    webhook_finance_monkey(msg='Lola is the best wife in the world')

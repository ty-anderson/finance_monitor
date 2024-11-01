import os
import json
from typing import IO, Optional
import requests


def webhook_finance_monkey(msg: str,
                           file: Optional[IO[bytes] | tuple | None] = None):
    """
    Send message via webhook to Discord Finance Monkey.

    :param msg: String message that should be sent.
    :param file: Optional, file-like object that handles bytes data.
    :return:
    """
    data = {"content": msg}
    if file is not None:
        files = {"file": file}
        response = requests.post(
            os.getenv('DISCORD_WEBHOOK_FINANCE_MONKEY'),
            data={"payload_json": json.dumps(data)},
            files=files
        )
    else:
        response = requests.post(os.getenv('DISCORD_WEBHOOK_FINANCE_MONKEY'), json=data)

    return response


if __name__ == '__main__':
    webhook_finance_monkey(msg='This was sent')

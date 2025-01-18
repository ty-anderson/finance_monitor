"""
Discord Alerts
--------------

This uses Webhooks from Discord, to send alerts.
To create a webhook in Discord:

1. Go to the channel that you want the messages
2. Click on that channels setting.
3. Select "Integrations"
4. Select "Create Webhook"

"""
import json
from typing import IO, Optional
import requests
from sqlalchemy import text
from db.model import discord_channel_tbl, engine


def send_alert(msg: str,
               channel: str,
               file: Optional[IO[bytes] | tuple | None] = None):
    """
    Send message via webhook to Discord Finance Monkey.

    :param msg: String message that should be sent.
    :param channel: String to Discord webhook URL
    :param file: Optional, file-like object that handles bytes data.
    :return:
    """
    webhook_url = get_channel(channel)
    if webhook_url is None:
        print('Error finding channel!')
        return

    data = {"content": msg}
    if file is not None:
        files = {"file": file}
        response = requests.post(
            webhook_url,
            data={"payload_json": json.dumps(data)},
            files=files
        )
    else:
        response = requests.post(webhook_url, json=data)

    return response


def get_channel(value):
    with engine.connect() as conn:
        select_stmt = f"""
        SELECT 
        url
        FROM {discord_channel_tbl.name}
        WHERE channel_name ILIKE '%{value}%'
        LIMIT 1
        """
        discord_url = conn.execute(text(select_stmt)).fetchone()
        return discord_url[0]


if __name__ == '__main__':
    send_alert(msg='This was sent', channel='finance monkey')


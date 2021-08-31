# -*- coding: utf-8 -*-
# @Time    : 2021-06-08 22:29
# @Author  : Ashao

import json
import time
import hmac
import base64
import hashlib
import requests
import urllib.parse
from loguru import logger


def _msg(text):
    json_text = {
        "msgtype": "text",
        "at": {
            "atMobiles": [
                "11111"
            ],
            "isAtAll": False
        },
        "text": {
            "content": text
        }
    }
    return json_text


def ding_talk(text, token, secret):
    timestamp = str(round(time.time() * 1000))
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    logger.info(text)
    headers = {'Content-Type': 'application/json;charset=utf-8'}
    api_url = f"https://oapi.dingtalk.com/robot/send?access_token={token}&timestamp={timestamp}&sign={sign}"
    json_text = _msg(text)
    requests.post(api_url, json.dumps(json_text), headers=headers)


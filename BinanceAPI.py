# -*- coding: utf-8 -*-
# @Time    : 2021-06-07 23:06
# @Author  : Ashao

import hmac
import time
import hashlib
import requests
from loguru import logger
from dingding import ding_talk
from authorization import recv_window, api_secret, api_key, b_token, b_secret


def dingding_warn(text):
    ding_talk(text, b_token, b_secret)


try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode


def _get_no_sign(path, market, params=None):
    if params is None:
        params = dict()
    query = urlencode(params)
    url = "%s?%s" % (path, query)
    res = requests.get(url, timeout=180, verify=True).json()
    if isinstance(res, dict):
        if 'code' in res:
            error_info = f"公共方法报警：币种{market},请求异常.\nGET url:{url}\n错误原因{str(res)}"
            dingding_warn(error_info)

    return res


def _format(price):
    return "{:.8f}".format(price)


def _order(market, quantity, side, price=None, order_id=None):
    """
    :param market:币种类型。如：BTCUSDT、ETHUSDT
    :param quantity: 购买量
    :param side: 订单方向，买还是卖
    :param price: 价格
    :return:
    """
    params = {}

    if price is not None:
        params["type"] = "LIMIT"
        params["price"] = _format(price)
        params["timeInForce"] = "GTC"
    else:
        params["type"] = "MARKET"

    if order_id:
        params["newClientOrderId"] = order_id
    params["symbol"] = market
    params["side"] = side
    params["quantity"] = '%.8f' % quantity

    return params


class BinanceAPI(object):
    SAPI = "https://api.binance.com/sapi/v1"
    BASE_URL_V3 = "https://api.binance.com/api/v3"

    # BASE_URL = "https://api.binance.com/api/v1"
    # FUTURE_URL = "https://fapi.binance.com"
    # PUBLIC_URL = "https://www.binance.com/exchange/public/product"

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    def ping(self):
        path = "%s/ping" % self.BASE_URL_V3
        return requests.get(path, timeout=180, verify=True).json()

    # 获取交易规则和交易对信息, GET /api/v3/exchangeInfo
    def exchange_info(self):
        path = "%s/exchangeInfo" % self.BASE_URL_V3
        return requests.get(path, timeout=180, verify=True).json()

    # 获取最新价格
    def get_ticker_price(self, market):
        path = "%s/ticker/price" % self.BASE_URL_V3
        params = {"symbol": market}
        res = _get_no_sign(path, market, params)
        time.sleep(2)
        return float(res['price'])

    # 24hr 价格变动情况
    def get_ticker_24hour(self, market):
        path = "%s/ticker/24hr" % self.BASE_URL_V3
        params = {"symbol": market}
        res = _get_no_sign(path, market, params)
        return res

    def get_klines(self, market, interval, limit, start_time=None, end_time=None):
        path = "%s/klines" % self.BASE_URL_V3
        if start_time is None:
            params = {"symbol": market, "interval": interval, "limit": limit}
        else:
            params = {"symbol": market, "limit": limit, "interval": interval, "startTime": start_time,
                      "endTime": end_time}
        return _get_no_sign(path, market, params)

    def buy_limit(self, market, quantity, rate, order_id=None):
        path = "%s/order" % self.BASE_URL_V3
        params = _order(market, quantity, "BUY", rate, order_id)
        return self._post(path, market, params)

    def sell_limit(self, market, quantity, rate, order_id=None):
        path = "%s/order" % self.BASE_URL_V3
        params = _order(market, quantity, "SELL", rate, order_id)
        return self._post(path, market, params)

    def buy_market(self, market, quantity, order_id=None):
        path = "%s/order" % self.BASE_URL_V3
        params = _order(market, quantity, "BUY", order_id=order_id)
        return self._post(path, market, params)

    def sell_market(self, market, quantity, order_id=None):
        path = "%s/order" % self.BASE_URL_V3
        params = _order(market, quantity, "SELL", order_id=order_id)
        return self._post(path, market, params)

    def delete_order(self, symbol, order_id):
        """撤销订单 (TRADE)"""
        path = "%s/order" % self.BASE_URL_V3
        params = {"symbol": symbol, "origClientOrderId": order_id}
        time.sleep(1)
        return self._delete(path, symbol, params)

    def delete_open_orders(self, symbol):
        """撤销单一交易对的所有挂单 (TRADE)"""
        path = "%s/openOrders" % self.BASE_URL_V3
        params = {"symbol": symbol}
        time.sleep(1)
        return self._delete(path, symbol, params)

    # 现货，账户信息，GET /api/v3/account
    def get_spot_userdata_account(self):
        stamp_now = int(round(time.time() * 1000))
        path = "%s/account" % self.BASE_URL_V3
        params = {"timestamp": stamp_now, "recvWindow": recv_window}
        res = self._get(path, params=params)
        return res

    # 资金账户
    def get_funding_asset(self, asset=None):
        stamp_now = int(round(time.time() * 1000))
        path = "%s/v1/asset/get-funding-asset" % self.SAPI
        params = {"timestamp": stamp_now}
        if asset:
            params.update({"asset": asset})
        res = self._post(path, params=params)
        return res

    # 获取所有币信息
    def get_capital_config(self):
        stamp_now = int(round(time.time() * 1000))
        path = "%s/capital/config/getall" % self.SAPI
        params = {"timestamp": stamp_now}
        res = self._get(path, params=params)
        return res

    # 交易手续费率查询
    def get_asset_trade_fee(self):
        stamp_now = int(round(time.time() * 1000))
        path = "%s/asset/tradeFee" % self.SAPI
        params = {"timestamp": stamp_now}
        res = self._get(path, params=params)
        return res

    # 查询每日资产快照，/sapi/v1/accountSnapshot
    def get_userdata_account_snapshot(self):
        stamp_now = int(round(time.time() * 1000))
        path = "%s/accountSnapshot" % self.SAPI
        params = {"type": "SPOT", "timestamp": stamp_now}
        res = self._get(path, params=params)
        return res

    # 当前最优挂单
    def get_book_ticker(self, symbol):
        path = "%s/ticker/bookTicker" % self.BASE_URL_V3
        params = {"symbol": symbol}
        time.sleep(1)
        return _get_no_sign(path, symbol, params)

    def get_order(self, symbol, order_id):
        """查询订单"""
        path = "%s/order" % self.BASE_URL_V3
        params = {"symbol": symbol, "origClientOrderId": order_id}
        time.sleep(1)
        return self._get(path, symbol, params)

    def get_spot_trades(self, symbol, from_id=0):
        """获取账户成交历史"""
        path = "%s/myTrades" % self.BASE_URL_V3
        params = {"symbol": symbol, "fromId": from_id, "limit": 500}
        time.sleep(1)
        return self._get(path, symbol, params)

    def get_open_orders(self, symbol):
        """获取当前委托"""
        path = "%s/openOrders" % self.BASE_URL_V3
        params = {"symbol": symbol}
        time.sleep(1)
        return self._get(path, symbol, params)

    def get_all_orders(self, symbol):
        """获取所有订单"""
        path = "%s/allOrders" % self.BASE_URL_V3
        params = {"symbol": symbol}
        time.sleep(1)
        return self._get(path, symbol, params)

    """----私有函数----"""

    def _sign(self, params=None):
        if params is None:
            params = {}
        data = params.copy()
        ts = int(1000 * time.time())
        data.update({"timestamp": ts})
        h = urlencode(data)
        b = bytearray()
        b.extend(self.secret.encode())
        signature = hmac.new(b, msg=h.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
        data.update({"signature": signature})
        return data

    def _get(self, path, market=None, params=None):
        if params is None:
            params = {}
        params.update({"recvWindow": recv_window})
        query = urlencode(self._sign(params))
        url = "%s?%s" % (path, query)
        header = {"X-MBX-APIKEY": self.key}
        res = requests.get(url, headers=header, timeout=30, verify=True)
        if res.status_code == 200:
            return res.json()
        else:
            error_info = f"公共方法报警：币种{market},请求异常.\nGET url:{url}\nparams:{params}\n错误原因{str(res.text)}"
            dingding_warn(error_info)
            return False

    def _post(self, path, market=None, params=None):
        if params is None:
            params = {}
        params.update({"recvWindow": recv_window})
        query = self._sign(params)
        url = "%s" % path
        header = {"X-MBX-APIKEY": self.key}
        res = requests.post(url, headers=header, data=query, timeout=180, verify=True)
        if res.status_code == 200:
            return res.json()
        else:
            error_info = f"公共方法报警：币种{market},请求异常.\nPOST url:{url}\nparams:{params}\n错误原因{str(res.text)}"
            dingding_warn(error_info)
            return False

    def _delete(self, path, market=None, params=None):
        if params is None:
            params = {}
        params.update({"recvWindow": recv_window})
        query = self._sign(params)
        url = "%s" % path
        header = {"X-MBX-APIKEY": self.key}
        res = requests.delete(url, headers=header, data=query, timeout=180, verify=True)
        if res.status_code == 200:
            return res.json()
        else:
            error_info = f"公共方法报警：币种{market},请求异常.\nDELETE url:{url}\nparams:{params}\n错误原因{str(res.text)}"
            dingding_warn(error_info)
            return False


if __name__ == "__main__":
    logger.info("-" * 60)
    coin = "ETHBUSD"
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    instance = BinanceAPI(api_key, api_secret)

    balances = instance.get_klines("ETHBUSD", "1m", 1)
    get_userdata_account_snapshot = instance.get_userdata_account_snapshot()
    print(balances)
    print(get_userdata_account_snapshot)

# -*- coding: utf-8 -*-
# @Time    : 2021-07-10 10:35
# @Author  : ashao
import time
from loguru import logger
from BinanceAPI import BinanceAPI
from dingding import ding_talk
from sqlite_db import sqlite_conn, sqlite_execute, sqlite_close
from authorization import api_secret, api_key, b_token, b_secret


def dingding_warn(text):
    ding_talk(text, b_token, b_secret)


class Build(object):
    conn, cur = sqlite_conn()
    instance = BinanceAPI(api_key, api_secret)

    def __init__(self, market, spacing=0.02, spacing_increment=0.3, investment_first=20, investment_increment=0.2,
                 profit_rate=0.03, ticker_price=0, batches_cns=16):

        # 调试批次
        # self.batches(market, "test", 0.02, 0.3, 20, 0.2, 0.03, 3233, batches_cns=16, db=False)
        # exit()

        ticker_price = self.instance.get_ticker_price(market)
        self.ticker_price = ticker_price

        self.update_commission(market)
        self.build(market, spacing, spacing_increment, investment_first, investment_increment, profit_rate,
                   self.ticker_price, batches_cns)
        sqlite_close(self.conn)

    def _execute(self, sql):
        try:
            return sqlite_execute(sql, self.conn, self.cur)
        except Exception as e:
            dingding_warn(f"分批建仓:\nbatches_build _execute:{e}")
            raise e

    def batches(self, market, period_id, spacing, spacing_increment, investment_first, investment_increment,
                profit_rate, buy_price_first, batches_cns, db=True):
        """
        可以先不关注这个函数
        :param market: 币种
        :param period_id: 期次
        :param spacing: 间距离
        :param spacing_increment: 间距递增
        :param investment_first: 首次投入
        :param investment_increment: 资金递增
        :param profit_rate: 利润比例
        :param buy_price_first: 首次买入单价
        :param batches_cns: 批次数
        :param db: 调试批次 db=False不存数据库
        :return:
        """
        print(
            f'{market}: {period_id}\n间距离 {spacing} 间距递增 {spacing_increment}\n首次买入单价 {buy_price_first} 首次投入 {investment_first}\n资金递增 {investment_increment} 利润比例 {profit_rate}')

        def _next_investment(investment):
            return investment * (1 + investment_increment)

        def _next_price(_price, _spacing_rate, _spacing=spacing, _spacing_increment=spacing_increment):
            if _price == buy_price_first:
                _price = _price * (1 - _spacing)
                return _price, _spacing_rate * (1 + _spacing_increment)
            else:
                _price = _price * (1 - _spacing_rate)
                return _price, _spacing_rate * (1 + _spacing_increment)

        total_quantity = investment_first / buy_price_first
        sell_amount = buy_price_first * (1 + profit_rate) * total_quantity
        profit = sell_amount - investment_first
        avg_price = buy_price_first
        total_investment = investment_first
        next_investment = investment_first
        next_buy_price = buy_price_first
        next_spacing_rate = spacing
        next_buy_quantity = total_quantity
        next_sell_price = buy_price_first * (1 + profit_rate)
        print("序号", "买入单价", "买入数量", "买入总价", "卖出单价", "卖出数量", "卖出总价", "累计金额", "累计买入", "利润", "当前均价")
        for i in range(1, batches_cns):
            print(i, round(next_buy_price, 2), round(next_buy_quantity, 4), round(next_investment, 2),
                  round(next_sell_price, 2), round(total_quantity - 0.00005, 4), round(sell_amount, 2),
                  round(total_investment, 2), round(total_quantity - 0.00005, 4), round(profit, 2),
                  round(avg_price, 2))
            if db:
                self._execute(
                    f'INSERT INTO `batches` (`period_id`,`market`,`serial`,`next_buy_price`,`next_buy_quantity`,'
                    f'`next_investment`,`next_sell_price`,`sell_quantity`,`sell_amount`,`total_investment`,'
                    f'`total_quantity`,`profit`,`avg_price`) VALUES({period_id},"{market}",{i},{round(next_buy_price, 2)},'
                    f'{round(next_buy_quantity, 4)},{round(next_investment, 2)},{round(next_sell_price, 2)},'
                    f'{round(total_quantity - 0.00005, 4)},{round(sell_amount, 2)},{round(total_investment, 2)},'
                    f'{round(total_quantity - 0.00005, 4)},{round(profit, 2)},{round(avg_price, 2)});')
            next_investment = _next_investment(next_investment)
            next_buy_price, next_spacing_rate = _next_price(next_buy_price, next_spacing_rate)
            next_buy_quantity = next_investment / next_buy_price
            total_quantity = next_buy_quantity + total_quantity
            total_investment = total_investment + next_investment
            next_sell_price = total_investment / total_quantity * (1 + profit_rate)
            sell_amount = next_sell_price * total_quantity
            profit = sell_amount - total_investment
            avg_price = total_investment / total_quantity

    def build(self, market, spacing, spacing_increment, investment_first, investment_increment,
              profit_rate, buy_price_first, batches_cns):
        in_progress = self._execute(
            f"SELECT * FROM `build` WHERE is_finish=0 AND `transaction`=0 AND market='{market}';")
        # 判断是否有正在执行的期次
        if in_progress:
            period_id = in_progress[0][1]
            order_id = time.strftime("%Y%m%d%H%M%S", time.localtime())
            buy_order_id = "B" + order_id
            sell_order_id = "S" + order_id
            batches_sell = self._execute(
                f'SELECT * FROM `batches` WHERE is_finish=0 AND period_id={period_id} ORDER BY serial DESC LIMIT 1;')
            avg_price = self._execute(
                f'SELECT sum(price * orig_qty) / sum(orig_qty) FROM `batches_trading` WHERE period_id = {period_id} AND is_finish=1 AND status="FILLED";')
            avg_price = round(avg_price[0][0], 2) if avg_price[0][0] else avg_price[0][0]

            buy_orders = self._execute(
                f'SELECT order_id,orig_qty,serial FROM `batches_trading` WHERE is_finish=0 AND side="BUY" AND period_id={period_id};')
            sell_orders = self._execute(
                f'SELECT order_id,orig_qty,serial FROM `batches_trading` WHERE is_finish=0 AND side="SELL" AND period_id={period_id};')
            buy_all_orders = self._execute(
                f'SELECT order_id,orig_qty,serial FROM `batches_trading` WHERE side="BUY" AND period_id={period_id} AND status !="CANCELED";')
            batches_db = self._execute(
                f'SELECT * FROM `batches` WHERE is_finish=-1 AND period_id={period_id} ORDER BY serial;')
            if batches_sell:
                investment_first_price = round(batches_sell[0][7] + 0.5)
                investment_first_qty = round(investment_first / (batches_sell[0][7] + 0.5), 4)
                actual_sell_qty = batches_sell[0][8] - investment_first_qty

                # 处理下一轮
                def next_period():
                    self.update_batches_trading(market)
                    _period_id = time.strftime("%Y%m%d%H%M", time.localtime())
                    self._execute(
                        f'INSERT INTO `build` (`period_id`,`market`) VALUES({_period_id},"{market}");')
                    _insert = self._execute(
                        f'SELECT * FROM `build` WHERE is_finish=0 AND period_id={_period_id};')
                    if _insert:
                        self.batches(market, _period_id, spacing, spacing_increment, investment_first,
                                     investment_increment,
                                     profit_rate, investment_first_price, batches_cns)
                        self._execute(
                            f'UPDATE `batches` SET is_finish=0 WHERE period_id={_period_id} AND serial=1;')
                        self.build(market, spacing, spacing_increment, investment_first,
                                   investment_increment, profit_rate,
                                   investment_first_price, batches_cns)
                    else:
                        dingding_warn(f'分批建仓:{market}\n查询分批建仓insert:{_insert}')

                if sell_orders:
                    if sell_orders[0][2] == batches_sell[0][3]:
                        # 当前有卖出委托
                        orders_status = self.instance.get_order(market, sell_orders[0][0])
                        if orders_status and orders_status.get("status") == "FILLED":
                            for _buy_orders in buy_orders:
                                logger.info(f'本轮完成，取消买入委托订单={_buy_orders[0]} serial={_buy_orders[2]}')
                                self.instance.delete_order(market, _buy_orders[0])
                            self._execute(
                                f'UPDATE `batches_trading` SET status="FILLED",is_finish=1 WHERE order_id="{sell_orders[0][0]}";')
                            self._execute(
                                f'UPDATE `batches` SET is_finish=1 WHERE period_id={period_id} AND serial={batches_sell[0][3]};')
                            self._execute(
                                f'UPDATE `build` SET profit={batches_sell[0][12]},serial={batches_sell[0][3]},is_finish=1 WHERE period_id={period_id};')
                            profit_cur = round((batches_sell[0][7] - avg_price) * batches_sell[0][8] - 0.005, 2)
                            dingding_warn(
                                f"分批建仓:{market}\n当前价:{self.ticker_price} 当前均价:{avg_price} 卖出数量:{batches_sell[0][8]}\n本期收益💰{profit_cur}")

                            # 开启下一轮
                            next_period()
                        else:
                            logger.info(
                                f'分批建仓:{market} 当前卖出委托等待交易\n当前价:{self.ticker_price} 当前均价:{avg_price} \n当前仓位:【{batches_sell[0][3]}】[{batches_sell[0][4]},{batches_sell[0][7]}]\n下次建仓:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]')
                    else:
                        self.instance.delete_order(market, sell_orders[0][0])
                        self._execute(
                            f'UPDATE `batches_trading` SET status="CANCELED",is_finish=1 WHERE order_id="{sell_orders[0][0]}";')
                        sell_limit = self.instance.sell_limit(market, actual_sell_qty, round(batches_sell[0][7] + 0.5),
                                                              order_id=sell_order_id)
                        if sell_limit:
                            sell_limit_order_id = sell_limit.get("orderId")
                            self._execute(
                                f'INSERT INTO `batches_trading` (`period_id`,`market`,`order_id`,`orderId`,`side`,`price`,`orig_qty`,`status`,`commission`,`is_finish`,`serial`) VALUES('
                                f'{period_id},"{market}","{sell_order_id}","{sell_limit_order_id}","SELL",{round(batches_sell[0][7])},{actual_sell_qty},"NEW",0,0,{batches_sell[0][3]});')
                        logger.info(f'当前卖出委托serial与实际需要委托serial不符合，卖出重新委托')
                        logger.info(
                            f'分批建仓:{market} 卖出委托\n当前价:{self.ticker_price} 当前均价:{avg_price} 卖出数量:{actual_sell_qty}\n当前仓位:【{batches_sell[0][3]}】[{batches_sell[0][4]},{batches_sell[0][7]}]\n下次建仓:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]')
                elif batches_sell[0][3] == 1:
                    # serial=1 不卖出
                    if self.ticker_price >= batches_sell[0][7]:
                        self._execute(
                            f'UPDATE `batches` SET is_finish=1 WHERE period_id={period_id} AND serial={batches_sell[0][3]};')
                        self._execute(
                            f'UPDATE `build` SET profit={batches_sell[0][12]},serial={batches_sell[0][3]},is_finish=1 WHERE period_id={period_id};')
                        profit_cur = round((batches_sell[0][7] - avg_price) * batches_sell[0][8] - 0.005, 2)
                        dingding_warn(
                            f"分批建仓:{market}\n当前价:{self.ticker_price} 当前均价:{avg_price} 卖出数量:{batches_sell[0][8]}\n本期收益💰{profit_cur}")
                        next_period()
                    else:
                        logger.info("serial=1 不挂单卖出")
                else:
                    # 判断价格是否满足卖出
                    sell = self.instance.sell_limit(market, actual_sell_qty, round(batches_sell[0][7] + 0.5),
                                                    order_id=sell_order_id)
                    if sell:
                        _order_id = sell.get("orderId")
                        self._execute(
                            f'INSERT INTO `batches_trading` (`period_id`,`market`,`order_id`,`orderId`,`side`,`price`,`orig_qty`,`status`,`commission`,`is_finish`,`serial`) VALUES('
                            f'{period_id},"{market}","{sell_order_id}","{_order_id}","SELL",{round(batches_sell[0][7])},{actual_sell_qty},"NEW",0,0,{batches_sell[0][3]});')
                        logger.info(
                            f'分批建仓:{market} 卖出委托\n当前价:{self.ticker_price} 当前均价:{avg_price} 卖出数量:{actual_sell_qty}\n当前仓位:【{batches_sell[0][3]}】[{batches_sell[0][4]},{batches_sell[0][7]}]\n下次建仓:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]')
                    else:
                        dingding_warn(
                            f'分批建仓:{market} 卖出失败\n当前价:{self.ticker_price} 当前均价:{avg_price} 卖出数量:{actual_sell_qty}')
                if buy_orders:
                    orders_status = self.instance.get_order(market, buy_orders[0][0])
                    if orders_status and orders_status.get("status") == "FILLED":
                        self._execute(
                            f'UPDATE `batches_trading` SET status="FILLED",is_finish=1 WHERE order_id="{buy_orders[0][0]}";')
                        self._execute(
                            f'UPDATE `batches` SET is_finish=0 WHERE period_id={period_id} AND serial={buy_orders[0][2]};')
                        avg_price = self._execute(
                            f'SELECT sum(price * orig_qty) / sum(orig_qty) FROM `batches_trading` WHERE status="FILLED" AND period_id = {period_id} AND is_finish=1;')
                        avg_price = round(avg_price[0][0], 2) if avg_price[0][0] else avg_price[0][0]
                        dingding_warn(
                            f'分批建仓:{market}\n当前价:{self.ticker_price} 当前委托:{int(batches_db[0][4])}\n当前均价:{avg_price} 买入数量:{buy_orders[0][1]}\n当前仓位:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]\n下次建仓:【{batches_db[1][3]}】[{batches_db[1][4]},{batches_db[1][7]}]')
                    else:
                        profit_now = round(batches_sell[0][8] * (self.ticker_price - avg_price), 3)
                        logger.info(
                            f'分批建仓:{market} 当前买入委托等待交易\n当前价:{self.ticker_price} 当前均价:{avg_price} 当前盈亏:💰{profit_now}\n当前仓位:【{batches_sell[0][3]}】[{batches_sell[0][4]},{batches_sell[0][7]}]\n下次建仓:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]')
                else:
                    buy_qty = batches_db[0][5]
                    serial = batches_db[0][3]
                    for i, j in enumerate(batches_db[1:]):
                        if self.ticker_price < j[4]:
                            buy_qty = round(buy_qty + j[5], 4)
                            serial = j[3]
                        else:
                            break
                    if buy_all_orders or serial == 2:
                        have_open_order = 0
                        for _buy_all_orders in buy_all_orders:
                            if serial == _buy_all_orders[2]:
                                have_open_order = 1
                                logger.info("当前已经有成交或者买入委托订单")
                        if have_open_order:
                            logger.info(
                                f"等待交易买入: 当前市价:{self.ticker_price} 当前均价:{avg_price}\n当前仓位:【{batches_sell[0][3]}】[{batches_sell[0][4]},{batches_sell[0][7]}]\n下次建仓:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]")
                        else:
                            buy = self.instance.buy_limit(market, buy_qty, int(batches_db[0][4]), order_id=buy_order_id)
                            _order_id = buy.get("orderId")
                            self._execute(
                                f'INSERT INTO `batches_trading` (`period_id`,`market`,`order_id`,`orderId`,`side`,`price`,`orig_qty`,`status`,`commission`,`is_finish`,`serial`) VALUES('
                                f'{period_id},"{market}","{buy_order_id}","{_order_id}","BUY",{int(batches_db[0][4])},{buy_qty},"NEW",0,0,{batches_db[0][3]});')
                            logger.info(
                                f'分批建仓:{market} 买入委托:{buy_qty}\n当前价:{self.ticker_price} 当前均价:{avg_price} \n当前仓位:【{batches_sell[0][3]}】[{batches_sell[0][4]},{batches_sell[0][7]}]\n下次建仓:【{batches_db[0][3]}】[{batches_db[0][4]},{batches_db[0][7]}]')
                    else:
                        dingding_warn(
                            f"分批建仓:{market}\n数据错误:当前市价:{self.ticker_price},[{batches_db[0][4]},{batches_db[0][7]}]")
            else:
                if batches_db:
                    # 当前价格小于补仓价，买入补仓
                    buy_qty = batches_db[0][5]
                    batch = -1
                    for i, j in enumerate(batches_db[1:]):
                        if self.ticker_price < j[4]:
                            buy_qty = round(buy_qty + j[5], 4)
                            batch = i
                        else:
                            break
                    buy = self.instance.buy_market(market, buy_qty, order_id=buy_order_id)
                    _order_id = buy.get("orderId")
                    batch = batch + 1
                    self._execute(
                        f'INSERT INTO `batches_trading` (`period_id`,`market`,`order_id`,`orderId`,`side`,`orig_qty`,`status`,`commission`,`is_finish`,`serial`) VALUES('
                        f'{period_id},"{market}","{buy_order_id}","{_order_id}","BUY",{buy_qty},"NEW",0,0,{batches_db[batch][3]});')
                    self._execute(
                        f'UPDATE `batches` SET is_finish=0 WHERE period_id={period_id} AND serial>={batches_db[0][3]} AND serial<={batches_db[batch][3]};')
                    self.update_batches_trading(market)
                    self.update_commission(market)
                    avg_price = self._execute(
                        f'SELECT sum(price * orig_qty) / sum(orig_qty) FROM `batches_trading` WHERE period_id = {period_id} AND is_finish=1;')
                    avg_price = round(avg_price[0][0], 2) if avg_price[0][0] else avg_price[0][0]
                    batches = self._execute(
                        f'SELECT * FROM `batches` WHERE is_finish=-1 AND period_id={period_id} ORDER BY serial LIMIT 1;')
                    if batches:
                        dingding_warn(
                            f"分批建仓:{market}\n当前价:{self.ticker_price}\n当前均价:{avg_price} 买入数量:{buy_qty}\n当前仓位:【{batches_db[batch][3]}】[{batches_db[batch][4]},{batches_db[batch][7]}]\n下次建仓:【{batches[0][3]}】[{batches[0][4]},{batches[0][7]}]")
                    else:
                        dingding_warn(
                            f"分批建仓:{market}\n当前价:{self.ticker_price} 当前均价:{avg_price} 买入数量:{buy_qty}\n当前仓位:【{batches_db[batch][3]}】[{batches_db[batch][4]},{batches_db[batch][7]}]")
                else:
                    dingding_warn(
                        f"分批建仓:{market}\n初始化数据错误:当前市价:{self.ticker_price},[{batches_db[0][4]},{batches_db[0][7]}]")
        else:
            """
            新建期次，开启下一轮
            """
            period_id = time.strftime("%Y%m%d%H%M", time.localtime())
            self._execute(f'INSERT INTO `build` (`period_id`,`market`) VALUES({period_id},"{market}");')
            insert = self._execute(f'SELECT * FROM `build` WHERE is_finish=0 AND period_id={period_id};')
            if insert:
                self.batches(market, period_id, spacing, spacing_increment, investment_first, investment_increment,
                             profit_rate, buy_price_first, batches_cns)
                self.build(market, spacing, spacing_increment, investment_first, investment_increment, profit_rate,
                           buy_price_first, batches_cns)
            else:
                dingding_warn(f'分批建仓:{market}\n查询分批建仓insert:{insert}')

    def update_batches_trading(self, market):
        """
        更新交易委托状态
        """
        orders = self._execute(
            f"SELECT order_id,market,status,orderId,commission FROM `batches_trading` WHERE is_finish=0 AND side='BUY' AND market='{market}';")
        if orders:
            for _order in orders:
                if _order[2] == "NEW":
                    orders_result = self.instance.get_order(_order[1], _order[0])
                    if orders_result and orders_result.get("status") != "NEW":
                        status = orders_result.get("status")
                        self._execute(
                            f'UPDATE `batches_trading` SET status="{status}",is_finish=1 WHERE order_id="{_order[0]}";')
                elif _order[2] == "FILLED":
                    self._execute(f'UPDATE `batches_trading` SET is_finish=1 WHERE order_id="{_order[0]}";')

    def update_commission(self, market):
        """
        更新手续费，trade_id 实际成交均价
        :param market:
        :return:
        """
        sum_commission = 0
        trades = self._execute(
            f"SELECT trade_id,period_id FROM `batches_trading` WHERE is_finish=1 AND status='FILLED' AND trade_id =0 AND market='{market}';")
        trade_from_id = self._execute(
            f"SELECT trade_id,period_id FROM `batches_trading` WHERE is_finish=1 AND status='FILLED' AND trade_id !=0 AND market='{market}' ORDER BY trade_id DESC LIMIT 1;")
        if trades:
            if not trade_from_id:
                trade_from_id = 0
            else:
                trade_from_id = trade_from_id[0][0]
            get_spot_trades = self.instance.get_spot_trades(market, trade_from_id)
            if get_spot_trades:
                for _order in get_spot_trades:
                    _order_id = _order.get("orderId")
                    trade_id = _order.get("id")
                    price = _order.get("price")
                    commission = _order.get("commission")
                    self._execute(
                        f'UPDATE `batches_trading` SET trade_id={trade_id},price={price},commission="{commission}" WHERE orderId="{_order_id}";')
            sum_commission = \
                self._execute(f"SELECT SUM(commission) FROM `batches_trading` WHERE period_id={trades[0][1]};")[0][0]
        return sum_commission

    def get_profit(self, market):
        """
        获取本轮利润，暂时没有什么用
        :return:
        """
        profit = 0
        trades = self._execute(
            f"SELECT * FROM `batches_trading` WHERE is_finish=1 AND side='SELL' AND market='{market}' ORDER BY period_id DESC LIMIT 1;")
        if trades:
            buy_amount = self._execute(
                f'SELECT SUM(price*orig_qty) FROM `batches_trading` WHERE side="BUY" AND period_id={trades[0][1]};')
            sell_amount = self._execute(
                f'SELECT SUM(price*orig_qty) FROM `batches_trading` WHERE side="SELL" AND period_id={trades[0][1]};')
            profit = round(sell_amount[0][0] - buy_amount[0][0], 2)
        return profit


if __name__ == "__main__":
    logger.info("-" * 60)
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    symbol = "ETHBUSD"
    # 程序入口
    Build(symbol)

import asyncio
import traceback

import nats
import json
import gzip
from datetime import datetime
from typing import Optional, Dict, Any
import threading
import time

class TopicDataHandler:
    """
    订阅主题数据回调接口,可自定义传递进来
    """
    def ch_stock_real(self, data):
        """
        A股实时行情
        :param data:
        :return:
        """
        pass

    def ch_market_real(self, data):
        """
        A股指数实时行情
        :param data:
        :return:
        """
        pass

    def ch_connect_real(self, data):
        """
        A股概念板块实时行情
        :param data:
        :return:
        """
        pass

    def ch_industry_real(self, data):
        """
        A股行业板块实时行情
        :param data:
        :return:
        """
        pass

    def ch_stock_l2_data_real(self, data):
        """
        A股L2 实时数据
        :param data:
        :return:
        """
        pass

    def ch_stock_ddx_data_real(self, data):
        """
        A股个股DDX数据实时行情
        :param data:
        :return:
        """
        pass

    def hk_market_real(self, data):
        """
        港股指数实时行情
        :param data:
        :return:
        """
        pass

    def hk_stock_real(self, data):
        """
        港股市场个股实时行情
        :param data:
        :return:
        """
        pass

    def ch_future_real(self, data):
        """
        国内股指期货，商品期货实时行情
        :param data:
        :return:
        """
        pass

    def ch_kzz_real(self, data):
        """
        国内可转账实时行情
        :param data:
        :return:
        """
        pass

    def ch_stock_l2_transaction(self, data):
        """
        l2 逐笔明细数据回调接口
        :param data:
        :return:
        """
        pass

    def ch_stock_l2_orders(self, data):
        """
        l2 委托明细数据回调接口
        :param data:
        :return:
        """
        pass

    def ch_stock_l2_depth(self, data):
        """
        l2 行情数据含十档数据回调接口
        :param data:
        :return:
        """
        pass


"""
使用事项：
1.需要安装  nats-py==2.14.0
"""
class PandaStock:
    def __init__(self, phone, nid, handler=TopicDataHandler()):
        """
        初始化客户端参数
        """
        self.phone = phone
        self.nid = nid
        self.server_url = f"nats://{self.phone}:{self.nid}@101.133.174.225:9990"
        self.nc = None
        self.loop = None
        self.loop_thread = None
        self.connected = False
        self.handler = handler

        self._ch_stock_real = {}    # A股当前实时行情

    def _start_event_loop(self):
        """启动事件循环（线程安全）"""
        if self.loop is None or not self.loop.is_running():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            self.loop_thread = threading.Thread(
                target=self.loop.run_forever,
                daemon=True
            )
            self.loop_thread.start()
            time.sleep(0.5)  # 等待事件循环启动

    def connect_server(self, timeout: float = 20.0) -> bool:
        """
        同步连接服务器

        Args:
            timeout: 连接超时时间（秒）

        Returns:
            连接是否成功
        """
        self._start_event_loop()

        async def connect():
            try:
                self.nc = await nats.connect(
                    self.server_url,
                    name=f"client_{self.phone}",
                    connect_timeout=5,
                    ping_interval=20,
                    max_reconnect_attempts=100,
                    allow_reconnect=True,
                )

                self.connected = True
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 用户 {self.phone} 连接成功")
                return True

            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 连接失败: {e}")
                self.connected = False
                return False

        future = asyncio.run_coroutine_threadsafe(connect(), self.loop)

        try:
            result = future.result(timeout=timeout)
            return result
        except Exception as e:
            print(f"连接超时或失败: {e}")
            return False

    def subscribe_ch_l2_deep_single(self, stock_code):
        """
        订阅单只个股L2行情信息，含十档数据
        :return:
        """
        self.__subscribe_topic(f"l2.depth.{stock_code}")

    def subscribe_ch_l2_depth_batch(self, stock_codes):
        """
        订阅多只股票的委托L2行情信息，含十档数据
        """
        for code in stock_codes:
            self.subscribe_ch_l2_deep_single(code)

    def subscribe_ch_l2_depth_all(self):
        """
        订阅所有个股L2行情信息，含十档数据
        :return:
        """
        self.__subscribe_topic("l2.depth.*")

    def subscribe_ch_l2_orders_single(self, stock_code):
        """
        订阅单只个股买一，卖一L2委托明细
        :return:
        """
        self.__subscribe_topic(f"l2.orders.{stock_code}")

    def subscribe_ch_l2_orders_batch(self, stock_codes):
        """
        订阅多只股票买一，卖一L2委托明细
        """
        for code in stock_codes:
            self.subscribe_ch_l2_orders_single(code)

    def subscribe_ch_l2_orders_all(self):
        """
        订阅所有个股买一，卖一L2委托明细
        :return:
        """
        self.__subscribe_topic("l2.orders.*")

    def subscribe_ch_l2_transaction_single(self, stock_code):
        """
        订阅单只个股L2逐笔成交明细
        :return:
        """
        self.__subscribe_topic(f"l2.tran.{stock_code}")

    def subscribe_ch_l2_transaction_batch(self, stock_codes):
        """
        订阅多只股票的逐笔成交明细数据
        """
        for code in stock_codes:
            self.subscribe_ch_l2_transaction_single(code)

    def subscribe_ch_l2_transaction_all(self):
        """
        订阅所有个股逐笔成交明细数据
        :return:
        """
        self.__subscribe_topic("l2.tran.*")

    def subscribe_ch_market_real(self) -> bool:
        """
        订阅A股指数实时行情
        :return:
        """
        return self.__subscribe_topic("chMarketReal")

    def subscribe_ch_stock_real(self) -> bool:
        """
        订阅A股个股实时行情
        :return:
        """
        return self.__subscribe_topic("chStockReal")

    def subscribe_ch_concept_real(self) -> bool:
        """
        订阅A股概念板块实时行情
        :return:
        """
        return self.__subscribe_topic("chConceptReal")

    def subscribe_ch_industry_real(self) -> bool:
        """
        订阅A股行业板块实时行情
        :return:
        """
        return self.__subscribe_topic("chIndustryReal")

    def subscribe_ch_l2_data_real(self) -> bool:
        """
        订阅A股个股L2实时数据
        :return:
        """
        return self.__subscribe_topic("l2Data")

    def subscribe_ch_ddx_data_real(self) -> bool:
        """
        订阅A股个股ddx实时数据
        :return:
        """
        return self.__subscribe_topic("DdxData")

    def subscribe_hk_market_real(self) -> bool:
        """
        订阅港股指数实时行情
        :return:
        """
        return self.__subscribe_topic("hkMarketReal")

    def subscribe_hk_stock_real(self) -> bool:
        """
        订阅港股个股实时行情
        :return:
        """
        return self.__subscribe_topic("hkStockReal")

    def subscribe_ch_kzz_real(self) -> bool:
        """
        订阅可转债实时行情
        :return:
        """
        return self.__subscribe_topic("chKzzReal")

    def subscribe_ch_future_real(self) -> bool:
        """
        订阅国内股指期货，商品期货实时行情
        :return:
        """
        return self.__subscribe_topic("futureReal")

    def subscribe_ch_kzz_stock_real(self) -> bool:
        """
        订阅可转债个股实时行情
        :return:
        """
        return self.__subscribe_topic("chKzzReal")

    def __subscribe_topic(self, topic):
        if not self.nc:
            print("未连接，请先调用connect()")
            return False

        async def connect_topic():
            handler = self.market_data_handler(topic)
            await self.nc.subscribe(topic, cb=handler)

        future = asyncio.run_coroutine_threadsafe(connect_topic(), self.loop)

        try:
            future.result(timeout=60)
            return True
        except Exception as e:
            print(f"连接超时或失败: {e}")
            return False

    def market_data_handler(self, topic):
        async def handler(msg):
            try:
                json_str = gzip.decompress(msg.data).decode('utf-8')
                data = json.loads(json_str)

                now = time.time()
                # 根据主题处理不同数据
                if topic == "chMarketReal":
                    self.handler.ch_market_real(data)

                elif topic == "chStockReal":
                    self.handler.ch_stock_real(data)

                elif topic == "chConceptReal":
                    self.handler.ch_connect_real(data)

                elif topic == "chIndustryReal":
                    self.handler.ch_industry_real(data)

                elif topic == "l2Data":
                    self.handler.ch_stock_l2_data_real(data)

                elif topic == "DdxData":
                    self.handler.ch_stock_ddx_data_real(data)

                elif topic == "hkMarketReal":
                    self.handler.hk_market_real(data)

                elif topic == "hkStockReal":
                    self.handler.hk_stock_real(data)

                elif topic == "futureReal":
                    self.handler.ch_future_real(data)

                elif topic == "chKzzReal":
                    self.handler.ch_kzz_real(data)

                elif topic.startswith("l2.tran"):
                    self.handler.ch_stock_l2_transaction(data)

                elif topic.startswith("l2.orders"):
                    self.handler.ch_stock_l2_orders(data)

                elif topic.startswith("l2.depth"):
                    self.handler.ch_stock_l2_depth(data)


            except Exception as e:
                print(f"消息解析失败: {e}")

        return handler


    def __request_sync(self,
                     subject: str,
                     data: Dict[str, Any] = {},
                     ) -> Optional[Dict[str, Any]]:
        """
        同步请求（复用连接）

        Args:
            subject: 请求主题
            data: 请求数据
        Returns:
            响应数据，失败返回 None
        """
        if not self.connected or not self.nc:
            print("❌️未连接，请先调用 connect_sync()")
            return None

        async def request_async():
            count = 1
            while count < 5:
                try:
                    data["phone"] = self.phone
                    data["nid"] = self.nid

                    json_str = json.dumps(data, ensure_ascii=False)
                    compressed = gzip.compress(json_str.encode('utf-8'))
                    # 发送请求
                    response = await self.nc.request(
                        subject,
                        compressed,
                        timeout=300
                    )

                    # 处理响应
                    try:
                        decompressed = gzip.decompress(response.data).decode('utf-8')
                        result = json.loads(decompressed)
                    except:
                        json_str = response.data.decode('utf-8')
                        result = json.loads(json_str)

                    return result

                except asyncio.TimeoutError:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ⏰ 请求超时: {subject}")
                    await asyncio.sleep(2)
                    count += 1
                    continue

                except Exception as e:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 请求失败: {e}")
                    await asyncio.sleep(2)
                    count += 1
                    continue

        try:
            future = asyncio.run_coroutine_threadsafe(request_async(), self.loop)
            result = future.result(timeout=300)
            return result
        except Exception as e:
            print(f"请求执行异常")
            print(traceback.format_exc())
            return None

    def close(self) -> bool:
        """
        同步关闭连接
        Returns:
            是否成功关闭
        """
        if not self.nc or not self.connected:
            return True

        async def close_async():
            try:
                await self.nc.close()
                self.connected = False
                print("✅ 连接已关闭")
                return True
            except Exception as e:
                print(f"❌ 关闭连接失败: {e}")
                return False

        try:
            future = asyncio.run_coroutine_threadsafe(close_async(), self.loop)
            result = future.result()

            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)

            if self.loop_thread:
                self.loop_thread.join(timeout=2.0)

            return result
        except Exception as e:
            print(f"关闭连接异常: {e}")
            return False

    def get_ch_stock(self) -> dict or None:
        """
        获取A股个列表
        :return:
        """
        return self.__request_sync(
            "chStockList",
        )

    def get_ch_concept(self) -> dict or None:
        """
        获取A股概念板块列表
        :return:
        """
        return self.__request_sync(
            "chConceptList",
        )

    def get_ch_industry(self) -> dict or None:
        """
        获取A股行业板块列表
        :return:
        """
        return self.__request_sync(
            "chIndustryList",
        )

    def get_ch_stock_minute_real(self, code: str, minute: int) -> dict or None:
        """
        获取A股个股实时分钟
        :param code:  查看个股代码
        :param minute:  查询分钟，值为 1,5,15,30,60 中的一个
        :return: dict，包含的key及value信息如下
            key      value
            code     个股代码
            data     分钟数据list，各个字段信息如下
                        date_time   // 日期时间
                        open         // 开盘价
                        high         // 最高价
                        low          // 最低价
                        close        // 收盘价
                        amount       // 成交额(元)
                        volume       // 成交量(手)
            date      日期
            minutes   查询的分钟数
        :return:
        """
        return self.__request_sync(
            "chStockMinuteReal",
            {"code": code, "minute": minute}
        )

    def get_ch_stock_front_day_history(self, code: str) -> list or None:
        """
        获取个股前复权历史日线
        :param code: 个股代码
        :return: pandas 各字段信息如下
            close            // 收盘价
            open             // 开盘价
            high             // 最高价
            low              // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
        """
        return self.__request_sync(
            "chStockFrontDayHistory",
            {"code": code}
        )

    def get_ch_concept_day_history(self, code: str) -> list or None:
        """
        获取概念板块历史日线
        :param code: 板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open             // 开盘价
            high             // 最高价
            low              // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
        """
        return self.__request_sync(
            "chConceptDayHistory",
            {"code": code}
        )

    def get_ch_industry_day_history(self, code: str) -> list or None:
        """
        获取行业板块历史日线
        :param code: 板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open             // 开盘价
            high             // 最高价
            low              // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
        """
        return self.__request_sync(
            "chIndustryDayHistory",
            {"code": code}
        )

    def get_ch_stock_minute_history(self, code: str, minute: int, date: str) -> dict or None:
        """
        查询个股历史分钟数据，目前支持按照年月进行查找。如查询2026年3月数据，date传递为20263
        :param code:   个股代码
        :param date:   日期，格式为年份+月份(1-12)， 如20263
        :param minute 分钟周期，取值为1,5,15,30， 60
        :return: dict
        key      value
        code     个股大妈
        data     分钟数据list，各字段信息如下
                        date_time   // 日期时间
                        open         // 开盘价
                        high         // 最高价
                        low          // 最低价
                        close        // 收盘价
                        amount       // 成交额(元)
                        volume       // 成交量(手)
        date      查询的年月信息
        """
        return self.__request_sync(
            "chStockMinuteHistory",
            {"code": code, "minute": minute, "date": date}
        )

    def get_ch_stock_transaction_real(self, code: str) -> dict or None:
        """
        获取个股当日实时分笔。该接口只在交易时段内有效，非交易时段无数据
        :param code: 个股代码
        :return: dict
        key    value
        code   个股代码
        data   分笔数据，各字段信息如下
                    time                // 时间
                    price                // 价格
                    volume               // 成交量
                    buy_or_sell          // 买卖方向  0 买盘  1 卖盘
        date     日期
        """
        return self.__request_sync(
            "chStockTransactionReal",
            {"code": code,}
        )

    def get_ch_stock_transaction_history(self, code: str, year: str, month: int) -> dict or None:
        """
        查询个股历史分笔
        :param code:    个股代码
        :param year:    查询年份
        :param month:   查询月份
        :return: 包含字典的list，每个字典信息如下
        key    value
        date   查询月份内的日期
        value  该日期当天的分笔数据，各个数据项信息如下
                    time                // 时间
                    price                // 价格
                    volume               // 成交量
                    buy_or_sell          // 买卖方向  0 买盘  1 卖盘
        """
        return self.__request_sync(
            "chStockTransactionHistory",
            {"code": code, "year": year, "month": month}
        )

    def get_sh_market_daily_info(self, date: str) -> dict or None:
        """
        获取上海证券交易所每个交易日汇总信息
        :param date: 日期，格式为  2026-03-01
        :return: dict，键值对信息如下
        key        value
        a_stock    上证A股信息字典，各字段信息如下
                        total_value      // 总市值(亿元)
                        trade_vol        // 成交量(亿股)
                        avg_pe           // 平均市盈率(倍)
                        total_to_rate    // 换手率(%)
                        nego_to_rate     // 流通换手率(%)
                        trade_amt        // 成交金额(亿元)
                        nego_value       // 流通市值(亿元)
                        list_num         // 股票数
        b_stock    上证B股信息字典，字段信息同a_stock
        kcb_stock  科创板信息字典，字典信息同a_stock
        market     上海证券交易所总体字典，字典信息同a_stock
        date       日期
        """
        return self.__request_sync(
            "ShMarketDailyInfo",
            {"date": date}
        )

    def get_sh_market_week_info(self, date: str) -> dict or None:
        """
        获取上海证券交易所每周汇总信息
        :param date: 日期，格式为  2026-03-01
        :return: dict，键值对信息如下
        key        value
        a_stock    上证A股信息字典，各字段信息如下
                        low_vol          // 最低成交量(亿股)
                        low_vol_date    // 最低成交量日期
                        low_amt          // 最低成交金额(亿元)
                        low_amt_date    // 最低成交金额日期
                        avg_pe_rate      // 平均市盈率(倍)
                        trade_amt        // 成交金额(亿元)
                        list_num         // 股票数量
                        high_amt         // 最高成交金额(亿元)
                        high_amt_date   // 最高成交金额日期
                        high_vol        // 最高成交量(亿股)
                        high_vol_date  // 最高成交量日期
                        total_value     // 市价总值(亿元)
                        trade_vol       // 成交量(亿股)
                        total_to_rate   // 换手率(%)
                        nego_value      // 流通市值(亿元)
                        trade_days      // 交易天数
                        to_rate          // 流通换手率(%)
        b_stock    上证B股信息字典，字段信息同a_stock
        kcb_stock  科创板信息字典，字典信息同a_stock
        market     上海证券交易所总体字典，字典信息同a_stock
        date       日期
        """
        return self.__request_sync(
            "ShMarketWeekInfo",
            {"date": date}
        )

    def get_sh_market_month_info(self, date: str) -> dict or None:
        """
        获取上海证券交易所每月汇总信息
        :param date: 日期，格式为  2026-03
        :return: dict，键值对信息如下
        key        value
        a_stock    上证A股信息字典，各字段信息如下
                        low_vol          // 最低成交量(亿股)
                        low_vol_date    // 最低成交量日期
                        low_amt          // 最低成交金额(亿元)
                        low_amt_date    // 最低成交金额日期
                        avg_pe_rate      // 平均市盈率(倍)
                        trade_amt        // 成交金额(亿元)
                        list_num         // 股票数量
                        high_amt         // 最高成交金额(亿元)
                        high_amt_date   // 最高成交金额日期
                        high_vol        // 最高成交量(亿股)
                        high_vol_date  // 最高成交量日期
                        total_value     // 市价总值(亿元)
                        trade_vol       // 成交量(亿股)
                        total_to_rate   // 换手率(%)
                        nego_value      // 流通市值(亿元)
                        trade_days      // 交易天数
                        to_rate          // 流通换手率(%)
        b_stock    上证B股信息字典，字段信息同a_stock
        kcb_stock  科创板信息字典，字典信息同a_stock
        market     上海证券交易所总体字典，字典信息同a_stock
        date       日期
        """
        return self.__request_sync(
            "ShMarketMonthInfo",
            {"date": date}
        )

    def get_ch_market_day_history(self, code: str) -> list or None:
        """
        获取指数历史日线
        :param code: 指数代码
        :return: list，字段信息如下
            close            // 收盘价
            open             // 开盘价
            high             // 最高价
            low              // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
        """
        return self.__request_sync(
            "ChMarketDayHistory",
            {"code": code}
        )

    def get_ch_stock_front_week_history(self, code: str) -> list or None:
        """
        获取个股前复权周线数据
        :param code: 个股代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChStockFrontWeekHistory",
            {"code": code}
        )

    def get_ch_stock_front_month_history(self, code: str) -> list or None:
        """
        获取个股前复权周线数据
        :param code: 个股代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChStockFrontMonthHistory",
            {"code": code}
        )

    def get_ch_concept_month_history(self, code: str) -> list or None:
        """
        获取概念板块月线数据
        :param code: 概念板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChConceptMonthHistory",
            {"code": code}
        )

    def get_ch_concept_week_history(self, code: str) -> list or None:
        """
        获取概念板块周线数据
        :param code: 概念板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChConceptWeekHistory",
            {"code": code}
        )

    def get_ch_industry_month_history(self, code: str) -> list or None:
        """
        获取行业板块月线数据
        :param code: 板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChIndustryMonthHistory",
            {"code": code}
        )

    def get_ch_industry_week_history(self, code: str) -> list or None:
        """
        获取行业板块周线数据
        :param code: 概念板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChIndustryWeekHistory",
            {"code": code}
        )

    def get_ch_market_month_history(self, code: str) -> list or None:
        """
        获取指数月线数据
        :param code: 板块代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChMarketMonthHistory",
            {"code": code}
        )

    def get_ch_market_week_history(self, code: str) -> list or None:
        """
        获取指数周线数据
        :param code: 指数代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChMarketWeekHistory",
            {"code": code}
        )

    def get_ch_stock_financial(self, code: str) -> dict or None:
        """
        获取个股历史财务信息
        :param code: 个股代码
        :return: dict 字段信息
        key             value
        announce_time   公告日期
        tag_time        报告期
        fn_fields       财务指标字段，指标过多，参考文档说明
        """
        return self.__request_sync(
            "ChStockFinancial",
            {"code": code}
        )

    def get_ch_one_stock_real(self, code) -> dict or None:
        """
        获取单一个股实时行情
        :param code  个股代码
        :return: dict
        key 为个股代码
        value 内各字段信息如下
            last_close       // 昨日收盘价
            open             // 今日开盘价
            high             // 今日最高价
            low              // 今日最低价
            close            // 现价
            volume          // 成交量(手)
            current_volume   // 当前成交量
            amount         // 成交额（万元）
            inside          // 内盘成交
            outside         // 外盘成交
            avg_price       // 均价(暂时不可用)
            change          // 涨幅
            turnover        // 换手率
            volume_ratio    // 量比
            bid_ask_ratio   // 委比
            market_cap      // 总市值
            float_cap       // 流通市值
            dynamic_pe     // 动态市盈率
            static_pe       // 静态市盈率
            pb              // 市净率
            time           // 时间
            buy1         // 买一价
            buy1_vol     // 买一量
            buy2         // 买二价
            buy2_vol     // 买二量
            buy3         // 买三价
            buy3_vol     // 买三量
            buy4         // 买四价
            buy4_vol     // 买四量
            buy5        // 买五价
            buy5_vol    // 买五量
            sell1       // 卖一价
            sell1_vol   // 卖一量
            sell2       // 卖二价
            sell2_vol   // 卖二量
            sell3       // 卖三价
            sell3_vol   // 卖三量
            sell4       // 卖四价
            sell4_vol   // 卖四量
            sell5       // 卖五价
            sell5_vol   // 卖五量
        """
        return self.__request_sync(
            "ChOneStockReal",
            {"code": code}
        )

    def get_ch_stock_income_statement(self, code: str) -> list or None:
        """
        获取个股利润表
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockIncomeStatement",
            {"code": code}
        )

    def get_ch_stock_cash_flow_statement(self, code: str) -> list or None:
        """
        获取个股现金流表
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockCashFlow",
            {"code": code}
        )

    def get_ch_stock_financial_indicators(self, code: str) -> list or None:
        """
        获取个股财务指标表
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockFinancialIndicators",
            {"code": code}
        )

    def get_ch_stock_share_capital_and_shareholders(self, code: str) -> list or None:
        """
        获取个股股本股东情况表
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockShareHolder",
            {"code": code}
        )

    def get_ch_stock_performance_forecast(self, code: str) -> list or None:
        """
        获取个股业绩快报表
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockPerformanceForecast",
            {"code": code}
        )

    def get_ch_stock_auxiliary_data(self, code: str) -> list or None:
        """
        获取个股财务辅助指标表
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockAuxiliaryData",
            {"code": code}
        )

    def get_ch_stock_balance_sheet(self, code) -> list or None:
        """
        获取个股资产负债表历史
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockBalanceSheet",
            {"code": code}
        )

    def get_core_new(self, date: str) -> list or None:
        """
        获取指定日期的核心新闻
        :return: 字典数据
        key    values
        date   查询日期
        data   当日新闻列表
                title   // 标题
                date    // 发布日期
                content // 正文
        """
        return self.__request_sync(
            "ChCoreNews",
            {"day": date}
        )

    def get_domestic_financial_news(self, date: str) -> list or None:
        """
        获取指定日期的国内主要新闻
        :param date: 日期
        :return: 字典数据
        key    values
        date   查询日期
        data   当日新闻列表
                title   // 标题
                date    // 发布日期
                content // 正文
        """
        return self.__request_sync(
            "ChDomesticNews",
            {"day": date}
        )

    def get_global_financial_news(self, date: str) -> dict or None:
        """
        获取指定日期的国际主要新闻
        :param date: 日期
        :return: 字典数据
        key    values
        date   查询日期
        data   当日新闻列表
                title   // 标题
                date    // 发布日期
                content // 正文
        """
        return self.__request_sync(
            "ChGlobalNews",
            {"day": date}
        )

    def get_options_news(self, date: str) -> dict or None:
        """
        获取指定日期时评新闻
        :param date: 日期
        :return: 字典数据
        key    values
        date   查询日期
        data   当日新闻列表
                title   // 标题
                date    // 发布日期
                content // 正文
        """
        return self.__request_sync(
            "ChOptionNews",
            {"day": date}
        )

    def get_ch_stock_month_news(self, code: str, day: str) -> dict or None:
        """
        获取指定年月A股个股新闻
        :param code: 个股代码
        :param day: 日期  2026-04
        :return: 字典数据
        key    values
        date   查询日期
        data   当日新闻列表
                title   // 标题
                date    // 发布日期
                content // 正文
        """
        return self.__request_sync(
            "ChStockMonthNews",
            {"code": code, "day": day}
        )

    def get_ch_stock_announce(self, code: str, date: str) -> dict or None:
        """
        获取个股指定日期公告
        """
        return self.__request_sync(
            "ChStockAnnounce",
            {"code": code, "date": date}
        )

    def get_ch_stock_research_report(self, code: str, date: str) -> dict or None:
        """
        获取个股指定日期研报
        """
        return self.__request_sync(
            "ChStockResearchReport",
            {"code": code, "date": date}
        )

    def get_ch_stock_attention_tank(self, code: str) -> list or None:
        """
        获取个股全市场及行业内部市场人气排名历史
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockAttentionTank",
            {"code": code}
        )

    def get_ch_stock_share_holder(self, code: str) -> list or None:
        """
        获取个股股东人数历史变化数据
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockHolderCount",
            {"code": code}
        )

    def get_ch_stock_block_trading(self, code: str) -> list or None:
        """
        获取个股大宗交易历史
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockBlockTrading",
            {"code": code}
        )

    def get_ch_stock_inc_or_dec(self, code: str) -> list or None:
        """
        获取个股大宗交易历史
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockIncOrDec",
            {"code": code}
        )

    def get_rzrq_balance(self) -> list or None:
        """
        获取融资融券余额历史数据
        :return:
        """
        return self.__request_sync(
            "ChRzRqBalance",
        )

    def get_ch_limit_up_down(self) -> list or None:
        """
        获取A股涨跌停数量历史数据
        :return:
        """
        return self.__request_sync(
            "ChLimitUpDown",
        )

    def get_hk_stock(self) -> dict or None:
        """
        获取港股个股列表
        :return
        """
        return self.__request_sync(
            "HkStock",
        )

    def get_hk_stock_day_history(self, code: str) -> list or None:
        """
        获取港股个股前复权日线数据
        :return:
        """
        return self.__request_sync(
            "HkStockDayHistory",
            {"code": code}
        )

    def get_hk_stock_week_history(self, code: str) -> list or None:
        """
        获取港股个股月线数据
        :return:
        """
        return self.__request_sync(
            "HkStockWeekHistory",
            {"code": code}
        )

    def get_hk_stock_month_history(self, code: str) -> list or None:
        """
        获取港股个股月线数据
        :return:
        """
        return self.__request_sync(
            "HkStockMonthHistory",
            {"code": code}
        )

    def get_hk_market_history(self, code: str) -> list or None:
        """
        获取港股指数日线数据
        :return:
        """
        return self.__request_sync(
            "HkMarketDayHistory",
            {"code": code}
        )

    def get_hk_market_week_history(self, code: str) -> list or None:
        """
        获取港股指数周线数据
        :return:
        """
        return self.__request_sync(
            "HkMarketWeekHistory",
            {"code": code}
        )

    def get_hk_market_month_history(self, code: str) -> list or None:
        """
        获取港股指数月线数据
        :return:
        """
        return self.__request_sync(
            "HkMarketMonthHistory",
            {"code": code}
        )

    def get_ch_stock_back_day_history(self, code: str) -> list or None:
        """
        获取A股个股前复权历史日线
        :param code: 个股代码
        :return: list，字段信息如下
            close            // 收盘价
            open             // 开盘价
            high             // 最高价
            low              // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
        """
        return self.__request_sync(
            "ChStockBackDayHistory",
            {"code": code}
        )

    def get_ch_stock_back_week_history(self, code: str) -> list or None:
        """
        获取个股后复权周线数据
        :param code: 个股代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChStockBackWeekHistory",
            {"code": code}
        )

    def get_ch_stock_back_month_history(self, code: str) -> list or None:
        """
        获取个股后复权周线数据
        :param code: 个股代码
        :return: list，字段信息如下
            close            // 收盘价
            open            // 开盘价
            high             // 最高价
            low             // 最低价
            last_close       // 昨收价
            volume           // 成交量(股)
            amount           // 成交额(万元)
            date            // 日期
            trading_days     // 周期内交易天数
        """
        return self.__request_sync(
            "ChStockBackMonthHistory",
            {"code": code}
        )

    def get_ch_stock_primer_info(self, code: str) -> dict or None:
        """
        查询个股主力及相关评分信息
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockPrimerInfo",
            {"code": code}
        )

    def get_ch_stock_fund_flow(self, code: str) -> dict or None:
        """
        获取个股历史资金流
        :param code:
        :return:
        """
        return self.__request_sync(
            "ChStockFundFlow",
            {"code": code}
        )

    def get_lhb_data(self, date: str) -> dict or None:
        """
        获取指定日期龙虎榜数据
        :param date:
        :return:
        """
        return self.__request_sync(
            "ChLhbData",
            {"date": date}
        )

    def get_ch_market_fund_flow(self) -> dict or None:
        """
        获取市场历史资金流
        :return:
        """
        return self.__request_sync(
            "ChMarketFundFlow",
        )

    def get_ch_stock_ddx_history(self, code: str) -> dict or None:
        """
        获取A股市场个股DDX,DDY等level2数据历史
        code: 个股代码
        :return:
        """
        return self.__request_sync(
            "ChStockDdxHistory",
            {"code": code}
        )

    def get_ch_stock_st_history(self, date: str) -> dict or None:
        """
        获取A股市场个历史交易日ST个股列表
        date: 交易日日期
        :return:
        """
        return self.__request_sync(
            "ChStStockHistory",
            {"date": date}
        )

    def get_hk_stock_main_fin_data(self, code: str) -> list or None:
        """
        获取港股个股主要财务指标
        code: 个股代码
        :return:
        """
        return self.__request_sync(
            "HkStockMainFinData",
            {"code": code}
        )

    def get_hk_stock_balance_sheet(self, code: str) -> list or None:
        """
        获取港股个股资产负债表
        code: 个股代码
        :return:
        """
        return self.__request_sync(
            "HkStockBalance",
            {"code": code}
        )

    def get_hk_stock_profit_statement(self, code: str) -> list or None:
        """
        获取港股个股利润表
        code: 个股代码
        :return:
        """
        return self.__request_sync(
            "HkStockProfitStatement",
            {"code": code}
        )

    def get_hk_stock_cash_flow(self, code: str) -> list or None:
        """
        获取港股个股
        code: 个股代码
        :return:
        """
        return self.__request_sync(
            "HkStockCashFlow",
            {"code": code}
        )

    def get_ch_stock_real(self) -> dict or None:
        """
        获取A股所有个股实时行情
        :return:
        """
        return self.__request_sync("ChStockCurReal")

    def get_ch_market_real(self) -> dict or None:
        """
        获取A股指数当前行情
        :return:
        """
        return self.__request_sync("ChMarketCurReal")

    def get_ch_concept_real(self) -> dict or None:
        """
        获取A股概念板块实时行情
        :return:
        """
        return self.__request_sync("ChConceptCurReal")

    def get_ch_industry_real(self) -> dict or None:
        """
        获取A股行业板块实时行情
        :return:
        """
        return self.__request_sync("ChIndustryCurReal")

    def get_hk_market_real(self) -> list or None:
        """
        获取港股指数实时行情
        :return:
        """
        return self.__request_sync("hkMarketReal")

    def get_hk_stock_real(self) -> dict or None:
        """
        获取港股个股实时行情
        :return:
        """
        return self.__request_sync("hkStockReal")

    def get_ch_l2_data_cur_real(self) -> dict or None:
        """
        获取A股l2实时数据
        :return:
        """
        return self.__request_sync("chL2DataReal")

    def get_ch_ddx_data_cur_real(self) -> dict or None:
        """
        获取A股ddx 实时数据
        :return:
        """
        return self.__request_sync("chDdxDataReal")

    def get_ch_kzz_cur_real(self) -> dict or None:
        """
        获取可转战当前实时数据
        :return:
        """
        return self.__request_sync("chKzzCurReal")

    def get_ch_kzz_stock(self) -> list or None:
        """
        获取可转债列表
        :return:
        """
        return self.__request_sync("chKzzStockList")

    def get_ch_future_cur_real(self) -> dict or None:
        """
        获取国内股指期货，商品期货实时行情
        :return:
        """
        return self.__request_sync("futureCurReal")

    def get_ch_stock_net_profit(self) -> dict or None:
        """
        获取A股，个股业绩预告
        :return:
        """
        return self.__request_sync("chStockNetProfit")

    def get_ch_select_stock(self) -> dict or None:
        """
        获取量化早盘选股结果
        :return:
        """
        return self.__request_sync("mainSelStock")

    def get_ch_end_select_stock(self) -> dict or None:
        """
        获取量化尾盘选股结果
        :return:
        """
        return self.__request_sync("mainEndSelStock")

    def get_ch_after_select_stock(self) -> dict or None:
        """
        获取量化盘后选股结果
        :return:
        """
        return self.__request_sync("mainAfterSelStock")

    def get_ch_stock_ddx_data(self, code: str) -> dict or None:
        """
        查询某只个股的DD数据
        :return:
        """
        return self.__request_sync("chDdxStockData", {'code': code})

    def get_ch_stock_l2_data(self, code: str) -> dict or None:
        """
        查询某只个股的l2数据
        :return:
        """
        return self.__request_sync("chStockL2Data", {'code': code})

    def get_ch_hs300_constituent_weight_history(self, date: str) -> dict or None:
        """
        获取沪深300 成分股历史权重信息。数据范围从 2015年1月到现在
        :param date: 个股是 2026-01
        :return:
        """
        return self.__request_sync("hs300Weight", {'date': date})

    def get_ch_sz50_constituent_weight_history(self, date: str) -> dict or None:
        """
        获取上证50 成分股历史权重信息。数据范围从 2015年1月到现在
        :param date: 个股是 2026-01
        :return:
        """
        return self.__request_sync("sz50Weight", {'date': date})

    def get_ch_zz500_constituent_weight_history(self, date: str) -> dict or None:
        """
        获取中证500 成分股历史权重信息。数据范围从 2015年1月到现在
        :param date: 个股是 2026-01
        :return:
        """
        return self.__request_sync("zz500Weight", {'date': date})

    def get_ch_zz1000_constituent_weight_history(self, date: str) -> dict or None:
        """
        获取中证1000 成分股历史权重信息。数据范围从 2015年1月到现在
        :param date: 个股是 2026-01
        :return:
        """
        return self.__request_sync("zz1000Weight", {'date': date})

    def get_ch_stock_dividend_history(self, code: str) -> dict or None:
        """
        获取A股，个股除权除息历史信息
        :param code: 个股代码
        :return:
        """
        return self.__request_sync("stockDividendData", {'code': code})

    def get_ch_year_high_stock_dividend(self, year: str) -> dict or None:
        """
        获取A股， 自然年报告期所有高送转，派息个股列表
        :param year: 年份
        :return:
        """
        return self.__request_sync("chYearHighStockDividend", {'year': year})

    def get_ch_stock_share_capital(self, code: str) -> dict or None:
        """
        获取A股个股股本变化情况
        :param code: 个股代码
        :return:
        """
        return self.__request_sync("chStockShareData", {'code': code})

    def get_ch_stock_fund_flow_detail_history(self, code: str) -> dict or None:
        """
        获取A股个股资金流详细历史数据
        :param code: 个股代码
        :return:
        """
        return self.__request_sync("chStockFundFlowHistory", {'code': code})

    def get_ch_year_stock_lock_up(self, year: str) -> list or None:
        """
        获取A股自然年内个股限售解禁数据，数据从2006年到2035年
        :param year: 年
        :return:
        """
        return self.__request_sync("chStockLockUp", {'year': year})

    def get_ch_rz_buy_1_day(self) -> list or None:
        """
        获取近1日个股融资买入信息
        :return:
        """
        return self.__request_sync("chRzBuy1Day")

    def get_ch_rz_buy_5_day(self) -> list or None:
        """
        获取近5日个股融资买入信息
        :return:
        """
        return self.__request_sync("chRzBuy5Day")

    def get_ch_rz_buy_20_day(self) -> list or None:
        """
        获取近20日个股融资买入信息
        :return:
        """
        return self.__request_sync("chRzBuy20Day")

    def get_ch_stock_front_ratio_history(self, code: str) -> list or None:
        """
        获取A股等比前复权日线
        :return:
        """
        return self.__request_sync("chStockFrontRatioHistory", {"code": code})

    def get_ch_stock_front_ratio_week_history(self, code: str) -> list or None:
        """
        获取A股等比前复权周线数据
        :return:
        """
        return self.__request_sync("chStockFrontRatioWeekHistory", {"code": code})

    def get_ch_stock_front_ratio_month_history(self, code: str) -> list or None:
        """
        获取A股等比前复权月线数据
        :return:
        """
        return self.__request_sync("chStockFrontRatioMonthHistory", {"code": code})

    def get_ch_stock_back_ratio_history(self, code: str) -> list or None:
        """
        获取A股等比后复权日线
        :return:
        """
        return self.__request_sync("chStockBackRatioHistory", {"code": code})

    def get_ch_stock_back_ratio_week_history(self, code: str) -> list or None:
        """
        获取A股等比后复权周线
        :return:
        """
        return self.__request_sync("chStockBackRatioWeekHistory", {"code": code})

    def get_ch_stock_back_ratio_month_history(self, code: str) -> list or None:
        """
        获取A股等比后复权月线
        :return:
        """
        return self.__request_sync("chStockBackMonthRatioHistory", {"code": code})

    def get_ch_stock_ten_holders(self, code: str) -> list or None:
        """
        获取个股十大股东历史数据
        :return:
        """
        return self.__request_sync("chStockTenHolders", {"code": code})

    def get_ch_stock_ten_circulating_holders(self, code: str) -> list or None:
        """
        获取个股十大流通股东历史数据
        :return:
        """
        return self.__request_sync("chStockTenCirculatingHolders", {"code": code})

    def get_ch_stock_pacs(self, code: str) -> dict or None:
        """
        获取A股个股一致行动人信息
        :return:
        """
        return self.__request_sync("chStockPacsData", {"code": code})

    def get_ch_si_stock_fin_key_indicators(self, code: str) -> list or None:
        """
        从SI 数据源获取个股财务核心指标
        :return:
        """
        return self.__request_sync("chSiStockFinKeyIndicators", {"code": code})

    def get_ch_si_stock_fin_balance_sheet(self, code: str) -> list or None:
        """
        从SI 数据源获取个股资产负债表
        :return:
        """
        return self.__request_sync("chSiStockFinBalanceSheets", {"code": code})

    def get_ch_si_stock_fin_cash_flow(self, code: str) -> list or None:
        """
        从SI 数据源获取个股现金流表
        :return:
        """
        return self.__request_sync("chSiStockFinCashFlow", {"code": code})

    def get_ch_si_stock_fin_income_statements(self, code: str) -> list or None:
        """
        从SI 数据源获取个股利润表
        :return:
        """
        return self.__request_sync("chSiStockFinIncomeStatements", {"code": code})

    def get_ch_ea_stock_fin_key_indicators(self, code: str) -> list or None:
        """
        从ea 数据源获取个股财务核心指标
        :return:
        """
        return self.__request_sync("chEaStockFinKeyIndicators", {"code": code})

    def get_ch_ea_stock_fin_balance_sheet(self, code: str) -> list or None:
        """
        从ea 数据源获取个股资产负债表
        :return:
        """
        return self.__request_sync("chEaStockFinBalanceSheets", {"code": code})

    def get_ch_ea_stock_fin_cash_flow(self, code: str) -> list or None:
        """
        从ea 数据源获取个股现金流表
        :return:
        """
        return self.__request_sync("chEaStockFinCashFlow", {"code": code})

    def get_ch_ea_stock_fin_income_statements(self, code: str) -> list or None:
        """
        从Ea 数据源获取个股利润表
        :return:
        """
        return self.__request_sync("chEaStockFinIncomeStatements", {"code": code})

    def get_ch_sn_kx(self, date: str) -> dict or None:
        """
        获取A股市场快讯数据
        :return:
        """
        return self.__request_sync("chSnKxNews", {"date": date})

    def get_ch_sa_kx(self, date: str) -> dict or None:
        """
        获取A股市场快讯数据
        :return:
        """
        return self.__request_sync("chSaKxNews", {"date": date})

    def get_ch_day_zd_count_history(self):
        """
        获取沪深京每个交易日涨跌平数量历史
        :return:
        """
        return self.__request_sync("chDayZdHistory")

    def get_ch_sh_day_zd_count_history(self):
        """
        获取上证A股每个交易日涨跌平数量历史
        :return:
        """
        return self.__request_sync("chShDayZdHistory")

    def get_ch_sz_day_zd_count_history(self):
        """
        获取深圳A股每个交易日涨跌平数量历史
        :return:
        """
        return self.__request_sync("chSzDayZdHistory")

    def get_ch_cyb_day_zd_count_history(self):
        """
        获取创业板A股每个交易日涨跌平数量历史
        :return:
        """
        return self.__request_sync("chCybDayZdHistory")

    def get_ch_kcb_day_zd_count_history(self):
        """
        获取科创板A股每个交易日涨跌平数量历史
        :return:
        """
        return self.__request_sync("chKcbDayZdHistory")

    def get_ch_bj_day_zd_count_history(self):
        """
        获取北证A股每个交易日涨跌平数量历史
        :return:
        """
        return self.__request_sync("chBjDayZdHistory")

    def get_ch_week_zd_count_history(self):
        """
        获取沪深京每个交易周涨跌平数量历史
        :return:
        """
        return self.__request_sync("chWeekZdHistory")

    def get_ch_sh_week_zd_count_history(self):
        """
        获取上证A股每个交易周涨跌平数量历史
        :return:
        """
        return self.__request_sync("chShWeekZdHistory")

    def get_ch_sz_week_zd_count_history(self):
        """
        获取深圳A股每个交易周涨跌平数量历史
        :return:
        """
        return self.__request_sync("chSzWeekZdHistory")

    def get_ch_cyb_week_zd_count_history(self):
        """
        获取创业板A股每个交易周涨跌平数量历史
        :return:
        """
        return self.__request_sync("chCybWeekZdHistory")

    def get_ch_kcb_week_zd_count_history(self):
        """
        获取科创板A股每个交易周涨跌平数量历史
        :return:
        """
        return self.__request_sync("chKcbWeekZdHistory")

    def get_ch_bj_week_zd_count_history(self):
        """
        获取北证A股每个交易周涨跌平数量历史
        :return:
        """
        return self.__request_sync("chBjWeekZdHistory")

    def get_ch_month_zd_count_history(self):
        """
        获取沪深京每个交易月涨跌平数量历史
        :return:
        """
        return self.__request_sync("chMonthZdHistory")

    def get_ch_sh_month_zd_count_history(self):
        """
        获取上证A股每个交易月涨跌平数量历史
        :return:
        """
        return self.__request_sync("chShMonthZdHistory")

    def get_ch_sz_month_zd_count_history(self):
        """
        获取深圳A股每个交易月涨跌平数量历史
        :return:
        """
        return self.__request_sync("chSzMonthZdHistory")

    def get_ch_cyb_month_zd_count_history(self):
        """
        获取创业板A股每个交易月涨跌平数量历史
        :return:
        """
        return self.__request_sync("chCybMonthZdHistory")

    def get_ch_kcb_month_zd_count_history(self):
        """
        获取科创板A股每个交易月涨跌平数量历史
        :return:
        """
        return self.__request_sync("chKcbMonthZdHistory")

    def get_ch_bj_month_zd_count_history(self):
        """
        获取北证A股每个交易月涨跌平数量历史
        :return:
        """
        return self.__request_sync("chBjMonthZdHistory")

    def get_ch_stock_thousand_level_order(self, code: str):
        """
        获取A股实时前档行情数据
        :return:
        """
        return self.__request_sync("chThousandLevel", {"code": code})

    def get_ch_stock_l2_fund_flow_sa(self, code: str):
        """
        数据源SA个股level2  实时资金流数据
        :return:
        """
        return self.__request_sync("chStockL2FundFlowSa", {"code": code})

    def get_ch_stock_l2_laster_transactions_sa(self, code: str):
        """
        获取最新 100 条逐笔成交（约最近 1-2 分钟）
        :return:
        """
        return self.__request_sync("chStockLasterTransactionsL2Sa", {"code": code})

    def get_ch_all_market_l2_fund_flow(self):
        """
        获取全市场实时资金流数据
        :return:
        """
        return self.__request_sync("chAllMarketL2FundFlowSa")

    def get_ch_sh_market_l2_fund_flow(self):
        """
        获取上证指数实时资金流数据
        :return:
        """
        return self.__request_sync("chShMarketL2FundFlowSa")

    def get_ch_sz_market_l2_fund_flow(self):
        """
        获取深圳综指实时资金流数据
        :return:
        """
        return self.__request_sync("chSzMarketL2FundFlowSa")

    def get_ch_cyb_market_l2_fund_flow(self):
        """
        获取创业板实时资金流数据
        :return:
        """
        return self.__request_sync("chCybMarketL2FundFlowSa")

    def get_ch_kcb_market_l2_fund_flow(self):
        """
        获取创业板实时资金流数据
        :return:
        """
        return self.__request_sync("chKcbMarketL2FundFlowSa")

    def get_ch_all_market_bear_compare(self):
        """
        全市场买一，卖一总量对比
        :return:
        """
        return self.__request_sync("chAllMarketBearCompare")

    def get_ch_sh_market_bear_compare(self):
        """
        上证买一，卖一总量对比
        :return:
        """
        return self.__request_sync("chShMarketBearCompare")

    def get_ch_sz_market_bear_compare(self):
        """
        深证买一，卖一总量对比
        :return:
        """
        return self.__request_sync("chSzMarketBearCompare")

    def get_ch_cyb_market_bear_compare(self):
        """
        创业板买一，卖一总量对比
        :return:
        """
        return self.__request_sync("chCybMarketBearCompare")

    def get_ch_kcb_market_bear_compare(self):
        """
        科创板买一，卖一总量对比
        :return:
        """
        return self.__request_sync("chKcbMarketBearCompare")

    def get_ch_bj_market_bear_compare(self):
        """
        北证买一，卖一总量对比
        :return:
        """
        return self.__request_sync("chBjMarketBearCompare")

    def get_auction_t1_result(self):
        """
         早盘竞价T+1持股周期量化策略筛选结果
        :return:
        """
        return self.__request_sync("chAuctionT1Result")

    def get_auction_t5_result(self):
        """
        早盘竞价T+5持股周期量化策略筛选结果
        :return:
        """
        return self.__request_sync("chAuctionT5Result")

    def get_late_result(self):
        """
        尾盘量化策略筛选结果
        :return:
        """
        return self.__request_sync("chLateResult")

    def get_post_t1_result(self):
        """
        盘后T+1持股周期量化策略筛选结果
        :return:
        """
        return self.__request_sync("chPostT1Result")

    def get_post_t5_result(self):
        """
        盘后T+5持股周期量化策略筛选结果
        :return:
        """
        return self.__request_sync("chPostT5Result")

    def get_stock_price_model_res(self):
        """
        早盘竞价后参参考价计算模型计算结果
        :return:
        """
        return self.__request_sync("chPriceResult")

    def get_auction_t1_result_history(self):
        """
        早盘竞价T+1持股周期量化策略筛选结果历史
        :return:
        """
        return self.__request_sync("chAuctionT1ResHistory")

    def get_auction_t5_result_history(self):
        """
        早盘竞价T+5持股周期量化策略筛选结果历史
        :return:
        """
        return self.__request_sync("chAuctionT5ResHistory")

    def get_late_result_history(self):
        """
        尾盘量化策略筛选结果历史
        :return:
        """
        return self.__request_sync("chLateResHistory")

    def get_post_t1_result_history(self):
        """
        盘后T+1持股周期量化策略筛选结果历史
        :return:
        """
        return self.__request_sync("chPostT1ResHistory")

    def get_post_t5_result_history(self):
        """
        盘后T+5持股周期量化策略筛选结果历史
        :return:
        """
        return self.__request_sync("chPostT5ResHistory")

    def get_ch_stock_big_order(self, code):
        """
        获取指定个股盘中大单数据
        :return:
        """
        return self.__request_sync("chStockBigOrderL2Sa", {"code": code})

    def get_ch_electricity_use_history(self):
        """
        中国宏观经济，全社会用电量同比
        :return:
        """
        return self.__request_sync("chElectricityUseHistory")

    def get_ch_market_pe_pb_month_history(self):
        """
        获取市场PE，PB 历史数据，按月统计
        :return:
        """
        return self.__request_sync("chMarketMonthPeHistory")

    def get_ch_market_pe_pb_day_history(self):
        """
        获取总体市场PE，PB 历史数据，按日统计
        :return:
        """
        return self.__request_sync("chMarketDayPeHistory")

    def get_ch_stock_price_summarize(self, code):
        """
        获取指定个股盘中分价
        :return:
        """
        return self.__request_sync("chStockPriceSummarizeL2Sa", {"code": code})

    def get_ch_stock_price_summarize_history(self, code, date):
        """
        获取指定个股历史分价表
        :return:
        """
        return self.__request_sync("chStockPriceSummarizeHistory", {"code": code, "date": date})

    def get_ch_stock_dark_rank(self):
        """
        获取个股实时暗盘资金数据
        :return:
        """
        return self.__request_sync("chStockDarkRank")

    def get_ch_stock_dark_rank_history(self, date):
        """
        获取个股历史暗盘资金数据
        :return:
        """
        return self.__request_sync("chStockDarkRankHistory", {'date': date})

    def get_ch_market_zd_map(self):
        """
        获取全市场涨跌分布
        :return:
        """
        return self.__request_sync("chMarketZdMap")

    def get_ch_today_limit_up_down(self):
        """
        获取实时个股涨跌停数量
        :return:
        """
        return self.__request_sync("chLimitUpDown")

    def get_ch_gdp_yearly_growth(self):
        """
        中国年度GDP同比增长率
        :return:
        """
        return self.__request_sync("chGdpYearlyGrowth")

    def get_ch_gdp_quarter_rate(self):
        """
        中国季度GDP同比增长率
        :return:
        """
        return self.__request_sync("chGdpQuarterRate")

    def get_ch_gdp_quarter_value(self):
        """
        中国季度GDP 数据
        :return:
        """
        return self.__request_sync("chGdpQuarterValue")

    def get_ch_limit_up_down_history(self, date: str):
        """
        获取指定交易日个股涨跌停数量历史数据
        :return:
        """
        return self.__request_sync("chLimitUpDownHistory", {"date": date})

    def get_ch_zd_map_history(self, date: str):
        """
        获取指定交易日个股涨跌分布历史数据
        :return:
        """
        return self.__request_sync("chZdMapHistory", {"date": date})

    def get_ch_today_lb_stock(self):
        """
        获取实时涨停股列表
        :return:
        """
        return self.__request_sync("chLbStockData")

    def get_ch_lb_stock_history_history(self, date: str):
        """
        获取指定交易日涨停个股信息
        :return:
        """
        return self.__request_sync("chLbStockHistory", {"date": date})

    def get_ch_market_amount_curve(self):
        """
        获取全市场实时成交额
        :return:
        """
        return self.__request_sync("chMarketAmountCurve")

    def get_ch_market_amount_curve_history(self, date: str):
        """
        获取全市场历史成交额曲线
        :return:
        """
        return self.__request_sync("chMarketAmountCurveHistory", {"date": date})

    def get_ch_gdp_qoq_rate(self):
        """
        中国季度GDP环比增长率
        :return:
        """
        return self.__request_sync("chGdpQOQRate")

    def get_ch_stock_time_line(self, code: str) -> dict:
        """
        获取指定个股的当日分时数据
        :return:
        """
        return self.__request_sync("chStockTimeLine", {"code": code})

    def get_ch_stock_time_line_yes(self, code: str) -> dict:
        """
        获取指定个股上个交易日分时数据
        :return:
        """
        return self.__request_sync("chYesStockTimeLine", {"code": code})

    def get_ch_stock_five_days_time_line(self, code: str) -> dict:
        """
        获取指定个股五日分时数据
        :return:
        """
        return self.__request_sync("chFiveDaysStockTimeLine", {"code": code})

    def get_ch_stock_auction_time_line(self, code: str) -> dict:
        """
        获取指定个股当日竞价分时图数据
        :return:
        """
        return self.__request_sync("chAuctionStockTimeLine", {"code": code})

    def get_ch_stock_ea_dde(self) -> dict:
        """
        获取dde 决策数据
        :return:
        """
        return self.__request_sync("chStockEaDde")

    def get_ch_gdp_year_value(self):
        """
        中国季度GDP年度值
        :return:
        """
        return self.__request_sync("chGdpYearValue")

    def get_hsbc_pmi_final(self):
        """
        汇丰中国制造业采购经理指数终值
        :return:
        """
        return self.__request_sync("hsbcPmiFinal")

    def get_hsbc_pmi_preliminary(self):
        """
        汇丰中国采购经理指数初值
        :return:
        """
        return self.__request_sync("hsbcPmiPreliminary")

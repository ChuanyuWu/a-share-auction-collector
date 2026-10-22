# A股当日竞价与分时成交采集器

Python 3.10+，Windows/Linux 命令行程序。默认沪深自选股，每10秒轮询，SQLite WAL存储，失败重试三次。安装和运行：

```powershell
cd 此文件所在目录
python -m pip install -r requirements.txt
python collector.py --symbols 000001,600519 --interval 10
```

建议交易日前启动并保持电脑联网、不休眠。Ctrl+C停止，重启继续写入同一数据库。程序常驻但只在交易日相关交易时段请求行情。日历接口失败或覆盖不足会停止，不猜测节假日。--once只执行一次，非交易时段不会请求行情。

数据位于data/market.sqlite3；日志位于data/collector.log。
- quotes：盘口原始字段、来源时间、采集时间、竞价/连续交易阶段、fresh/stale_or_unverified状态。分析时必须筛选fresh。
- detail_batches：每次完整成交明细响应，保留原始顺序与重复行。
- details：按原始行与同批出现次数做尽力去重。字段通常为时间、价格、手数、来源附加字段、买卖方向；没有交易所序号，不能保证跨窗口去重或完整性。日期依靠同轮新鲜行情推断，明确标为未核验。

竞价数据是定时盘口快照，不是完整竞价委托流；公开源可能不提供虚拟匹配价、未匹配量。竞价结果不能单凭最新价认定。成交接口最多请求最近2000条，网络中断或高成交股票可能丢失，不能盘后补全全天或宣称真实逐笔。若需要交易所完整逐笔成交/委托、订单序号和可靠补漏，需要另接已授权Level-2服务；本版本未实现该接入。大规模全市场不适合逐股轮询，应使用授权批量/推送接口。

当前交付经过离线时段边界与重复行保留测试，尚未进行交易时段接口实测；公共接口可变更，首次交易日请观察日志、原始字段和来源时间。

参考研究（本项目独立编写，未复制仓库代码）：
- https://github.com/tiantianlaolao/astock-data-toolkit ：竞价采集、旧快照校验思路
- https://github.com/YangSal/ashares_data_collect ：调度与采集架构
- https://github.com/yuandanmiao/Ashare_xapi ：区分快照、成交和委托存储
- https://github.com/T3rry7f/A-share-replay ：复盘方向；本次未核实其实现细节
- https://github.com/akfamily/akshare ：交易日日历、公开成交接口字段参考

## 数据来源与定时运行

- 盘口/竞价采样：东方财富公开 `https://push2.eastmoney.com/api/qt/stock/get` 接口，程序直接使用 requests 请求。
- 分时成交明细：东方财富公开 `https://push2.eastmoney.com/api/qt/stock/details/get` 接口；不是交易所 Level-2 真实逐笔。
- 交易日：AKShare `tool_trade_date_hist_sina()`，来源为新浪交易日日历。
- 无需行情账号或密钥，不提供完整性或服务稳定性保证。

定时任务使用 `--scheduled` 参数：非交易日退出，北京时间15:05结束；Windows使用文件锁避免重复运行。当前桌面任务每天9:10启动，要求用户已登录，允许锁屏；关机无法执行。

测试：在项目根目录执行 `python -m unittest discover -s . -v`。测试临时数据库创建在当前目录，测试结束自动清理。

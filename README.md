# A股行情采集与数据整理工具

本机中文网页：管理沪深自选股、控制采集、查看原始和规范化数据、导出CSV/Parquet。**公开成交明细，非完整交易所逐笔。** 不提供策略或指标计算。

## 启动

Python 3.10+：

```powershell
python -m pip install -r requirements.txt
python app.py --serve --open
```

浏览器访问 http://127.0.0.1:8765 ，点击“启动采集”。服务只监听本机，拒绝外部Host/Origin，不向局域网公开。端口被其他程序占用时需先解决冲突。

网页股票区支持粘贴六位代码、逐项删除和导入带 `symbol` 列的UTF-8 CSV：

```csv
symbol
000001
600519
300750
```

添加后点击“保存配置”，下一轮生效。去重并反馈不支持代码；仅检查支持的代码格式，不保证标的存在或仍上市。配置保存到 `data/config.json`，不纳入Git。

命令行共享配置（带symbols/interval参数时会保存）：

```powershell
python collector.py --symbols 000001,600519 --interval 10
python collector.py --once
python app.py --serve --scheduled
```

`--scheduled` 在服务启动时自动采集，非交易日结束采集，15:05停止采集，网页保持运行。每日Windows任务仍需调用此命令，已运行的服务会收到启动采集请求。服务和采集均有进程锁。关机无法采集，未登录无法运行当前桌面任务，锁屏可运行。启动时日历失败会每60秒重试。

## 数据来源与局限

- 东方财富公开 `https://push2.eastmoney.com/api/qt/stock/get`：盘口快照。按来源时间检查当天及90秒新鲜度。
- 东方财富公开 `https://push2.eastmoney.com/api/qt/stock/details/get`：最多最近2000条公开明细。时间不含交易日期或交易所序号，日期按采集日推断，去重仅尽力保留同值出现次数。
- AKShare `tool_trade_date_hist_sina()`：新浪交易日日历。覆盖不足时不采集。
- `provider.py` 为数据源适配层，分离盘口、明细及日历；未来可接入有权限的Level-2源，当前无需账号或密钥。

竞价期间接口也会返回明细，**不能认定为实际成交**，按来源时间单独归类并标记 `auction_semantics_unverified`。不作为成交量或VWAP依据。连续交易记录仍为公开源明细，不保证真实逐笔或完整性。停牌、网络中断、高成交量和窗口截断均可能造成缺失。开盘集合竞价成交价、虚拟撮合量、未匹配量未核验时不展示为已知结果。

数量保留 `quantity_raw`，单位未核验，不自动乘100或计算成交额。方向代码保留原值，1/2/4仅展示供应商标签，未知代码不猜测。阶段按来源时刻分类，不根据首次采集时间混入连续交易。

## 浏览、导出与后续量化

页面按日期、股票、阶段、质量筛选，100条分页；每行可展开原始响应。盘口来源时间、采集时间、延迟和质量独立显示。运行概览包括每股记录数、最后落盘，事件记录成功、失败、陈旧、超时段间隔与窗口上限；这些提示不构成完整性认证。

- CSV：UTF-8 BOM、稳定英文列名；Excel导入时将symbol设为文本以保留前导零。
- Parquet：列类型固定，附数据字典metadata，适合pandas、DuckDB。
- 字段字典：`/api/dictionary`；价格单位元，时间UTC+8，日期推断、数据等级和完整性字段随每条导出。
- 只导出当前筛选结果；导出单次数据库读取快照，当前版本将结果载入内存，超大数据建议按日期/股票分批。

```python
import pandas as pd
records = pd.read_parquet('details.parquet')
continuous = records[records['phase'].eq('continuous')]
# 先审核数量单位、质量与采集缺口，再计算任何成交指标。
```

## 存储、升级和备份

`data/market.sqlite3` 保存原始quotes、details、detail_batches及采集events。规范化查询不覆盖原始行；旧竞价记录查询/导出时重新标注未核验。首次升级至v2自动使用SQLite备份接口生成 `backup-before-v2-*.sqlite3`，添加索引和事件表，不删除原记录。

网页“下载一致性数据库备份”使用SQLite备份接口，包含已提交WAL数据。不要只拷贝运行中的主数据库或删除-wal/-shm。数据、日志、配置和备份不上传GitHub。采集错误写入events，启动与异常日志写入 `data/web.log`。

网络支持系统代理/直连，默认系统代理；测试按钮使用当前选择但保存后才影响采集。每次请求连接超时5秒、读超时15秒，最多重试三次并退避，单股失败继续其他股票。停止等待在途请求结束，不立即强杀。

## 验证

```powershell
python -m unittest discover -s . -v
```

测试覆盖代码导入、共享配置、本机访问限制、旧库备份迁移、竞价隔离、未知方向、固定响应去重、连接失败恢复、文件锁和CSV/Parquet导出。

交易时段验收应核对网页最后成功时间、数据库新记录、导出数量一致；竞价和收盘尚未在新版中现场观察时不得宣称已验证。数据源接口可能变更。

参考研究（独立编写，未复制仓库代码）：
- https://github.com/tiantianlaolao/astock-data-toolkit
- https://github.com/YangSal/ashares_data_collect
- https://github.com/yuandanmiao/Ashare_xapi
- https://github.com/T3rry7f/A-share-replay （未核实实现细节）
- https://github.com/akfamily/akshare

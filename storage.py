"""Configuration, additive migration and normalized reads."""
import csv, datetime as dt, io, json, math, os, re, sqlite3, threading
from pathlib import Path
from collector import connect, phase, TZ

DEFAULT = {'symbols':['000001','600519'], 'interval':10, 'network':'system'}
COLUMNS = {
 'day':'交易日期（明细日期推断）','symbol':'六位股票代码','source_time':'来源时间，UTC+8；明细不含日期，按采集日推断',
 'received':'首次采集时间，UTC+8','phase':'按来源时刻分类','price':'价格，元','quantity_raw':'来源数量，未核验单位，不自动换算股数',
 'direction_code':'来源买卖方向代码','direction':'来源方向标签，不代表真实主动买卖识别',
 'quality':'语义及去重质量标记','freshness':'盘口新鲜度；明细独立日期未核验','provider':'供应商','data_level':'公开源，非交易所真实逐笔',
 'occurrence':'同批同值出现序号，尽力去重','raw':'原始响应/明细行','delay_seconds':'盘口来源时间至采集时间秒数','completeness':'完整性不可保证'}

class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try: return super().__exit__(*args)
        finally: self.close()

class Store:
    def __init__(self, root):
        self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True)
        self.path=self.root/'market.sqlite3'; self.config_path=self.root/'config.json'
        self.config_lock=threading.Lock()
        existing=self.path.exists()
        db=connect(self.path)
        version=db.execute('PRAGMA user_version').fetchone()[0]
        if existing and version < 2:
            target=self.root/('backup-before-v2-'+dt.datetime.now().strftime('%Y%m%d-%H%M%S-%f')+'.sqlite3')
            with sqlite3.connect(target, factory=ClosingConnection) as backup: db.backup(backup)
        db.executescript('''
          CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,received TEXT,symbol TEXT,kind TEXT,message TEXT);
          CREATE INDEX IF NOT EXISTS quote_filter ON quotes(day,symbol,phase);
          CREATE INDEX IF NOT EXISTS detail_filter ON details(day,symbol);
          PRAGMA user_version=2;
        ''')
        db.close()
        if not self.config_path.exists(): self.save_config(DEFAULT)
    def db(self):
        db=sqlite3.connect(self.path,timeout=15,factory=ClosingConnection); db.row_factory=sqlite3.Row
        return db
    def config(self):
        with self.config_lock: return json.loads(self.config_path.read_text(encoding='utf-8'))
    def save_config(self, value):
        symbols,invalid=parse_symbols(value.get('symbols',[]))
        if invalid or not symbols: raise ValueError('股票代码无效或为空: '+','.join(invalid))
        interval=float(value.get('interval',10))
        if not math.isfinite(interval) or not 3 <= interval <= 3600: raise ValueError('采集间隔必须为3至3600秒')
        network=value.get('network','system')
        if network not in ('system','direct'): raise ValueError('网络方式无效')
        result={'symbols':symbols,'interval':interval,'network':network}
        with self.config_lock:
            tmp=self.config_path.with_suffix('.tmp'); tmp.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8'); os.replace(tmp,self.config_path)
        return result
    def event(self,symbol,kind,message):
        with self.db() as db: db.execute('INSERT INTO events(received,symbol,kind,message) VALUES(?,?,?,?)',(dt.datetime.now(TZ).isoformat(),symbol,kind,str(message)[:1500]))
    def backup(self):
        path=self.root/('backup-'+dt.datetime.now().strftime('%Y%m%d-%H%M%S-%f')+'.sqlite3')
        with self.db() as db, sqlite3.connect(path,factory=ClosingConnection) as out: db.backup(out)
        return path
    def rows(self,kind='details',day='',symbol='',stage='',quality='',limit=100,offset=0):
        if kind not in ('quotes','details'): raise ValueError('未知数据类型')
        filters=[]; args=[]
        if day: filters.append('day=?'); args.append(day)
        if symbol: filters.append('symbol=?'); args.append(symbol)
        if kind=='quotes':
            phase_expr='phase'; quality_expr='status'
        else:
            clock="substr(raw,1,8)"
            phase_expr=f"CASE WHEN {clock} >= '09:15:00' AND {clock} <= '09:25:30' THEN 'opening_auction' WHEN {clock} >= '14:57:00' AND {clock} <= '15:00:30' THEN 'closing_auction' WHEN ({clock} >= '09:30:00' AND {clock} <= '11:30:05') OR ({clock} >= '13:00:00' AND {clock} < '14:57:00') THEN 'continuous' ELSE 'unknown' END"
            quality_expr=f"CASE WHEN ({phase_expr}) IN ('opening_auction','closing_auction') THEN 'auction_semantics_unverified' WHEN ({phase_expr})='continuous' THEN 'public_detail_date_inferred_sequence_unavailable' ELSE 'unclassified_semantics_unverified' END"
        if stage: filters.append(f'({phase_expr})=?'); args.append(stage)
        if quality: filters.append(f'({quality_expr})=?'); args.append(quality)
        where=' WHERE '+' AND '.join(filters) if filters else ''
        with self.db() as db:
            db.execute('BEGIN')
            total=db.execute('SELECT count(*) FROM '+kind+where,args).fetchone()[0]
            query='SELECT * FROM '+kind+where+' ORDER BY rowid DESC'
            if limit is not None: query+=' LIMIT ? OFFSET ?'; args += [limit,offset]
            values=[normalize(dict(r),kind) for r in db.execute(query,args)]
        return {'total':total,'rows':values}

def parse_symbols(value):
    values=re.split(r'[\s,;，；]+',value.strip()) if isinstance(value,str) else value
    good=[];bad=[]
    for item in values:
        s=str(item).strip()
        if not s: continue
        if re.fullmatch(r'(600|601|603|605|688|000|001|002|003|300|301)\d{3}',s):
            if s not in good: good.append(s)
        elif s not in bad: bad.append(s)
    return good,bad

def import_csv(text):
    reader=csv.DictReader(io.StringIO(text.lstrip('\ufeff')))
    if not reader.fieldnames or 'symbol' not in reader.fieldnames: raise ValueError('CSV必须含symbol列；代码须保留前导零')
    return parse_symbols([r['symbol'] for r in reader])

def number(s):
    try:
        value=float(s)
        return value if math.isfinite(value) else None
    except (ValueError,TypeError): return None

def normalize(row,kind):
    result=dict.fromkeys(COLUMNS)
    result.update(day=row['day'],symbol=row['symbol'],provider='eastmoney',data_level='public_detail_not_exchange_tick',raw=row['raw'],completeness='not_guaranteed')
    if kind=='quotes':
        raw=json.loads(row['raw']); result.update(source_time=row['source_time'],received=row['received'],phase=row['phase'],price=number(raw.get('f43')),quality=row['status'],freshness=row['status'])
        if row['source_time']:
            result['delay_seconds']=(dt.datetime.fromisoformat(row['received'])-dt.datetime.fromisoformat(row['source_time'])).total_seconds()
    else:
        parts=row['raw'].split(','); clock=parts[0]
        try:
            source=dt.datetime.fromisoformat(row['day']+'T'+clock).replace(tzinfo=TZ)
            stage=phase(source) or 'unknown'; source_time=source.isoformat()
        except ValueError: stage='unknown'; source_time=None
        quality='auction_semantics_unverified' if 'auction' in stage else ('public_detail_date_inferred_sequence_unavailable' if stage=='continuous' else 'unclassified_semantics_unverified')
        code=parts[4] if len(parts)>4 else None
        result.update(source_time=source_time,received=row['first_seen'],phase=stage,price=number(parts[1]) if len(parts)>1 else None,quantity_raw=number(parts[2]) if len(parts)>2 else None,direction_code=code,direction={'1':'卖盘（来源标签）','2':'买盘（来源标签）','4':'中性（来源标签）'}.get(code,'未知'),quality=quality,freshness='detail_date_unverified',occurrence=row['occurrence'])
    return result

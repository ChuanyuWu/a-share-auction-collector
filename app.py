"""Local-only control service and collection worker."""
import argparse, collections, datetime as dt, io, json, logging, os, socket, threading, time
from contextlib import asynccontextmanager
from pathlib import Path
import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, FileResponse
from collector import phase, TZ
from provider import Eastmoney, fresh
from storage import Store, COLUMNS, parse_symbols, import_csv

BASE=Path(__file__).resolve().parent

class ProcessLock:
    def __init__(self,path): self.path=path; self.file=None
    def acquire(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        if not self.path.exists(): self.path.touch()
        self.file=open(self.path,'r+b'); self.file.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return True
        except OSError: self.file.close(); self.file=None; return False
    def close(self):
        if self.file: self.file.close(); self.file=None

class Worker:
    def __init__(self,store):
        self.store=store; self.stop_event=threading.Event(); self.thread=None; self.guard=threading.Lock()
        self.state='stopped'; self.error=''; self.last_success={}; self.last_poll={}; self.calendar=None
    def start(self):
        with self.guard:
            if self.thread and self.thread.is_alive(): return False
            self.stop_event.clear(); self.state='starting'
            self.thread=threading.Thread(target=self.run,daemon=True); self.thread.start(); return True
    def stop(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive(): self.state='stopping'
    def cycle(self,provider,symbol):
        now=dt.datetime.now(TZ); stage=phase(now)
        if not stage: return
        started=time.monotonic(); raw=provider.quote(symbol); received=dt.datetime.now(TZ)
        source,delay,valid=fresh(raw,received)
        with self.store.db() as db:
            db.execute('INSERT INTO quotes VALUES(?,?,?,?,?,?,?)',(now.date().isoformat(),symbol,received.isoformat(),source,stage,'fresh' if valid else 'stale_or_unverified',json.dumps(raw,ensure_ascii=False)))
        if not valid:
            self.store.event(symbol,'stale','来源时间陈旧或未核验；跳过明细'); return
        detail=provider.details(symbol); received=dt.datetime.now(TZ)
        if received.date()!=now.date(): self.store.event(symbol,'gap','响应跨日，未写入明细'); return
        counts=collections.Counter(detail.get('details') or [])
        with self.store.db() as db:
            db.execute('INSERT INTO detail_batches VALUES(?,?,?,?)',(now.date().isoformat(),symbol,received.isoformat(),json.dumps(detail,ensure_ascii=False)))
            for row,count in counts.items():
                for occurrence in range(count):
                    quality='public_detail_date_inferred_sequence_unavailable'
                    if row[:8] < '09:30:00' or row[:8] >= '14:57:00': quality='auction_semantics_unverified'
                    db.execute('INSERT OR IGNORE INTO details VALUES(?,?,?,?,?,?)',(now.date().isoformat(),symbol,row,occurrence,received.isoformat(),quality))
        previous=self.last_poll.get(symbol)
        interval=self.store.config()['interval']
        if previous and previous['phase']==stage and (received-dt.datetime.fromisoformat(previous['received'])).total_seconds() > max(30,interval*3):
            self.store.event(symbol,'gap','采集间隔超出预期；可能存在缺口，不代表可补齐')
        if len(detail.get('details') or [])>=2000: self.store.event(symbol,'window_limit','返回明细达到2000条窗口；完整性未知')
        self.last_poll[symbol]={'received':received.isoformat(),'phase':stage}
        self.last_success[symbol]=received.isoformat()
        self.store.event(symbol,'success',f'保存明细批次 {sum(counts.values())} 行，延迟 {delay:.1f}s，请求耗时 {time.monotonic()-started:.1f}s')
    def run(self):
        lock=ProcessLock(self.store.root/'collector.lock')
        if not lock.acquire(): self.state='blocked'; self.error='已有采集进程占用锁'; return
        provider=None; network=None; calendar_day=None; retry_at=0
        try:
            while not self.stop_event.is_set():
                now=dt.datetime.now(TZ)
                if now.strftime('%H:%M:%S') >= '15:05:00': self.state='finished'; break
                config=self.store.config()
                if network!=config['network']:
                    if provider: provider.close()
                    provider=Eastmoney(config['network']); network=config['network']; self.calendar=None
                if self.calendar is None or calendar_day!=now.date():
                    if time.monotonic()<retry_at: self.stop_event.wait(1); continue
                    try:
                        self.calendar=provider.calendar()
                        if not self.calendar or now.date().isoformat()>max(self.calendar): raise ValueError('交易日日历未覆盖今天')
                        calendar_day=now.date(); self.error=''
                    except Exception as exc:
                        self.state='calendar_error'; self.error=str(exc); self.store.event('', 'error', self.error)
                        retry_at=time.monotonic()+60; self.stop_event.wait(1); continue
                if now.date().isoformat() not in self.calendar: self.state='non_trading_day'; break
                if not phase(now): self.state='waiting'; self.stop_event.wait(1); continue
                self.state='collecting'
                for symbol in config['symbols']:
                    if self.stop_event.is_set() or not phase(dt.datetime.now(TZ)): break
                    try: self.cycle(provider,symbol); self.error=''
                    except Exception as exc:
                        self.error=str(exc); self.store.event(symbol,'error',self.error); logging.exception('collection failed %s',symbol)
                self.stop_event.wait(config['interval'])
        except Exception as exc:
            self.state='error'; self.error=str(exc); self.store.event('', 'error', self.error)
        finally:
            if provider: provider.close()
            lock.close()
            if self.stop_event.is_set(): self.state='stopped'
    def status(self):
        with self.store.db() as db:
            counts=[dict(r) for r in db.execute('SELECT day,symbol,count(*) AS count,max(first_seen) AS last_seen FROM details GROUP BY day,symbol ORDER BY day DESC,symbol LIMIT 100')]
            events=[dict(r) for r in db.execute('SELECT * FROM events ORDER BY id DESC LIMIT 40')]
            quotes=[dict(r) for r in db.execute('SELECT day,symbol,status,count(*) AS count,max(received) AS last_seen FROM quotes GROUP BY day,symbol,status ORDER BY day DESC LIMIT 100')]
        return {'state':self.state,'running':bool(self.thread and self.thread.is_alive()),'error':self.error,'last_success':dict(self.last_success),'last_poll':dict(self.last_poll),'counts':counts,'quotes':quotes,'events':events,'now':dt.datetime.now(TZ).isoformat(),'completeness':'不可保证；明细无交易所序号，日期为推断'}

def create_app(root=BASE/'data',autostart=False):
    store=Store(root); worker=Worker(store)
    scheduler_stop=threading.Event()
    schedule={'enabled':autostart,'last_day':None}
    def supervise():
        while not scheduler_stop.wait(1):
            now=dt.datetime.now(TZ)
            if schedule['enabled'] and now.strftime('%H:%M:%S') >= '09:10:00' and schedule['last_day']!=now.date():
                schedule['last_day']=now.date(); worker.start()
    @asynccontextmanager
    async def lifespan(app):
        if autostart:
            schedule['last_day']=dt.datetime.now(TZ).date(); worker.start()
        supervisor=threading.Thread(target=supervise,daemon=True); supervisor.start()
        yield
        scheduler_stop.set(); supervisor.join(timeout=2)
        worker.stop()
        if worker.thread: worker.thread.join(timeout=5)
    app=FastAPI(lifespan=lifespan); app.state.store=store; app.state.worker=worker
    @app.middleware('http')
    async def local_only(request,call_next):
        host=request.headers.get('host','').split(':')[0]
        if host not in ('127.0.0.1','localhost','testserver'): return Response('Host rejected',status_code=403)
        origin=request.headers.get('origin')
        if origin and origin not in ('http://127.0.0.1:8765','http://localhost:8765','http://testserver'): return Response('Origin rejected',status_code=403)
        return await call_next(request)
    @app.get('/',response_class=HTMLResponse)
    def home(): return (BASE/'index.html').read_text(encoding='utf-8-sig')
    @app.get('/api/config')
    def config(): return store.config()
    @app.post('/api/config')
    async def save(request:Request):
        try: return store.save_config(await request.json())
        except (ValueError,TypeError) as exc: raise HTTPException(400,str(exc))
    @app.post('/api/import')
    async def csv_import(request:Request):
        try:
            value=await request.json(); good,bad=import_csv(value['text'])
            return {'symbols':good,'invalid':bad}
        except (ValueError,KeyError) as exc: raise HTTPException(400,str(exc))
    @app.get('/api/status')
    def status(): return worker.status()
    @app.post('/api/schedule')
    def enable_schedule():
        schedule['enabled']=True; schedule['last_day']=dt.datetime.now(TZ).date()
        return {'started':worker.start(),'daily_schedule':True}
    @app.post('/api/start')
    def start(): return {'started':worker.start()}
    @app.post('/api/stop')
    def stop(): worker.stop(); return {'stopping':True}
    @app.post('/api/network-test')
    async def test_network(request:Request):
        # synchronous provider work goes to a thread so control requests stay responsive
        import asyncio
        value=await request.json(); mode=value.get('network',store.config()['network'])
        if mode not in ('system','direct'): raise HTTPException(400,'网络方式无效')
        def check():
            provider=Eastmoney(mode)
            try:
                raw=provider.quote(store.config()['symbols'][0]); source,delay,valid=fresh(raw,dt.datetime.now(TZ))
                return {'ok':True,'source_time':source,'delay_seconds':delay,'fresh':valid,'note':'连通不代表数据完整；非交易时间陈旧属正常情况'}
            except Exception as exc: return {'ok':False,'error':str(exc)}
            finally: provider.close()
        return await asyncio.to_thread(check)
    @app.get('/api/data')
    def data(kind:str='details',day:str='',symbol:str='',stage:str='',quality:str='',page:int=1,page_size:int=100):
        if page<1 or not 1<=page_size<=500: raise HTTPException(400,'分页参数无效')
        try: return store.rows(kind,day,symbol,stage,quality,page_size,(page-1)*page_size)
        except ValueError as exc: raise HTTPException(400,str(exc))
    @app.get('/api/dictionary')
    def dictionary(): return COLUMNS
    @app.get('/api/export')
    def export(format:str='csv',kind:str='details',day:str='',symbol:str='',stage:str='',quality:str=''):
        if format not in ('csv','parquet'): raise HTTPException(400,'导出格式无效')
        try: rows=store.rows(kind,day,symbol,stage,quality,None)['rows']
        except ValueError as exc: raise HTTPException(400,str(exc))
        import csv
        if format=='csv':
            out=io.StringIO(); writer=csv.DictWriter(out,fieldnames=list(COLUMNS)); writer.writeheader(); writer.writerows(rows)
            payload=out.getvalue().encode('utf-8-sig'); media='text/csv'
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq
            schema=pa.schema([(key,pa.float64() if key in ('price','quantity_raw','delay_seconds') else pa.int64() if key=='occurrence' else pa.string()) for key in COLUMNS],metadata={b'data_dictionary':json.dumps(COLUMNS,ensure_ascii=False).encode('utf-8')})
            out=io.BytesIO(); pq.write_table(pa.Table.from_pylist(rows,schema=schema),out); payload=out.getvalue(); media='application/octet-stream'
        return Response(payload,media_type=media,headers={'Content-Disposition':f'attachment; filename="{kind}.{format}"'})
    @app.post('/api/backup')
    def backup(): return FileResponse(store.backup(),filename='market-backup.sqlite3')
    return app

def cli():
    p=argparse.ArgumentParser(); p.add_argument('--symbols'); p.add_argument('--interval',type=float); p.add_argument('--data-dir',default=str(BASE/'data')); p.add_argument('--scheduled',action='store_true'); p.add_argument('--once',action='store_true'); p.add_argument('--serve',action='store_true'); p.add_argument('--open',action='store_true'); args=p.parse_args()
    store=Store(args.data_dir)
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[logging.FileHandler(store.root/'web.log',encoding='utf-8')])
    if args.symbols or args.interval is not None:
        config=store.config()
        if args.symbols: config['symbols']=parse_symbols(args.symbols)[0]; good,bad=parse_symbols(args.symbols); assert not bad, 'invalid symbols: '+str(bad)
        if args.interval is not None: config['interval']=args.interval
        store.save_config(config)
    if args.serve:
        service_lock=ProcessLock(store.root/'service.lock')
        if not service_lock.acquire():
            if args.scheduled:
                requests.post('http://127.0.0.1:8765/api/schedule', timeout=5)
            if args.open:
                import webbrowser
                webbrowser.open('http://127.0.0.1:8765')
            return
        try:
            import uvicorn
            if args.open:
                import webbrowser
                threading.Timer(2,lambda:webbrowser.open('http://127.0.0.1:8765')).start()
            uvicorn.run(create_app(args.data_dir,args.scheduled),host='127.0.0.1',port=8765,log_config=None)
        finally: service_lock.close()
    else:
        worker=Worker(store)
        if args.once:
            once_lock=ProcessLock(store.root/'collector.lock')
            if not once_lock.acquire(): raise RuntimeError('已有采集进程')
            provider=Eastmoney(store.config()['network'])
            try:
                now=dt.datetime.now(TZ); calendar=provider.calendar()
                if not calendar or now.date().isoformat()>max(calendar): raise RuntimeError('calendar expired')
                if now.date().isoformat() in calendar and phase(now):
                    for symbol in store.config()['symbols']: worker.cycle(provider,symbol)
            finally: provider.close(); once_lock.close()
        else:
            worker.start()
            try:
                while worker.thread and worker.thread.is_alive(): time.sleep(1)
            except KeyboardInterrupt: worker.stop(); worker.thread.join()

if __name__=='__main__': cli()

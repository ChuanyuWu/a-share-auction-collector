import datetime as dt, io, json, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient
import pyarrow.parquet as pq
from app import create_app, Worker, ProcessLock
from storage import Store, parse_symbols, import_csv
from provider import fresh
from collector import TZ, connect

class WebChecks(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir='.'); self.root=Path(self.temp.name)
    def tearDown(self): self.temp.cleanup()
    def seed(self,store):
        with store.db() as db:
            for raw in ['09:15:00,11.30,62,0,4','09:30:01,11.40,12,0,2','09:30:02,11.41,8,0,99']:
                db.execute('INSERT INTO details VALUES(?,?,?,?,?,?)',('2026-10-23','000001',raw,0,'2026-10-23T09:30:03+08:00','legacy'))
    def test_import_and_config(self):
        self.assertEqual(parse_symbols('000001,000001 600519 abc')[0],['000001','600519'])
        self.assertEqual(import_csv('\ufeffsymbol\n000001\n600519\nabc')[1],['abc'])
        with TestClient(create_app(self.root)) as c:
            self.assertEqual(c.post('/api/config',json={'symbols':['bad']}).status_code,400)
            self.assertEqual(c.post('/api/config',json={'symbols':['300750'],'interval':3,'network':'direct'}).status_code,200)
            self.assertEqual(Store(self.root).config()['symbols'],['300750'])
            self.assertEqual(c.post('/api/config',json={'symbols':['000001']},headers={'Origin':'https://evil.example'}).status_code,403)
    def test_migration_and_export(self):
        db=connect(self.root/'market.sqlite3'); db.execute("INSERT INTO details VALUES('2026-10-23','000001','09:15:00,1,2,0,4',0,'2026-10-23T09:15:01+08:00','legacy')");db.commit();db.close()
        app=create_app(self.root); store=app.state.store
        self.assertEqual(len(list(self.root.glob('backup-before-v2-*'))),1)
        self.assertEqual(store.rows()['total'],1)
        self.seed(store)
        with TestClient(app) as c:
            auction=c.get('/api/data?stage=opening_auction').json()
            self.assertEqual(auction['total'],2)
            self.assertTrue(all(r['quality']=='auction_semantics_unverified' for r in auction['rows']))
            self.assertEqual(c.get('/api/data?stage=continuous').json()['total'],2)
            self.assertEqual(c.get('/api/data?symbol=000001&page_size=1').json()['rows'][0]['direction'],'未知')
            csv_result=c.get('/api/export?format=csv&stage=continuous')
            parquet=c.get('/api/export?format=parquet&stage=continuous')
            self.assertEqual(csv_result.status_code,200)
            table=pq.read_table(io.BytesIO(parquet.content)); self.assertEqual(table.num_rows,2)
            self.assertIn(b'data_dictionary',table.schema.metadata)
            self.assertTrue(c.post('/api/backup').content.startswith(b'SQLite format 3'))
            self.assertEqual(c.get('/api/data?kind=evil').status_code,400)
    def test_freshness_and_lock(self):
        now=dt.datetime(2026,10,23,9,30,tzinfo=TZ)
        self.assertTrue(fresh({'f86':now.timestamp()},now)[2])
        self.assertFalse(fresh({'f86':(now-dt.timedelta(days=1)).timestamp()},now)[2])
        self.assertFalse(fresh({'f86':'-'},now)[2])
        a=ProcessLock(self.root/'lock'); b=ProcessLock(self.root/'lock')
        self.assertTrue(a.acquire());self.assertFalse(b.acquire());a.close();self.assertTrue(b.acquire());b.close()
    def test_network_error_and_recovery(self):
        with TestClient(create_app(self.root)) as c:
            with patch('provider.Eastmoney.quote',side_effect=RuntimeError('proxy failed')):
                self.assertFalse(c.post('/api/network-test',json={'network':'system'}).json()['ok'])
            with patch('provider.Eastmoney.quote',return_value={'f86':dt.datetime.now(TZ).timestamp()}):
                self.assertTrue(c.post('/api/network-test',json={'network':'direct'}).json()['ok'])
    def test_cycle_duplicate_and_raw(self):
        store=Store(self.root); worker=Worker(store)
        class Mock:
            def quote(self,symbol): return {'f86':dt.datetime.now(TZ).timestamp(),'f43':11.3}
            def details(self,symbol): return {'details':['09:30:00,11.3,1,0,2']*2}
        with patch('app.phase',return_value='continuous'):
            worker.cycle(Mock(),'000001');worker.cycle(Mock(),'000001')
        self.assertEqual(store.rows()['total'],2)
        with store.db() as db: self.assertEqual(db.execute('select count(*) from detail_batches').fetchone()[0],2)

if __name__=='__main__': unittest.main()

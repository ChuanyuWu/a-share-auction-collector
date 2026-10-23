"""A-share public quote collector. All times Asia/Shanghai (UTC+8)."""
import argparse, collections, datetime as dt, json, logging, sqlite3, time
from pathlib import Path
import requests

TZ = dt.timezone(dt.timedelta(hours=8))

def phase(t):
    s = t.strftime('%H:%M:%S')
    if '09:15:00' <= s <= '09:25:30': return 'opening_auction'
    if '14:57:00' <= s <= '15:00:30': return 'closing_auction'
    if '09:30:00' <= s <= '11:30:05' or '13:00:00' <= s < '14:57:00': return 'continuous'
    return None

def fetch(session, url, params):
    for attempt in range(3):
        try:
            r = session.get(url, params=params, timeout=(5, 15))
            r.raise_for_status()
            value = r.json()
            if not value.get('data'): raise ValueError('empty provider response')
            return value['data']
        except (requests.RequestException, ValueError):
            if attempt == 2: raise
            time.sleep(2 ** attempt)

def connect(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS quotes(day TEXT,symbol TEXT,received TEXT,source_time TEXT,phase TEXT,status TEXT,raw TEXT);
    CREATE TABLE IF NOT EXISTS detail_batches(day TEXT,symbol TEXT,received TEXT,raw TEXT);
    CREATE TABLE IF NOT EXISTS details(day TEXT,symbol TEXT,raw TEXT,occurrence INTEGER,first_seen TEXT,quality TEXT,
      PRIMARY KEY(day,symbol,raw,occurrence));
    ''')
    return db

def collect(db, session, symbol, now, p):
    secid = ('1.' if symbol.startswith('6') else '0.') + symbol
    data = fetch(session, 'https://push2.eastmoney.com/api/qt/stock/get', {
        'secid':secid, 'fltt':'2', 'fields':'f57,f58,f43,f44,f45,f46,f47,f48,f60,f86,' + ','.join('f'+str(i) for i in range(11,41))})
    stamp = data.get('f86')
    source = dt.datetime.fromtimestamp(int(stamp), TZ) if stamp else None
    valid = source is not None and source.date() == now.date() and -5 <= (now-source).total_seconds() <= 90
    status = 'fresh' if valid else 'stale_or_unverified'
    day, received = now.date().isoformat(), now.isoformat()
    with db:
        db.execute('INSERT INTO quotes VALUES(?,?,?,?,?,?,?)', (day,symbol,received,source.isoformat() if source else None,p,status,json.dumps(data,ensure_ascii=False)))
    if not valid:
        logging.warning('%s quote rejected for current-day details: %s',symbol,status)
        return
    detail = fetch(session, 'https://push2.eastmoney.com/api/qt/stock/details/get', {
        'secid':secid,'fields1':'f1,f2,f3,f4','fields2':'f51,f52,f53,f54,f55','fltt':'2','pos':'-2000'})
    rows = detail.get('details', [])
    # Provider supplies no exchange sequence/date. Preserve batches and multiplicity;
    # normalized rows are best-effort, never certified exchange executions.
    counts = collections.Counter(rows)
    with db:
        db.execute('INSERT INTO detail_batches VALUES(?,?,?,?)',(day,symbol,received,json.dumps(detail,ensure_ascii=False)))
        for raw, count in counts.items():
            for occurrence in range(count):
                db.execute('INSERT OR IGNORE INTO details VALUES(?,?,?,?,?,?)',(day,symbol,raw,occurrence,received,'public_detail_date_inferred_sequence_unavailable'))
    logging.info('%s %s saved %d detail rows; quote %s',symbol,p,len(rows),source)

def main():
    from app import cli
    cli()

if __name__ == '__main__': main()

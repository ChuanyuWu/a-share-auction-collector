"""Provider adapter; prices yuan, quantities kept in provider units."""
import datetime as dt
import requests
from collector import fetch, TZ

class Eastmoney:
    name = 'eastmoney'
    level = 'public_detail_not_exchange_tick'
    def __init__(self, network='system'):
        self.session = requests.Session()
        self.session.trust_env = network == 'system'
        self.session.headers['User-Agent'] = 'Mozilla/5.0'
    def quote(self, symbol):
        return fetch(self.session, 'https://push2.eastmoney.com/api/qt/stock/get', {
            'secid': ('1.' if symbol.startswith('6') else '0.')+symbol,
            'fltt':'2','fields':'f57,f58,f43,f44,f45,f46,f47,f48,f60,f86,'+','.join('f'+str(i) for i in range(11,41))})
    def details(self, symbol):
        return fetch(self.session, 'https://push2.eastmoney.com/api/qt/stock/details/get', {
            'secid':('1.' if symbol.startswith('6') else '0.')+symbol,
            'fields1':'f1,f2,f3,f4','fields2':'f51,f52,f53,f54,f55','fltt':'2','pos':'-2000'})
    def calendar(self):
        import akshare as ak
        from unittest.mock import patch
        original = requests.Session
        network_session = original
        if not self.session.trust_env:
            def network_session(*args, **kwargs):
                value = original(*args, **kwargs); value.trust_env = False
                return value
        with patch('requests.sessions.Session', network_session):
            return set(ak.tool_trade_date_hist_sina()['trade_date'].astype(str))
    def close(self): self.session.close()

def fresh(raw, now):
    try: source = dt.datetime.fromtimestamp(int(raw.get('f86')), TZ)
    except (ValueError, TypeError, OverflowError, OSError): return None, None, False
    delay = (now-source).total_seconds()
    return source.isoformat(), delay, source.date()==now.date() and -5 <= delay <= 90

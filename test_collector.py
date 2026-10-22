import datetime as dt
import tempfile
import unittest
from collector import phase, connect
class Checks(unittest.TestCase):
    def test_sessions(self):
        for clock, expected in [('09:15:00','opening_auction'),('12:00:00',None),('14:57:00','closing_auction'),('15:01:00',None)]:
            self.assertEqual(phase(dt.datetime.fromisoformat('2026-10-22T'+clock)),expected)
    def test_multiplicity(self):
        with tempfile.TemporaryDirectory(dir=".") as folder:
            db=connect(folder+'/test.db')
            for occurrence in [0,1,0,1]:
                db.execute('INSERT OR IGNORE INTO details VALUES(?,?,?,?,?,?)',('2026-10-22','000001','same',occurrence,'now','unverified'))
            self.assertEqual(db.execute('SELECT count(*) FROM details').fetchone()[0],2)
            db.close()
if __name__=='__main__': unittest.main()



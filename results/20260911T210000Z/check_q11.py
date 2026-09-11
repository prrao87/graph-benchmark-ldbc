"""Regression check for issue #16: repeated cached Q11 on the SF1 graph."""
import contextlib
import json
import sys
from importlib.metadata import version
from pathlib import Path

OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(OUT.parents[1] / 'ladybugdb'))
import ladybug as lb
import query

assert version('ladybug') == '0.20.4'
with lb.Database(str(OUT.parents[1] / 'ladybugdb/ldbc_snb_sf1.lbdb')) as db:
    with lb.Connection(db) as conn:
        conn.execute('ANALYZE').close()
        with (OUT / 'q11-plain.txt').open('w') as log, contextlib.redirect_stdout(log):
            for iteration in range(20):
                rows = query.run_query11(conn).to_dicts()
                assert rows == [{'num_e': 190, 'o.name': 'MDLR_Airlines'}], rows
                print(f'Plain execution {iteration + 1}/20 passed')
        # Exercise actual prepared-plan reuse separately from plain execution.
        class PreparedConnection:
            def __init__(self):
                self.statement = None
            def execute(self, text):
                if self.statement is None:
                    self.statement = conn.prepare(text)
                return conn.execute(self.statement)
        prepared = PreparedConnection()
        with (OUT / 'q11-prepared.txt').open('w') as log, contextlib.redirect_stdout(log):
            for iteration in range(20):
                rows = query.run_query11(prepared).to_dicts()
                assert rows == [{'num_e': 190, 'o.name': 'MDLR_Airlines'}], rows
                print(f'Prepared execution {iteration + 1}/20 passed')
print(json.dumps({'ladybug': version('ladybug'), 'plain_executions': 20, 'prepared_executions': 20, 'passed': True}))

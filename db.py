
import os,select,sys
import psycopg2,psycopg2.extras
from urlparse import urlparse

TABLES = (
    ('users',   '''id SERIAL PRIMARY KEY,
                   name TEXT NOT NULL,
                   password TEXT NOT NULL,
                   active BOOLEAN NOT NULL DEFAULT true,
                   admin BOOLEAN NOT NULL DEFAULT false,
                   properties HSTORE,
                   inserted TIMESTAMP NOT NULL DEFAULT NOW()'''),
)

db_params = urlparse(os.environ.get('HEROKU_POSTGRESQL_GOLD_URL','postgres://localhost/'))

db = psycopg2.connect(database=db_params.path[1:],
                      user=db_params.username,
                      password=db_params.password,
                      host=db_params.hostname,
                      port=db_params.port)

db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
psycopg2.extras.register_hstore(db)

def query(sql,params):
    with cursor() as c:
        c.execute(sql,params)
        return c.fetchall()

def queryone(sql,params):
    with cursor() as c:
        c.execute(sql,params)
        return c.fetchone()

def row(table,field,value,multi=False):
    q = 'SELECT * from %s where %s = %%s' % (table,field)
    if multi:
        return query(q,(value,))
    else:
        return queryone(q,(value,))
        
class cursor(object):
    def __init__(self):
        pass
    def __enter__(self):
        self.c = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self.c
    def __exit__(self,type,value,traceback):
        self.c.close()

def check_table(t,c=None):
    with cursor() as c:
        c.execute('SELECT tablename FROM pg_tables WHERE schemaname=%s and tablename=%s',
                                             ('public',t));
        return c.fetchone() is not None

def drop_table(t):
    with cursor() as c:
        c.execute('DROP TABLE %s CASCADE' % t)

def create_table(name,schema):
    if not check_table(name):
        with cursor() as c:
            c.execute('CREATE TABLE %s (%s)' % (name,schema))

def init_db(tables):
    for (name,schema) in tables:
        create_table(name,schema)

if __name__ == '__main__':
    import code
    with cursor() as c:
        def q(s):
            c.execute(s)
            try:
                print "\n".join(map(str,c.fetchall()))
            except psycopg2.ProgrammingError,e:
                pass
        code.interact(local=locals())


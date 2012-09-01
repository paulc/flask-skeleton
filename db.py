
import os,select,sys
import psycopg2,psycopg2.extras
from urlparse import urlparse

TABLES = (
    ('users',   '''id SERIAL PRIMARY KEY,
                   name TEXT NOT NULL,
                   password TEXT NOT NULL,
                   active BOOLEAN NOT NULL DEFAULT true,
                   admin BOOLEAN NOT NULL DEFAULT false,
                   properties HSTORE DEFAULT ''::hstore,
                   inserted TIMESTAMP NOT NULL DEFAULT NOW()'''),
)

_params = urlparse(os.environ.get('HEROKU_POSTGRESQL_GOLD_URL','postgres://localhost/'))

_connection = psycopg2.connect(database=_params.path[1:],
                               user=_params.username,
                               password=_params.password,
                               host=_params.hostname,
                               port=_params.port)

_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
psycopg2.extras.register_hstore(_connection)

class cursor(object):
    def __init__(self):
        pass
    def __enter__(self):
        self.c = _connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self.c
    def __exit__(self,type,value,traceback):
        self.c.close()

def query(sql,params=None):
    with cursor() as c:
        c.execute(sql,params)
        return c.fetchall()

def query_one(sql,params=None):
    with cursor() as c:
        c.execute(sql,params)
        return c.fetchone()

_operators = { 'lt':'<', 'gt':'>', 'in':'in', 'ne':'!=', 'like':'like' }

def _where(where):
    if where: 
        _where = []
        for f in where.keys():
            field,_,op = f.partition('__')
            _where.append('%s %s %%(%s)s' % (field,_operators.get(op,op) or '=',f))
        return ' WHERE ' + ' AND '.join(_where)
    else:
        return ''

def _order(order):
    if order:
        _order = []
        for f in order:
            field,_,direction = f.partition('__')
            _order.append(field + (' DESC' if direction == 'desc' else ''))
        return ' ORDER BY ' + ', '.join(_order)
    else:
        return ''

def select(table,where=None,order=None):
    sql = 'SELECT * FROM %s' % table + _where(where) + _order(order)
    return query(sql,where)
        
def select_one(table,where=None,order=None):
    sql = 'SELECT * FROM %s' % table + _where(where) + _order(order)
    return query_one(sql,where)
        
def insert(table,values):
    _into = []
    _values = []
    for v in values.keys():
        _into.append(v)
        _values.append('%%(%s)s' % v)
    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table,','.join(_into),','.join(_values))
    with cursor() as c:
        c.execute(sql,values)
        return c.rowcount

def delete(table,where=None):
    sql = 'DELETE FROM %s' % table + _where(where)
    with cursor() as c:
        c.execute(sql,where)
        return c.rowcount

def update(table,values,where=None):
    sql = 'UPDATE %s SET %s' % (table,','.join(['%s = %%(%s)s' % (v,v) for v in values.keys()]))
    with cursor() as c:
        sql = c.mogrify(sql,values)
        if where:
            sql += c.mogrify(_where(where),where)
        c.execute(sql)
        return c.rowcount

def check_table(t):
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


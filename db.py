
import os,urlparse
import psycopg2,psycopg2.extras

_connection = None

def _get_url():
    try:
        return [ os.environ[k] for k in os.environ if k.startswith('HEROKU_POSTGRES') ][0]
    except IndexError:
        return 'postgres://localhost/'

def connect(url=None,autocommit=True,hstore=True):
    global _connection
    if not _connection:
        params = urlparse.urlparse(url or _get_url())
        _connection = psycopg2.connect(database=params.path[1:],
                                       user=params.username,
                                       password=params.password,
                                       host=params.hostname,
                                       port=params.port)

        if autocommit:
            _connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        if hstore:
            psycopg2.extras.register_hstore(_connection)

class cursor(object):
    def __init__(self):
        if not _connection:
            raise ValueError("No database connection")
    def __enter__(self):
        self.c = _connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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

def _columns(columns):
    if columns:
        return ",".join([(c if isinstance(c,(str,unicode)) else "%s AS %s" % c) for c in columns])
    else:
        return '*'

def _on((t1,t2),on):
    if on:
        return "%s = %s" % on
    else:
        return "%s.id = %s.%s_id" % (t1,t2,t1)

def _limit(limit):
    if limit:
        return ' LIMIT %d' % limit
    else:
        return ''

def select(table,where=None,order=None,columns=None,limit=None):
    sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
    return query(sql,where)
        
def select_one(table,where=None,order=None,columns=None,limit=None):
    sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
    return query_one(sql,where)

def select_dict(table,key,where=None,order=None,columns=None,limit=None):
    _d = {}
    for row in select(table,where,order,columns,limit):
        _d[row[key]] = row
    return _d

def join(t1,t2,where=None,on=None,order=None,columns=None,limit=None):
    sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) + _where(where) + _order(order) + _limit(limit)
    return query(sql,where)

def join_one(t1,t2,where=None,on=None,order=None,columns=None,limit=None):
    sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) + _where(where) + _order(order) + _limit(limit)
    return query_one(sql,where)

def join_dict(t1,t2,key,where=None,on=None,order=None,columns=None,limit=None):
    _d = {}
    for row in join(t1,t2,where,on,order,columns,limit):
        _d[row[key]] = row
    return _d

def insert(table,values):
    _values = [ '%%(%s)s' % v for v in values.keys() ]
    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table,','.join(values.keys()),','.join(_values))
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

if __name__ == '__main__':
    import code
    connect()
    code.interact(local=locals())


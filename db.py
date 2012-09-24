
"""
    >>> connect('postgres://localhost/')
    >>> tables = (('doctest_t1','''id SERIAL PRIMARY KEY,
    ...                            name TEXT NOT NULL,
    ...                            active BOOLEAN NOT NULL DEFAULT true,
    ...                            properties HSTORE NOT NULL DEFAULT ''::hstore'''),
    ...           ('doctest_t2','''id SERIAL PRIMARY KEY,
    ...                            value TEXT NOT NULL,
    ...                            doctest_t1_id INTEGER NOT NULL REFERENCES doctest_t1(id)'''),
    ...          )
    >>> for name,_ in tables:
    ...     drop_table(name)
    >>> init_db(tables)
    >>> for i in range(10):
    ...     _ = insert('doctest_t1',{'name':chr(97+i)*5,'properties':{'key':str(i)}})

"""

import os,urlparse
import psycopg2,psycopg2.extras,psycopg2.pool

_pool = None

def _get_url():
    try:
        return [ os.environ[k] for k in os.environ if k.startswith('HEROKU_POSTGRES') ][0]
    except IndexError:
        return 'postgres://localhost/'

def connect(url=None,min=1,max=5):
    global _pool
    if not _pool:
        params = urlparse.urlparse(url or _get_url())
        _pool = psycopg2.pool.ThreadedConnectionPool(min,max,
                                                     database=params.path[1:],
                                                     user=params.username,
                                                     password=params.password,
                                                     host=params.hostname,
                                                     port=params.port)
    
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

class cursor(object):

    def __init__(self,hstore=True):
        self.hstore = hstore
        if not _pool:
            raise ValueError("No database pool")

    def __enter__(self):
        self.connection = _pool.getconn()
        if self.hstore:
            psycopg2.extras.register_hstore(self.connection)
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self

    def __exit__(self,type,value,traceback):
        self.commit()
        self.cursor.close()
        _pool.putconn(self.connection)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def execute(self,sql,params=None):
        self.cursor.execute(sql,params)
        return self.cursor.rowcount

    def query(self,sql,params=None):
        self.cursor.execute(sql,params)
        return self.cursor.fetchall()

    def query_one(self,sql,params=None):
        self.cursor.execute(sql,params)
        return self.cursor.fetchone()

    def query_dict(self,sql,key,params=None):
        _d = {}
        for row in self.query(sql,params):
            _d[row[key]] = row
        return _d

    def select(self,table,where=None,order=None,columns=None,limit=None):
        sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
        return self.query(sql,where)

    def select_one(self,table,where=None,order=None,columns=None,limit=None):
        sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
        return self.query_one(sql,where)

    def select_dict(self,table,key,where=None,order=None,columns=None,limit=None):
        sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
        return self.query_dict(sql,key,where)

    def join(self,t1,t2,where=None,on=None,order=None,columns=None,limit=None):
        sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) \
                                + _where(where) + _order(order) + _limit(limit)
        return self.query(sql,where)

    def join_one(self,t1,t2,where=None,on=None,order=None,columns=None,limit=None):
        sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) \
                                + _where(where) + _order(order) + _limit(limit)
        return self.query_one(sql,where)

    def join_dict(self,t1,t2,key,where=None,on=None,order=None,columns=None,limit=None):
        sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) \
                                + _where(where) + _order(order) + _limit(limit)
        return self.query_dict(sql,key,where)

    def insert(self,table,values,returning=None):
        _values = [ '%%(%s)s' % v for v in values.keys() ]
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table,','.join(values.keys()),','.join(_values))
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query_one(sql,values)
        else:
            return self.execute(sql,values)

    def delete(self,table,where=None,returning=None):
        sql = 'DELETE FROM %s' % table + _where(where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql,where)
        else:
            return self.execute(sql,where)

    def update(self,table,values,where=None,returning=None):
        sql = 'UPDATE %s SET %s' % (table,','.join(['%s = %%(%s)s' % (v,v) for v in values.keys()]))
        sql = self.cursor.mogrify(sql,values)
        if where:
            sql += self.cursor.mogrify(_where(where),where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql)
        else:
            return self.execute(sql)

def execute(sql,params=None):
    with cursor() as c:
        return c.execute(sql,params)

def query(sql,params=None):
    with cursor() as c:
        return c.query(sql,params)

def query_one(sql,params=None):
    with cursor() as c:
        return c.query_one(sql,params)

def select(table,where=None,order=None,columns=None,limit=None):
    with cursor() as c:
        return c.select(table,where,order,columns,limit)

def select_one(table,where=None,order=None,columns=None,limit=None):
    with cursor() as c:
        return c.select_one(table,where,order,columns,limit)

def select_dict(table,key,where=None,order=None,columns=None,limit=None):
    with cursor() as c:
        return c.select_dict(table,key,where,order,columns,limit)

def join(t1,t2,where=None,on=None,order=None,columns=None,limit=None):
    with cursor() as c:
        return c.join(t1,t2,where,on,order,columns,limit)

def join_one(t1,t2,where=None,on=None,order=None,columns=None,limit=None):
    with cursor() as c:
        return c.join_one(t1,t2,where,on,order,columns,limit)

def join_dict(t1,t2,key,where=None,on=None,order=None,columns=None,limit=None):
    with cursor() as c:
        return c.join_dict(t1,t2,key,where,on,order,columns,limit)

def insert(table,values,returning=None):
    with cursor() as c:
        return c.insert(table,values,returning)

def delete(table,where=None,returning=None):
    with cursor() as c:
        return c.delete(table,where,returning)

def update(table,values,where=None,returning=None):
    with cursor() as c:
        return c.update(table,values,where,returning)

def check_table(t):
    with cursor() as c:
        _sql = 'SELECT tablename FROM pg_tables WHERE schemaname=%s and tablename=%s'
        return c.query_one(_sql,('public',t)) is not None

def drop_table(t):
    with cursor() as c:
        c.execute('DROP TABLE IF EXISTS %s CASCADE' % t)

def create_table(name,schema):
    if not check_table(name):
        with cursor() as c:
            c.execute('CREATE TABLE %s (%s)' % (name,schema))

def init_db(tables):
    for (name,schema) in tables:
        create_table(name,schema)

if __name__ == '__main__':
    import code,doctest,sys
    if sys.argv.count('--test'):
        doctest.testmod(optionflags=doctest.ELLIPSIS)
    else:
        connect()
        code.interact(local=locals())


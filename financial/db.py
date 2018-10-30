import pymysql

from flask import current_app, g

def get_db():
    if 'db' not in g:
        # g.db = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='financing7', charset='utf8')
        g.db = pymysql.connect(host='47.93.250.19', port=3306, user='root',
                passwd='root', db='financing7', charset='utf8')
    return g.db

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_app(app):
    app.teardown_appcontext(close_db)

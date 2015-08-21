from config import dbhost, user, pw, log_dir
from debug import set_config
from dbutils import BaseDB, WikiDB, open_session as dbutils_open_session
from model.master import Base


def init_logger():
    set_config(log_dir + '/log')


def get_wiki_db(lang):
    return WikiDB(lang, user, dbhost, pw)


def get_amz_db(lang):
    return BaseDB(user, dbhost, pw, '%samz' % (lang,))


def open_session(**kwargs):
    return dbutils_open_session(dbhost, user, 'master', Base, pw, **kwargs)

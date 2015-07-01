from debug import get_logger
from urllib.parse import parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
from dbutils import master_session
from model.master import Base
from .calculator import Calculator
import json

NOT_FOUND = 404
ERROR = 501
SUCCESS = 200


def parse_idmap(data_dict):
    logger = get_logger(__name__)
    if 'idmap' not in data_dict:
        logger.debug('"idmap" was not found')
        return False, None, None

    if 'lang' not in data_dict:
        logger.debug('"lang" was not found')
        return False, None, None

    idmap_str = data_dict['idmap'][0]
    if not isinstance(idmap_str, str):
        logger.debug('"idmap" was not a str: %s' % (type(idmap_str)))
        return False, None, None

    logger.debug(idmap_str)
    idmap = json.loads(idmap_str)

    if not isinstance(idmap, dict):
        logger.debug('"idmap" was not a dict in json')
        return False, None, None
    idmap = {int(key): float(val) for key, val in idmap.items()}

    return True, idmap, data_dict['lang'][0]


class Handler(BaseHTTPRequestHandler):
    def not_found(self, *args):
        return NOT_FOUND, None

    def dispatch(self, data_string):
        data_dict = parse_qs(data_string)
        callback = self.not_found
        if self.path.startswith('/feature_to_item'):
            callback = self.feature_to_item
        elif self.path.startswith('/feature_to_feature'):
            callback = self.feature_to_feature
        elif self.path.startswith('/item_to_feature'):
            callback = self.item_to_feature

        code, rt_obj = callback(data_dict)

        self.send_response(code)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(rt_obj), 'utf-8'))
        return

    def feature_to_item(self, data_dict):
        ok, idmap, lang = parse_idmap(data_dict)
        if ok and lang in self.server.lang_calc:
            return SUCCESS, \
                self.server.lang_calc[lang].get_items_by_features(idmap)
        else:
            return ERROR, None

    def feature_to_feature(self, data_dict):
        ok, idmap, lang = parse_idmap(data_dict)
        if ok and lang in self.server.lang_calc:
            return SUCCESS, \
                self.server.lang_calc[lang].get_features_by_features(idmap)
        else:
            return ERROR, None

    def item_to_feature(self, data_dict):
        ok, idmap, lang = parse_idmap(data_dict)
        if ok and lang in self.server.lang_calc:
            return SUCCESS, \
                self.server.lang_calc[lang].get_features_by_items(idmap)
        else:
            return ERROR, None

    def do_GET(self):
        data = None
        pos = self.path.find('?')
        if pos != -1:
            data = self.path[pos+1:]
        return self.dispatch(data.decode('utf-8'))

    def do_POST(self):
        data = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        return self.dispatch(data.decode('utf-8'))


class Server(HTTPServer):
    def set_lang_calculator(self, lang_calc):
        self.lang_calc = lang_calc


def run():
    address = ('', 8000)
    httpd = Server(address, Handler)
    langs = ['en', 'ja']
    lang_calc = {}
    with master_session('master', Base) as session:
        for lang in langs:
            calc = Calculator(lang)
            calc.load(session)
            lang_calc[lang] = calc

    httpd.set_lang_calculator(lang_calc)
    httpd.serve_forever()

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
        return False, None

    idmap_str = data_dict['idmap'][0]
    if not isinstance(idmap_str, str):
        logger.debug('"idmap" was not a str: %s' % (type(idmap_str)))
        return False, None

    logger.debug(idmap_str)
    idmap = json.loads(idmap_str)

    if not isinstance(idmap, dict):
        logger.debug('"idmap" was not a dict in json')
        return False, None
    idmap = {int(key): float(val) for key, val in idmap.items()}

    return True, idmap


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
        ok, idmap = parse_idmap(data_dict)
        if ok:
            return SUCCESS, self.server.calc.get_features_by_items(idmap)
        else:
            return ERROR, None

    def feature_to_feature(self, data_dict):
        ok, idmap = parse_idmap(data_dict)
        if ok:
            return SUCCESS, self.server.calc.get_features_by_features(idmap)
        else:
            return ERROR, None

    def item_to_feature(self, data_dict):
        ok, idmap = parse_idmap(data_dict)
        if ok:
            return SUCCESS, self.server.calc.get_items_by_features(idmap)
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
    def set_calculator(self, calc):
        self.calc = calc


def run():
    address = ('', 8000)
    httpd = Server(address, Handler)
    with master_session('master', Base) as session:
        calc = Calculator()
        calc.load(session)

    httpd.set_calculator(calc)
    httpd.serve_forever()

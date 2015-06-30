import http.client
from urllib.parse import urlparse, urlencode


def get(url_str):
    port = 80
    url = urlparse(url_str)
    if not url.hostname or not url.path:
        raise Exception('Invalid url')
    if url.port:
        port = url.port
    conn = http.client.HTTPConnection(url.hostname, port)
    conn.request('GET', url.path)
    res = conn.getresponse()
    return res


def post(url_str, keyvalue):
    port = 80
    url = urlparse(url_str)
    if not url.hostname or not url.path:
        raise Exception('Invalid url')
    if url.port:
        port = url.port
    conn = http.client.HTTPConnection(url.hostname, port)
    conn.request('POST', url.path, urlencode(keyvalue))
    res = conn.getresponse()
    return res

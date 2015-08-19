import http.client
import re
from urllib.parse import urlparse, urlencode
from bs4 import BeautifulSoup as Soup


def get(url_str, proxy=None):
    '''
    @param proxy like '1.1.1.1:8080'
    '''
    port = 80
    url = urlparse(url_str)
    if not url.hostname or not url.path:
        raise Exception('Invalid url')
    if url.port:
        port = url.port

    if proxy:
        p_host, p_port = proxy.split(':')
        conn = http.client.HTTPConnection(p_host, p_port)
        conn.request('GET', url_str)
    else:
        conn = http.client.HTTPConnection(url.hostname, port)
        conn.request('GET', url.path)

    res = conn.getresponse()
    return res


content_type_pattern = re.compile('charset=(.+)$', re.IGNORECASE)


def get_html(url_str, proxy=None, encode=None):
    res = get(url_str, proxy=proxy)
    if res.code == 200:
        if encode:
            return res.readall().decode(encode)
        content_type = res.getheader('content-type')
        mat = content_type_pattern.search(content_type)
        if mat is None:
            raise 'Invalid content_type: %s' % (content_type,)
        return res.readall().decode(mat.group(1))

    return None


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


def get_proxy_list():
    url = 'http://www.cybersyndrome.net/pla.html'
    html = get_html(url, encode='utf-8')
    if not html:
        return None
    sp = Soup(html, 'html.parser')
    return [el.get_text() for el in sp.select('a.A')]

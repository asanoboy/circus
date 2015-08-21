import http.client
import re
from urllib.parse import urlparse, urlencode
from bs4 import BeautifulSoup as Soup


class ProxyHost:
    def __init__(self, host_and_port, region):
        host, port = host_and_port.split(':')
        self.host = host
        self.port = port
        self.region = region

    def __str__(self):
        if self.region:
            return '%s:%s(%s)' % (self.host, self.port, self.region)
        else:
            return '%s:%s' % (self.host, self.port)


def get(url_str, proxy=None, timeout=None):
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
        conn = http.client.HTTPConnection(
            proxy.host, proxy.port, timeout=timeout)
        conn.request('GET', url_str)
    else:
        conn = http.client.HTTPConnection(url.hostname, port, timeout=timeout)
        conn.request('GET', url.path)

    res = conn.getresponse()
    return res


content_type_pattern = re.compile('charset=(.+)$', re.IGNORECASE)


def get_html(url_str, proxy=None, encode=None, timeout=None):
    try:
        res = get(url_str, proxy=proxy, timeout=timeout)
    except:
        return None

    if res.code == 200:
        try:
            content = res.readall()
        except:
            return None
        if encode:
            return content.decode(encode)
        content_type = res.getheader('content-type')
        mat = content_type_pattern.search(content_type)
        if mat is None:
            raise 'Invalid content_type: %s' % (content_type,)
        return content.decode(mat.group(1))

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
    return [
        ProxyHost(
            el.get_text(),
            el.attrs['title'] if el.has_attr('title') else None)
        for el in sp.select('a.A')]

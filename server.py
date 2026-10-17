"""Nonblocking Japanese X feed. Secrets are read only from environment variables."""
import datetime as dt
import html
import json
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor

VERSION = '20261017-buffer-1'
LOCK = threading.Lock()
CACHE = {'schema': 'xplay-v1', 'updated': '', 'posts': []}
LAST = 0.0
DAY = ''
COUNT = 0
COLLECTING = False
LAST_STATE = 'ready'
INTERVAL = max(30, int(os.getenv('MIN_REFRESH_SECONDS', '30')))
LIMIT = max(0, int(os.getenv('MAX_COLLECTIONS_PER_DAY', '24')))
MAX_AGE_SECONDS = 86400
POST_URL = re.compile(r'https://(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)(?:/[^?]*)?(?:\?.*)?$')


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def parse_time(value):
    try:
        stamp = dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return stamp.replace(tzinfo=dt.timezone.utc) if stamp.tzinfo is None else stamp.astimezone(dt.timezone.utc)
    except (ValueError, TypeError):
        return None


def candidate_urls(raw):
    try:
        envelope = json.loads(raw)
        if isinstance(envelope, dict) and isinstance(envelope.get('body'), str):
            raw = envelope['body']
    except ValueError:
        pass
    for _ in range(2):
        raw = html.unescape(urllib.parse.unquote(raw))
    raw = raw.replace('\\/', '/').replace(r'\u002F', '/').replace(r'\u002f', '/').replace(r'\u003A', ':')
    by_id = {}
    for user, ident in re.findall(r'(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)', raw):
        by_id[ident] = f'https://x.com/{user.lower()}/status/{ident}'
    return [by_id[key] for key in sorted(by_id, key=int, reverse=True)]


def search_targets(now):
    # No topic, account, popularity or engagement criteria. Japanese is checked on the returned text.
    query = 'site:x.com/'
    return ['https://www.google.com/search?' + urllib.parse.urlencode({
        'q': query, 'num': 20, 'hl': 'ja', 'lr': 'lang_ja', 'gl': 'jp', 'tbs': f'qdr:{period},sbd:1',
    }) for period in ('h', 'd')]


def search_body(raw):
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get('body'), str):
            return data['body']
    except ValueError:
        pass
    return raw


class ResultLinks(HTMLParser):
    def __init__(self):
        super().__init__()
        self.href, self.links = '', []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.href = dict(attrs).get('href', '')
        elif tag == 'h3' and self.href.startswith('/goto?url='):
            self.links.append('https://www.google.com' + self.href)


class FoundPost(Exception):
    pass


class PostRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if POST_URL.fullmatch(newurl):
            raise FoundPost(newurl)
        parsed = urllib.parse.urlsplit(newurl)
        if parsed.scheme != 'https' or parsed.hostname not in ('www.google.com', 'google.com'):
            raise ValueError('not_a_post_redirect')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def resolve_result(url):
    try:
        # Stop at the redirect header; do not download X, and never follow arbitrary hosts.
        request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.build_opener(PostRedirect()).open(request, timeout=8):
            pass
    except FoundPost as post:
        return str(post)
    except Exception:
        pass
    return None


def discover_urls(raw):
    urls = candidate_urls(raw)
    parser = ResultLinks()
    parser.feed(search_body(raw))
    if parser.links:
        with ThreadPoolExecutor(max_workers=4) as pool:
            urls += [url for url in pool.map(resolve_result, list(dict.fromkeys(parser.links))[:12]) if url]
    return candidate_urls('\n'.join(urls))


def normalize_posts(rows, now=None):
    now = now or utcnow()
    posts = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        text = row.get('description') or row.get('body') or ''
        url = row.get('url') or row.get('source') or ''
        match = POST_URL.fullmatch(url)
        stamp = parse_time(row.get('date_posted') or row.get('posted_at'))
        lang = str(row.get('lang') or row.get('language') or '').lower()
        if not isinstance(text, str) or not 1 <= len(text.encode('utf-16-le')) // 2 <= 8000:
            continue
        if lang and lang not in ('ja', 'japanese', '日本語'):
            continue
        if not lang and not re.search('[ぁ-んァ-ン]', text):
            continue
        if not match or not stamp or not -300 <= (now - stamp).total_seconds() <= MAX_AGE_SECONDS:
            continue
        ident = match[2]
        posts[ident] = {
            'title': row.get('title') or '@' + str(row.get('user_posted') or match[1]),
            'body': text, 'source': f'https://x.com/{match[1].lower()}/status/{ident}',
            'posted_at': stamp.isoformat(timespec='seconds'),
            'fetched_at': row.get('fetched_at') or now.isoformat(timespec='seconds'),
            'full_text': True,
        }
    return sorted(posts.values(), key=lambda post: parse_time(post['posted_at']), reverse=True)


def collect():
    deadline = time.monotonic() + 140

    def api(path, payload=None):
        remaining = deadline - time.monotonic()
        if remaining < 2:
            raise TimeoutError('collection_timeout')
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request('https://api.brightdata.com/' + path, data=data, headers={
            'Authorization': 'Bearer ' + os.environ['BRIGHTDATA_API_KEY'], 'Content-Type': 'application/json',
        })
        with urllib.request.urlopen(request, timeout=min(45, remaining)) as response:
            return response.read().decode('utf-8', 'replace')

    urls = []
    now = utcnow()
    for target in search_targets(now):
        raw = api('request', {'zone': os.getenv('BRIGHTDATA_ZONE', 'mcp_unlocker'), 'url': target, 'format': 'raw'})
        urls = discover_urls(raw)
        # A post ID embeds its creation time. Discard old search-index hits before paying to scrape them.
        urls = [url for url in urls if -300 <= now.timestamp() - ((int(url.rsplit('/', 1)[1]) >> 22) + 1288834974657) / 1000 <= MAX_AGE_SECONDS]
        if len(urls) >= 3:
            break
    if not urls:
        raise ValueError('no_fresh_candidates')
    data = json.loads(api('datasets/v3/scrape?dataset_id=gd_lwxkxvnf1cynvib9co&format=json', {'input': [{'url': url} for url in urls[:10]]}))
    if isinstance(data, dict) and data.get('snapshot_id'):
        snapshot = data['snapshot_id']
        if not re.fullmatch(r'[A-Za-z0-9_-]+', snapshot):
            raise ValueError('invalid_snapshot')
        while time.monotonic() < deadline - 5:
            time.sleep(5)
            state = json.loads(api('datasets/v3/progress/' + snapshot))
            if state.get('status') == 'ready':
                data = json.loads(api('datasets/v3/snapshot/' + snapshot + '?format=json'))
                break
            if state.get('status') == 'failed':
                raise ValueError('scrape_failed')
    if not isinstance(data, list):
        raise TimeoutError('collection_timeout')
    posts = normalize_posts(data)
    if not posts:
        raise ValueError('no_fresh_japanese_posts')
    return {'schema': 'xplay-v1', 'updated': utcnow().isoformat(timespec='seconds'), 'posts': posts}


def merge_feed(previous, incoming):
    # Retain original acquisition time for duplicate posts; a failed/new-empty search cannot fake freshness.
    by_source = {p['source']: p for p in incoming.get('posts', [])}
    for post in previous.get('posts', []):
        by_source[post['source']] = post
    posts = normalize_posts(list(by_source.values()))[:12]
    result = {'schema': 'xplay-v1', 'updated': incoming.get('updated', previous.get('updated', '')), 'posts': posts}
    while posts and len(json.dumps(result, ensure_ascii=False).encode('utf-16-le')) // 2 > 44000:
        posts.pop()
    return result


def collection_worker():
    global CACHE, LAST, COLLECTING, LAST_STATE
    try:
        incoming = collect()
        with LOCK:
            old_sources = {p['source'] for p in CACHE.get('posts', [])}
            CACHE = merge_feed(CACHE, incoming)
            LAST_STATE = 'ready' if any(p['source'] not in old_sources for p in incoming['posts']) else 'no_new_posts'
    except Exception as error:
        with LOCK:
            if isinstance(error, urllib.error.HTTPError) and error.code in (402, 429):
                LAST_STATE = 'upstream_limit'
            elif isinstance(error, ValueError) and str(error).startswith('no_fresh'):
                LAST_STATE = 'no_new_posts'
            else:
                LAST_STATE = 'error'
    finally:
        with LOCK:
            LAST = time.monotonic()
            COLLECTING = False


def get_feed():
    global DAY, COUNT, COLLECTING
    with LOCK:
        today = utcnow().date().isoformat()
        if DAY != today:
            DAY, COUNT = today, 0
        remaining = max(0, math.ceil(INTERVAL - (time.monotonic() - LAST)))
        state = LAST_STATE
        if COLLECTING:
            state, retry = 'collecting', 5
        elif COUNT >= LIMIT:
            state = 'daily_limit'
            tomorrow = (utcnow() + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            retry = max(5, math.ceil((tomorrow - utcnow()).total_seconds()))
        elif remaining:
            state, retry = ('cooldown' if LAST_STATE == 'ready' else LAST_STATE), remaining
        else:
            COLLECTING = True
            COUNT += 1
            state, retry = 'collecting', 5
            threading.Thread(target=collection_worker, daemon=True).start()
        notices = {
            'collecting': '次の日本語投稿を先読みしています',
            'ready': '新しい日本語投稿を準備しました',
            'cooldown': '取得間隔を空けて、必要な分を自動で補充します',
            'daily_limit': '本日の収集上限です。準備済みの投稿を表示します',
            'no_new_posts': '新しい日本語投稿がまだ見つかりません。自動で再試行します',
            'upstream_limit': '取得サービスの利用枠を確認してください。準備済みの投稿を維持します',
            'error': '取得に失敗しました。準備済みの投稿を保って自動で再試行します',
        }
        return dict(CACHE, collection_state=state, retry_after=retry,
                    refresh_notice=notices[state], ordering='posted_at_desc', language='ja')


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        if path == '/health':
            self.respond(200, {'ok': True, 'version': VERSION, 'refresh_interval_seconds': INTERVAL,
                               'collection_state': 'collecting' if COLLECTING else LAST_STATE})
        elif path != '/xposts.json':
            self.respond(404, {'error': 'not_found'})
        elif not os.getenv('BRIGHTDATA_API_KEY'):
            self.respond(503, {'error': 'server_not_configured'})
        else:
            self.respond(200, get_feed())

    def respond(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0', int(os.getenv('PORT', '10000'))), Handler).serve_forever()

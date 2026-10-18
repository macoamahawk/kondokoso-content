"""Nonblocking Japanese X feed. Secrets are read only from environment variables."""
import datetime as dt
import html
import json
import math
import os
import re
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor

VERSION = '20261019-no-replies-or-tags-1'
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
PREFERRED_AGE_SECONDS = 60
MAX_METRICS = {'likes': 10, 'reposts': 2, 'replies': 5, 'views': 1000}
FILTER_POLICY = 'quiet-text-ja-no-replies-tags-v2'
PROMOTION = re.compile(
    r'告知|宣伝|お知らせ|販売(?:開始|中|予定)|発売|新商品|新発売|入荷|再販|通販|'
    r'予約(?:受付|開始|はこちら)|受付(?:開始|中)|募集中|募集開始|求人|採用情報|'
    r'キャンペーン|プレゼント企画|抽選(?:で|販売|プレゼント)|クーポン|割引|セール|'
    r'購入はこちら|ご購入|お買い求め|お申し込み|お申込み|申し込みはこちら|'
    r'無料(?:相談|体験|配布)|案件(?:募集|受付)|依頼(?:募集|受付)|'
    r'配信(?:開始|中|予定|します|のお知らせ)|ライブ配信|生配信|'
    r'動画(?:公開|投稿しました|アップしました)|新作(?:公開|販売)|'
    r'記事(?:公開|更新)|ブログ(?:公開|更新)|イベント(?:開催|告知)|'
    r'(?:^|[\s#＃【])pr(?:$|[\s】:#＃])|プロモーション|広告|'
    r'\b(?:sponsored|giveaway|coupon|sale|discount|affiliate)\b', re.I)
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
    # These are discovery hints. Actual post timestamps, media and metrics are checked after scraping.
    query = 'site:x.com/'
    return ['https://www.google.com/search?' + urllib.parse.urlencode({
        'q': query, 'num': 20, 'hl': 'ja', 'lr': 'lang_ja', 'gl': 'jp', 'tbs': f'qdr:{period},sbd:1',
    }) for period in ('n1', 'n15', 'h', 'd')]


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


def has_content(value):
    if isinstance(value, dict):
        return any(has_content(v) for v in value.values())
    if isinstance(value, list):
        return any(has_content(v) for v in value)
    if isinstance(value, str):
        return value.strip().lower() not in ('', 'null', 'none', 'false', '[]', '{}')
    return bool(value)


def metric_count(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) if math.isfinite(value) and value >= 0 and value == int(value) else None
    if isinstance(value, str):
        clean = unicodedata.normalize('NFKC', value).strip().replace(',', '')
        match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([kKmM万億]?)', clean)
        if match:
            count = float(match[1]) * {'': 1, 'k': 1000, 'm': 1000000, '万': 10000, '億': 100000000}[match[2].lower()]
            return math.ceil(count) if math.isfinite(count) else None
    return None


def eligible_metadata(row, text):
    # A reply can have no visible @ prefix. Check structured relationships as well as text.
    reply_fields = ('is_reply', 'in_reply_to_status_id', 'in_reply_to_status_id_str',
                    'in_reply_to_user_id', 'in_reply_to_user_id_str', 'reply_to',
                    'replying_to', 'parent_post_url', 'parent_url')
    if any(has_content(row.get(key)) for key in reply_fields):
        return None
    if str(row.get('post_type', '')).lower() in ('reply', 'replied_to'):
        return None
    parent = row.get('parent_post_details') or {}
    own_id = str(row.get('id') or str(row.get('url') or row.get('source') or '').split('/status/')[-1].split('?')[0])
    if isinstance(parent, dict) and has_content(parent.get('post_id')) and str(parent['post_id']) != own_id:
        return None
    if any(ref.get('type') == 'replied_to' for ref in (row.get('referenced_tweets') or []) if isinstance(ref, dict)):
        return None
    normalized = unicodedata.normalize('NFKC', text)
    if re.search(r'[#@]', normalized) or any(has_content(row.get(key)) for key in ('hashtags', 'tagged_users', 'mentions')):
        return None
    # Missing metadata is unknown, never evidence of zero views or no image/quote.
    if 'photos' not in row or 'quoted_post' not in row:
        return None
    if any(has_content(row.get(key)) for key in ('photos', 'external_image_urls', 'ai_generated_images')):
        return None
    if has_content(row['quoted_post']) or any(has_content(row.get(key)) for key in ('is_quote', 'is_quote_status', 'quoted_status_id')):
        return None
    if any(ref.get('type') == 'quoted' for ref in (row.get('referenced_tweets') or []) if isinstance(ref, dict)):
        return None
    metrics = {key: metric_count(row.get(key)) for key in MAX_METRICS}
    if any(value is None or value > MAX_METRICS[key] for key, value in metrics.items()):
        return None
    if PROMOTION.search(normalized) or any(has_content(row.get(key)) for key in ('is_promoted', 'is_ad', 'promoted_content')):
        return None
    # Links are conservatively excluded as well to reduce unlabelled ads and announcements.
    if re.search(r'https?://|www\.', normalized, re.I) or has_content(row.get('external_url')):
        return None
    return metrics


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
        if not match or not stamp or not -30 <= (now - stamp).total_seconds() <= MAX_AGE_SECONDS:
            continue
        metrics = eligible_metadata(row, text)
        if metrics is None:
            continue
        ident = match[2]
        posts[ident] = {
            'title': row.get('title') or '@' + str(row.get('user_posted') or match[1]),
            'body': text, 'source': f'https://x.com/{match[1].lower()}/status/{ident}',
            'posted_at': stamp.isoformat(timespec='seconds'),
            'fetched_at': row.get('fetched_at') or now.isoformat(timespec='seconds'),
            'full_text': True,
            'photos': [], 'quoted_post': None, **metrics,
            'is_reply': False, 'hashtags': [], 'tagged_users': [],
            'filter_policy': FILTER_POLICY,
        }
    return sorted(posts.values(), key=lambda post: parse_time(post['posted_at']), reverse=True)


def collect():
    deadline = time.monotonic() + 180

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

    now = utcnow()
    def discover_window(pair):
        target, window = pair
        try:
            raw = api('request', {'zone': os.getenv('BRIGHTDATA_ZONE', 'mcp_unlocker'), 'url': target, 'format': 'raw'})
            found = discover_urls(raw)
            # Check the actual creation time embedded in each ID, not the search engine's date label.
            return [url for url in found if -30 <= now.timestamp() - ((int(url.rsplit('/', 1)[1]) >> 22) + 1288834974657) / 1000 <= window]
        except urllib.error.HTTPError as error:
            if error.code in (402, 429):
                raise
            return []
        except (OSError, ValueError):
            return []
    # Search all fallback windows concurrently, then scrape only the ten newest distinct candidates.
    # This bounds the collection cost and gives sub-minute candidates priority without serial waits.
    with ThreadPoolExecutor(max_workers=4) as pool:
        batches = list(pool.map(discover_window, zip(search_targets(now), (60, 900, 3600, MAX_AGE_SECONDS))))
    urls = candidate_urls('\n'.join(url for batch in batches for url in batch))
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
    accepted = {post['source'] for post in posts}
    rejected = []
    for row in data:
        if isinstance(row, dict):
            source = candidate_urls(str(row.get('url') or row.get('source') or ''))
            rejected += [url for url in source if url not in accepted]
    return {'schema': 'xplay-v1', 'updated': utcnow().isoformat(timespec='seconds'),
            'posts': posts, 'rejected_sources': rejected}


def merge_feed(previous, incoming):
    # Retain original acquisition time for duplicate posts; a failed/new-empty search cannot fake freshness.
    by_source = {p['source']: p for p in incoming.get('posts', [])}
    rejected = set(incoming.get('rejected_sources', []))
    for post in normalize_posts(previous.get('posts', [])):
        if post['source'] not in rejected:
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
            'collecting': '画像・引用・返信・タグ・告知を除き、反応の少ない日本語投稿を先読み中です',
            'ready': '条件に合う日本語投稿を新しい順に準備しました',
            'cooldown': '取得間隔を空けて、必要な分を自動で補充します',
            'daily_limit': '本日の収集上限です。準備済みの投稿を表示します',
            'no_new_posts': '条件に合う新しい投稿がまだありません。条件を保って自動で再試行します',
            'upstream_limit': '取得サービスの利用枠を確認してください。準備済みの投稿を維持します',
            'error': '取得に失敗しました。準備済みの投稿を保って自動で再試行します',
        }
        return dict(CACHE, collection_state=state, retry_after=retry,
                    refresh_notice=notices[state], ordering='posted_at_desc', language='ja',
                    filter_policy=FILTER_POLICY, preferred_age_seconds=PREFERRED_AGE_SECONDS,
                    filters={'no_images': True, 'no_quotes': True, 'no_promotion': True,
                             'no_replies': True, 'no_hashtags': True, 'no_mentions': True,
                             'no_links': True, 'max_metrics': MAX_METRICS,
                             'unknown_metadata': 'exclude', 'freshness': 'prefer_60s_then_newest_available'})


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

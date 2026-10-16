"""Small HTTPS-hosted feed service; credentials belong only in environment variables."""
import datetime as dt
import html,json,os,re,threading,time,urllib.request,urllib.parse
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
LOCK=threading.Lock()
CACHE=json.loads(Path(__file__).with_name('xposts.json').read_text(encoding='utf-8'))
LAST=0.0
DAY=''
COUNT=0
INTERVAL=max(30,int(os.getenv('MIN_REFRESH_SECONDS','30')))
LIMIT=max(0,int(os.getenv('MAX_COLLECTIONS_PER_DAY','24')))

def candidate_urls(raw):
    raw=html.unescape(urllib.parse.unquote(raw)).replace('\\/','/')
    return list(dict.fromkeys(f'https://x.com/{a}/status/{b}' for a,b in re.findall(r'(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)',raw)))[:6]

def collect():
    deadline=time.monotonic()+95
    def api(path,payload=None):
        remaining=deadline-time.monotonic()
        if remaining<2: raise TimeoutError('Collection timeout')
        data=None if payload is None else json.dumps(payload).encode()
        req=urllib.request.Request('https://api.brightdata.com/'+path,data=data,headers={'Authorization':'Bearer '+os.environ['BRIGHTDATA_API_KEY'],'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=min(35,remaining)) as response:return response.read().decode('utf-8','replace')
    query=os.getenv('X_SEARCH_QUERY','site:x.com/ "おやすみ"')
    target='https://www.google.com/search?'+urllib.parse.urlencode({'q':query,'num':10,'tbs':'qdr:w'})
    urls=[]
    for attempt in range(2):
        raw=api('request',{'zone':os.getenv('BRIGHTDATA_ZONE','mcp_unlocker'),'url':target,'format':'raw'})
        urls=candidate_urls(raw)
        if urls:break
    reused_candidates=not urls
    if reused_candidates:
        urls=[p['source'] for p in CACHE.get('posts',[]) if re.fullmatch(r'https://(?:x\.com|twitter\.com)/[A-Za-z0-9_]+/status/\d+/?',p.get('source',''))][:6]
    if not urls:raise ValueError('No candidate URLs')
    data=json.loads(api('datasets/v3/scrape?dataset_id=gd_lwxkxvnf1cynvib9co&format=json',{'input':[{'url':u} for u in urls]}))
    if isinstance(data,dict) and data.get('snapshot_id'):
        snapshot=data['snapshot_id']
        if not re.fullmatch(r'[A-Za-z0-9_-]+',snapshot):raise ValueError('Invalid snapshot')
        while time.monotonic()<deadline-5:
            time.sleep(5)
            state=json.loads(api('datasets/v3/progress/'+snapshot))
            if state.get('status')=='ready':
                data=json.loads(api('datasets/v3/snapshot/'+snapshot+'?format=json'));break
            if state.get('status')=='failed':raise ValueError('Collection failed')
    if not isinstance(data,list):raise TimeoutError('Collection not complete')
    posts=[];seen=set()
    for p in data:
        text=p.get('description') or '';url=p.get('url') or ''
        if not re.search('[ぁ-んァ-ン]',text) or not 16<=len(text)<=8000 or url in seen:continue
        if text.startswith('@') or re.search(r'殺|死ね|自殺|エロ|セックス|プレゼント企画|懸賞',text):continue
        if not re.fullmatch(r'https://(?:x\.com|twitter\.com)/[A-Za-z0-9_]+/status/\d+/?',url):continue
        if not p.get('date_posted'):continue
        posts.append({'title':'@'+str(p.get('user_posted','')),'body':text,'source':url,'posted_at':p['date_posted'],'full_text':True});seen.add(url)
    if len(posts)<2:raise ValueError('Too few usable posts')
    result={'schema':'xplay-v1','updated':dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec='seconds'),'posts':posts}
    if reused_candidates:result['refresh_notice']='取得完了：新しい候補なし・既存候補の本文を再取得しました'
    return result

def get_feed():
    global CACHE,LAST,DAY,COUNT
    with LOCK:
        today=dt.datetime.now(dt.timezone.utc).date().isoformat()
        if DAY!=today:DAY=today;COUNT=0
        now=time.monotonic()
        if COUNT>=LIMIT:return dict(CACHE,refresh_notice='本日の収集上限です。保存済み候補を表示します')
        if now-LAST<INTERVAL:return dict(CACHE,refresh_notice='サーバーの次の収集まで'+str(int(INTERVAL-(now-LAST))+1)+'秒・保存済み候補を表示')
        LAST=now;COUNT+=1
        CACHE=collect() # Failures keep the last valid feed. Count attempts toward the cap.
        return CACHE

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=='/health':self.respond(200,{'ok':True,'version':'20261017-3','refresh_interval_seconds':INTERVAL});return
        if self.path!='/xposts.json':self.respond(404,{'error':'not_found'});return
        if not os.getenv('BRIGHTDATA_API_KEY'):self.respond(503,{'error':'server_not_configured'});return
        try:self.respond(200,get_feed())
        except Exception as exc:
            reason='upstream_unavailable'
            if isinstance(exc,urllib.error.HTTPError):reason='upstream_http_'+str(exc.code)
            elif isinstance(exc,TimeoutError):reason='collection_timeout'
            elif isinstance(exc,ValueError) and str(exc) in ('No candidate URLs','Too few usable posts','Collection failed'):reason={'No candidate URLs':'no_candidates','Too few usable posts':'insufficient_posts','Collection failed':'scrape_failed'}[str(exc)]
            self.respond(503,{'error':'collection_failed','reason':reason,'retry_after':INTERVAL})
    def respond(self,status,data):
        body=json.dumps(data,ensure_ascii=False).encode()
        self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers()
        try:self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError):pass
    def log_message(self,*args):pass # Never print keys or upstream responses.

if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()










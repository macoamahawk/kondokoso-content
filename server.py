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
INTERVAL=max(30,int(os.getenv('MIN_REFRESH_SECONDS','60')))
LIMIT=max(0,int(os.getenv('MAX_COLLECTIONS_PER_DAY','24')))

def collect():
    deadline=time.monotonic()+95
    def api(path,payload=None):
        remaining=deadline-time.monotonic()
        if remaining<2: raise TimeoutError('Collection timeout')
        data=None if payload is None else json.dumps(payload).encode()
        req=urllib.request.Request('https://api.brightdata.com/'+path,data=data,headers={'Authorization':'Bearer '+os.environ['BRIGHTDATA_API_KEY'],'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=min(35,remaining)) as response:return response.read().decode('utf-8','replace')
    query=os.getenv('X_SEARCH_QUERY','site:x.com/ ("おやすみ" OR "お腹すいた" OR "楽しかった")')
    target='https://www.google.com/search?'+urllib.parse.urlencode({'q':query,'num':10,'tbs':'qdr:d'})
    raw=api('request',{'zone':os.getenv('BRIGHTDATA_ZONE','mcp_unlocker'),'url':target,'format':'raw'})
    raw=html.unescape(urllib.parse.unquote(raw)).replace('\\/','/')
    urls=list(dict.fromkeys(f'https://x.com/{a}/status/{b}' for a,b in re.findall(r'(?:x\.com|twitter\.com)/([A-Za-z0-9_]+)/status/(\d+)',raw)))[:6]
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
    return {'schema':'xplay-v1','updated':dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat(timespec='seconds'),'posts':posts}

def get_feed():
    global CACHE,LAST,DAY,COUNT
    with LOCK:
        today=dt.datetime.now(dt.timezone.utc).date().isoformat()
        if DAY!=today:DAY=today;COUNT=0
        now=time.monotonic()
        if now-LAST<INTERVAL or COUNT>=LIMIT:return CACHE
        LAST=now;COUNT+=1
        CACHE=collect() # Failures keep the last valid feed. Count attempts toward the cap.
        return CACHE

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=='/health':self.respond(200,{'ok':True});return
        if self.path!='/xposts.json':self.respond(404,{'error':'not_found'});return
        if not os.getenv('BRIGHTDATA_API_KEY'):self.respond(503,{'error':'server_not_configured'});return
        try:self.respond(200,get_feed())
        except Exception:self.respond(503,{'error':'collection_failed','retry_after':INTERVAL})
    def respond(self,status,data):
        body=json.dumps(data,ensure_ascii=False).encode()
        self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers()
        try:self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError):pass
    def log_message(self,*args):pass # Never print keys or upstream responses.

if __name__=='__main__':ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','10000'))),Handler).serve_forever()

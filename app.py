import streamlit as st
import pandas as pd
import time, json, threading, websocket, requests
from collections import deque
from datetime import datetime, timezone

st.set_page_config(page_title='BTC Order Flow', layout='wide')
class Feed:
    def __init__(self):
        self.lock=threading.Lock(); self.trades={x:deque(maxlen=500000) for x in ('Coinbase','Kraken')}; self.status={}; self.started={}; self.last={}; self.ids={}; self.books={'buy':{},'sell':{}}; self.booktime=0; self.gap={}
        for x in self.trades: threading.Thread(target=self.run,args=(x,),daemon=True).start()
    def run(self,x):
        while True:
            def opened(ws):
                with self.lock:
                    self.status[x]='Connected'; self.started[x]=time.time(); self.gap[x]=False; self.ids.pop(x,None)
                    if x=='Coinbase': self.books={'buy':{},'sell':{}}; self.booktime=0
                ws.send(json.dumps({'type':'subscribe','product_ids':['BTC-USD'],'channels':['matches','heartbeat','level2_batch']} if x=='Coinbase' else {'method':'subscribe','params':{'channel':'trade','symbol':['BTC/USD'],'snapshot':False}}))
            def msg(ws,raw):
                d=json.loads(raw)
                with self.lock:
                    self.last[x]=time.time()
                    if d.get('type')=='error' or d.get('success') is False: self.status[x]='Subscription error'; return
                    if x=='Coinbase':
                        t=d.get('type')
                        if t=='snapshot': self.books={'buy':dict((float(p),float(q)) for p,q in d['bids']),'sell':dict((float(p),float(q)) for p,q in d['asks'])}; self.booktime=time.time()
                        if t=='l2update' and self.booktime:
                            for s,p,q in d['changes']:
                                p,q=float(p),float(q)
                                if q: self.books[s][p]=q
                                else: self.books[s].pop(p,None)
                            self.booktime=time.time()
                        rows=[(d['trade_id'],d['time'],d['price'],d['size'],'buy' if d['side']=='sell' else 'sell')] if t=='match' else []
                    else: rows=[(r['trade_id'],r['timestamp'],r['price'],r['qty'],r['side']) for r in d.get('data',[])] if d.get('channel')=='trade' and d.get('type')=='update' else []
                    for i,t,p,q,s in rows:
                        i=int(i); prev=self.ids.get(x)
                        if prev is not None and i<=prev: continue
                        if x=='Coinbase' and prev is not None and i>prev+1: self.gap[x]=True; self.started[x]=time.time()
                        self.ids[x]=i; self.trades[x].append((datetime.fromisoformat(t.replace('Z','+00:00')).timestamp(),x,float(p),float(q),s))
            def closed(*args):
                with self.lock: self.status[x]='Disconnected'
            ws=websocket.WebSocketApp('wss://ws-feed.exchange.coinbase.com' if x=='Coinbase' else 'wss://ws.kraken.com/v2',on_open=opened,on_message=msg,on_close=closed,on_error=closed)
            ws.run_forever(ping_interval=20,ping_timeout=10); closed(); time.sleep(3)
@st.cache_resource
def feed(): return Feed()
f=feed()
@st.cache_data(ttl=15)
def markets():
    out=[]
    for series in ['KXBTC15M','KXBTCD']:
        try:
            r=requests.get('https://api.elections.kalshi.com/trade-api/v2/markets',params={'series_ticker':series,'status':'open','limit':100},timeout=4); r.raise_for_status(); out.extend(r.json().get('markets',[]))
        except Exception: pass
    return out
st.title('₿ BTC Order Flow Terminal')
st.caption('Live exchange executions • research only • not official BRTI')
w=st.sidebar.selectbox('Rolling window (seconds)',[10,30,60,300,900,3600],index=2)
@st.fragment(run_every=1)
def dashboard():
    now=time.time()
    with f.lock:
        status=f.status.copy(); started=f.started.copy(); last=f.last.copy(); gaps=f.gap.copy()
        rows=[r for x,rs in f.trades.items() for r in rs if now-w<=r[0]<=now+2 and r[0]>=started.get(x,now)]; book={s:dict(v) for s,v in f.books.items()}; bt=f.booktime
    for x in f.trades:
        live=status.get(x)=='Connected' and now-last.get(x,0)<15
        st.caption(f'{x}: {"LIVE" if live else "STALE / CONNECTING"} | continuous collection {max(0,int(now-started.get(x,now)))}s'+(' | gap detected since connection' if gaps.get(x) else ''))
    ready=all(status.get(x)=='Connected' and now-last.get(x,0)<15 and now-started.get(x,now)>=w for x in f.trades)
    if not ready: st.warning('Partial window or disconnected feed. Counts are observed only; control signal withheld until both feeds warm up.')
    if not rows: st.info('Waiting for live trades…'); return
    d=pd.DataFrame(rows,columns=['time','exchange','price','btc','side']).sort_values('time'); d['usd']=d.price*d.btc
    b=d[d.side=='buy']; s=d[d.side=='sell']; bv=b.usd.sum(); sv=s.usd.sum(); share=bv/(bv+sv); delta=bv-sv
    cols=st.columns(4); cols[0].metric('Observed executions',len(d)); cols[1].metric('Aggressive buys',len(b)); cols[2].metric('Aggressive sells',len(s)); cols[3].metric('USD volume',f'${bv+sv:,.0f}')
    cols=st.columns(3); cols[0].metric('Buy volume',f'${bv:,.0f}'); cols[1].metric('Sell volume',f'${sv:,.0f}'); cols[2].metric('Volume delta',f'${delta:,.0f}')
    st.progress(float(share),text=f'Buy aggression {share:.1%} | Sell aggression {1-share:.1%}')
    moves=d.groupby('exchange').price.agg(['first','last']); change=((moves['last']/moves['first']-1)*100).median()
    bias='BUYER PRESSURE' if share>=.60 and change>0 else 'SELLER PRESSURE' if share<=.40 and change<0 else 'MIXED / POSSIBLE ABSORPTION'
    st.subheader(bias if ready else 'WAITING FOR FULL WINDOW'); st.caption(f'Median venue price change: {change:+.3f}% • heuristic, not calibrated prediction')
    d['utc']=pd.to_datetime(d.time,unit='s',utc=True); d['bucket']=d.utc.dt.floor('5s'); d['signed']=d.usd.where(d.side=='buy',-d.usd)
    st.bar_chart(d.pivot_table(index='bucket',columns='side',values='usd',aggfunc='sum',fill_value=0),color=['#16a34a','#ef4444'] if set(d.side)=={'buy','sell'} else None)
    st.line_chart(d.set_index('utc').signed.cumsum().rename('Window cumulative USD delta'))
    st.dataframe(d.groupby(['exchange','side']).agg(trades=('usd','size'),volume_usd=('usd','sum')),use_container_width=True)
    st.subheader('Large executions (at least $100,000)'); st.dataframe(d[d.usd>=100000][['utc','exchange','side','price','usd']].tail(20),hide_index=True)
    st.subheader('Coinbase resting liquidity')
    if now-bt<15 and book['buy'] and book['sell']:
        bid=max(book['buy']); ask=min(book['sell']); mid=(bid+ask)/2; buys=sum(p*q for p,q in book['buy'].items() if p>=mid*.999); sells=sum(p*q for p,q in book['sell'].items() if p<=mid*1.001)
        st.metric('Coinbase midpoint',f'${mid:,.2f}'); st.write(f'Within 0.1%: bids ${buys:,.0f} | asks ${sells:,.0f}'); st.caption('Resting orders can be cancelled. Midpoint is NOT BRTI.')
    else: st.info('Order book unavailable or stale.')
    st.subheader('Kalshi market context'); ms=markets()
    if ms:
        st.dataframe(pd.DataFrame([{k:m.get(k) for k in ['ticker','title','close_time','floor_strike','yes_bid_dollars','yes_ask_dollars']} for m in ms]),hide_index=True)
    else: st.info('Kalshi public market metadata unavailable.')
    st.caption('Official BRTI feed not connected. Check official settlement reference separately. No settlement prediction or trading execution.')
dashboard()

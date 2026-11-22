import streamlit as st
import pandas as pd
import time, json, threading, websocket, requests
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import math
from brti import BRTI
from pro import Trainer, snapshot, heatmap, features
import plotly.graph_objects as go
from engine import candles, advanced, advanced_forecast
from intelligence import simulation, footprint, log_forecast, history, settle

@st.cache_data(ttl=30)
def chart_signals():
    result={}
    for m in (1,5,15,60):
        try: result[m]=advanced(candles(m))
        except Exception: pass
    return result

st.set_page_config(page_title='BTC Intelligence X Pro', layout='wide')
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
@st.cache_resource
def brti_feed():
    try:
        cfg=st.secrets['kalshi']
        return BRTI(str(cfg['key_id']),str(cfg['private_key']))
    except Exception: return None
brti=brti_feed()
@st.cache_data(ttl=15)
def markets():
    out=[]
    for series in ['KXBTC15M','KXBTCD']:
        try:
            r=requests.get('https://api.elections.kalshi.com/trade-api/v2/markets',params={'series_ticker':series,'status':'open','limit':100},timeout=4); r.raise_for_status(); out.extend(r.json().get('markets',[]))
        except Exception: pass
    return out
st.title('₿ BTC Intelligence X Pro')
@st.cache_resource
def trainer(): return Trainer()
ml=trainer()
page=st.sidebar.radio('Workspace',['Live terminal','Performance lab','Machine learning lab','Liquidity history'])
st.caption('Live exchange executions • research only • optional authenticated BRTI')
w=st.sidebar.selectbox('Rolling window (seconds)',[10,30,60,300,900,3600],index=2)
st.sidebar.header('Kalshi target')
target=st.sidebar.number_input('Target BTC price ($)',min_value=0.0,value=0.0,step=0.01,format='%.2f',help='Enter the exact price to beat from Kalshi.')
local=ZoneInfo('America/Chicago')
mode=st.sidebar.radio('Expiration mode',['15-minute boundary','Hourly boundary','Custom (Central time)'])
now_local=datetime.now(local)
if mode=='Custom (Central time)':
    day=st.sidebar.date_input('Expiration date',now_local.date())
    clock=st.sidebar.time_input('Expiration time (CT)',(now_local+timedelta(minutes=15)).time().replace(second=0,microsecond=0),step=60)
    expiry=datetime.combine(day,clock,tzinfo=local)
else:
    minutes=15 if mode=='15-minute boundary' else 60
    boundary=(int(time.time())//(minutes*60)+1)*minutes*60
    expiry=datetime.fromtimestamp(boundary,local)
# Fixed on full rerun, not on the one-second fragment refresh.
@st.fragment(run_every=1)
def dashboard():
    now=time.time()
    with f.lock:
        status=f.status.copy(); started=f.started.copy(); last=f.last.copy(); gaps=f.gap.copy()
        allrows=[r for x,rs in f.trades.items() for r in rs if now-max(w,60)<=r[0]<=now+2 and r[0]>=started.get(x,now)]; rows=[r for r in allrows if r[0]>=now-w]; book={s:dict(v) for s,v in f.books.items()}; bt=f.booktime
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
    st.subheader('Target price monitor')
    remaining=expiry.timestamp()-now
    latest=d.groupby('exchange').tail(1)
    latest=latest[latest.time>=now-15]
    proxy=float(latest.price.median()) if len(latest)==2 else None
    c=st.columns(3)
    c[0].metric('Target',f'${target:,.2f}' if target else 'Set target in sidebar')
    c[1].metric('Time remaining',f'{int(max(0,remaining))//60:02d}:{int(max(0,remaining))%60:02d}')
    c[2].metric('Live spot proxy',f'${proxy:,.2f}' if proxy is not None else 'Unavailable')
    st.caption(f'Expiration: {expiry:%Y-%m-%d %I:%M %p %Z} | two-exchange median, NOT official BRTI')
    brti_status,tick=brti.read() if brti else ('Not configured or invalid credentials',{})
    st.caption('Official BRTI feed: '+brti_status+(' | stale or waiting' if not tick else ''))
    if tick:
        st.metric('Official BRTI',f"${tick['price']:,.2f}")
        for title,k in [('Trailing 60-second average','avg'),('Quarter-hour final-minute average','quarter')]:
            a=tick.get(k,{})
            if 'value' in a: st.write(f"{title}: ${float(a['value']):,.2f} ({a.get('window_size','?')} observations)")
        if proxy is not None: st.write(f"BRTI minus spot proxy: ${tick['price']-proxy:+,.2f}")
        proxy=tick['price']
    st.caption('Forecast reference: '+('official BRTI tick' if tick else 'exchange proxy; NOT official BRTI'))
    if target and proxy is not None and remaining>0:
        distance=proxy-target
        st.metric('Distance from target',f'${distance:+,.2f}',f'{distance/target:+.3%}')
        signals=chart_signals()
        if signals:
            st.subheader('Multi-timeframe chart analysis')
            st.dataframe(pd.DataFrame([{'Timeframe':f'{m}m','Bias':'Bullish' if v['score']>0.15 else 'Bearish' if v['score']<-.15 else 'Mixed',**{k:round(v[k],3) for k in ('RSI','MACD','VWAP','EMA9','EMA21','EMA50','EMA200','support','resistance','lower','upper','ATR','ADX','relative_volume','momentum')},'Regime':v['regime']} for m,v in signals.items()]),hide_index=True)
        check_time=time.time()
        checks={}
        for m in (1,5,15,60):
            age=check_time-signals[m]['last_time'] if m in signals else float('inf')
            checks[f'{m}m chart']=(m in signals and 0<=age<=m*120+60, f'Age {age:.0f}s' if m in signals else 'Missing chart')
        for x in f.trades:
            age=now-last.get(x,0); warm=max(0,now-started.get(x,now))
            checks[x]=(status.get(x)=='Connected' and age<15 and warm>=max(w,60), f'{status.get(x,"Connecting")} | message age {age:.1f}s | collection {warm:.0f}/{max(w,60)}s')
        checks['Coinbase order book']=(bool(book['buy'] and book['sell']) and now-bt<15, f'Age {now-bt:.1f}s' if bt else 'No snapshot received')
        with st.expander('Prediction readiness — connection diagnostics',expanded=not all(v[0] for v in checks.values())):
            for name,(ok,detail) in checks.items():
                st.write(f'{"PASS" if ok else "WAIT"} | {name}: {detail}')
        if all(v[0] for v in checks.values()):
            mid=(max(book['buy'])+min(book['sell']))/2
            bidv=sum(p*q for p,q in book['buy'].items() if p>=mid*.999); askv=sum(p*q for p,q in book['sell'].items() if p<=mid*1.001)
            flow=2*share-1; imbalance=(bidv-askv)/(bidv+askv) if bidv+askv else 0
            flows={}; flowtable=[]
            for seconds in (10,30,60):
                rr=[r for r in allrows if r[0]>=now-seconds]; buy=sum(r[2]*r[3] for r in rr if r[4]=='buy'); sell=sum(r[2]*r[3] for r in rr if r[4]=='sell'); flows[seconds]=(buy-sell)/(buy+sell) if buy+sell else 0
                flowtable.append({'Seconds':seconds,'Executions':len(rr),'Buy USD':round(buy),'Sell USD':round(sell),'Delta USD':round(buy-sell),'Bias':round(flows[seconds],3)})
            label,projected,margin,score,sigma,chartscore,flowscore=advanced_forecast(proxy,target,remaining,signals,flows,imbalance)
            st.dataframe(pd.DataFrame(flowtable),hide_index=True)
            st.write(f'Chart bias {chartscore:+.2f} | Multi-window flow {flowscore:+.2f} | Book imbalance {imbalance:+.2f}')
            st.subheader('Final directional estimate: '+label)
            probability, quantiles=simulation(proxy,target,remaining,score,signals[1]['vol'])
            st.metric('Simulated ABOVE frequency',f'{probability:.1%}')
            st.caption('512 conditional simulated paths; not calibrated odds or official settlement. During the final minute, elapsed averaging observations are missing.')
            st.write(f'Scenario 10th / 50th / 90th percentiles: ${quantiles[0]:,.2f} / ${quantiles[1]:,.2f} / ${quantiles[2]:,.2f}')
            st.dataframe(pd.DataFrame({'Engine':['Chart','Order flow','Liquidity','Simulation'],'Score':[chartscore,flowscore,imbalance,2*probability-1]}),hide_index=True)
            log_forecast(expiry.timestamp(),target,remaining,probability,proxy)
            cc=st.columns(3); cc[0].metric('Estimated expiry scenario',f'${projected:,.2f}'); cc[1].metric('Combined market bias',f'{score:+.2f}'); cc[2].metric('Volatility scale',f'${sigma:,.2f}')
            st.write(f'Illustrative expiry range (one volatility scale): ${projected-sigma:,.2f} – ${projected+sigma:,.2f}')
            if abs(margin)<.75 or chartscore*flowscore<-.10: st.warning('NO TRADE research signal: weak target separation or conflicting evidence.')
            if remaining<=60: st.warning('Final settlement averaging window: the scenario does not incorporate elapsed averaging observations; especially uncertain.')
            st.caption('Unvalidated heuristic, not a win probability. Weights: 55% chart signals, 35% aggressive volume, 10% resting liquidity. Price scenario uses the reference displayed above, not a verified settlement average.')
        else: st.info('Final estimate withheld. See readiness diagnostics above for the exact missing requirements.')
    elif remaining<=0: st.warning('Expiration passed. Set a new target or expiration. Official settlement is not available here.')
    elif not target: st.info('Enter the Kalshi price to beat in the sidebar.')
    st.subheader(bias if ready else 'WAITING FOR FULL WINDOW'); st.caption(f'Median venue price change: {change:+.3f}% • heuristic, not calibrated prediction')
    d['utc']=pd.to_datetime(d.time,unit='s',utc=True); d['bucket']=d.utc.dt.floor('5s'); d['signed']=d.usd.where(d.side=='buy',-d.usd)
    st.bar_chart(d.pivot_table(index='bucket',columns='side',values='usd',aggfunc='sum',fill_value=0),color=['#16a34a','#ef4444'] if set(d.side)=={'buy','sell'} else None)
    st.line_chart(d.set_index('utc').signed.cumsum().rename('Window cumulative USD delta'))
    st.dataframe(d.groupby(['exchange','side']).agg(trades=('usd','size'),volume_usd=('usd','sum')),use_container_width=True)
    st.subheader('Large executions (at least $100,000)'); st.dataframe(d[d.usd>=100000][['utc','exchange','side','price','usd']].tail(20),hide_index=True)
    st.subheader('Price-level footprint — $10 buckets')
    fp=footprint(d)
    st.dataframe(fp.assign(delta=fp.buy-fp.sell),use_container_width=True)
    st.bar_chart(fp.sort_index())
    st.subheader('Coinbase resting liquidity')
    if now-bt<15 and book['buy'] and book['sell']:
        bid=max(book['buy']); ask=min(book['sell']); mid=(bid+ask)/2; snapshot(book,mid); buys=sum(p*q for p,q in book['buy'].items() if p>=mid*.999); sells=sum(p*q for p,q in book['sell'].items() if p<=mid*1.001)
        st.dataframe(pd.DataFrame([{'side':side,'price':p,'USD':p*q} for side,levels in book.items() for p,q in sorted(levels.items(),key=lambda z:abs(z[0]-mid))[:25]]),hide_index=True)
        st.metric('Coinbase midpoint',f'${mid:,.2f}'); st.write(f'Within 0.1%: bids ${buys:,.0f} | asks ${sells:,.0f}'); st.caption('Resting orders can be cancelled. Midpoint is NOT BRTI.')
    else: st.info('Order book unavailable or stale.')
    st.subheader('Kalshi market context'); ms=markets()
    if ms:
        st.dataframe(pd.DataFrame([{k:m.get(k) for k in ['ticker','title','close_time','floor_strike','yes_bid_dollars','yes_ask_dollars']} for m in ms]),hide_index=True)
    else: st.info('Kalshi public market metadata unavailable.')
    st.caption('Official BRTI feed not connected. Check official settlement reference separately. Directional heuristic only; no official settlement prediction or trading execution.')
if page=='Live terminal':
    dashboard()
elif page=='Machine learning lab':
    st.header('Machine learning lab'); st.write(ml.status)
    st.caption('Seven days of Coinbase candles; six-hour retraining while server runs. Purged chronological holdout. Models predict future close direction, not Kalshi settlement.')
    for h,(models,metrics) in ml.models.items():
        st.subheader(f'{h}-minute model'); st.json(metrics)
        if metrics['passes']:
            try:
                xx=features(candles(1)).dropna().tail(1); p=sum(m.predict_proba(xx)[0,1] for m in models)/len(models); st.metric('Uncalibrated direction estimate',f'{p:.1%} UP')
            except Exception as e: st.warning(str(e))
        else: st.warning('Candidate failed baseline; prediction withheld.')
elif page=='Liquidity history':
    st.header('Historical liquidity heatmap'); st.caption('Collected Coinbase snapshots while live terminal is open; local SQLite. Red = asks, blue = bids. No backfilled history.')
    h=heatmap()
    if h.empty: st.info('Open live terminal to collect snapshots.')
    else: st.plotly_chart(go.Figure(go.Heatmap(x=h.columns,y=h.index,z=h.values,colorscale='RdBu',zmid=0)),use_container_width=True)
else:
    st.header('Performance lab')
    h=history(); st.caption('Local SQLite; hosting resets may erase records. Outcomes must be entered from official contract settlement. No automatic settlement verification.')
    done=h[h.outcome.notna()]
    if len(done):
        st.metric('Recorded direction accuracy',f'{((done.estimate>=.5)==done.outcome.astype(bool)).mean():.1%}')
        st.metric('Brier score',f'{((done.estimate-done.outcome)**2).mean():.3f}')
        st.dataframe(done.groupby('bucket').apply(lambda a: pd.Series({'count':len(a),'accuracy':((a.estimate>=.5)==a.outcome.astype(bool)).mean()})))
    st.dataframe(h,hide_index=True)
    pending=h[(h.expiry<time.time()) & h.outcome.isna()][['expiry','target']].drop_duplicates()
    if len(pending):
        choices=list(pending.itertuples(index=False,name=None)); pick=st.selectbox('Expired target',choices,format_func=lambda a:f'{datetime.fromtimestamp(a[0],local)} | ${a[1]:,.2f}')
        result=st.radio('Official settlement result (verify manually)',['ABOVE / YES','BELOW / NO'])
        if st.button('Record verified outcome'):
            settle(*pick,int(result=='ABOVE / YES')); st.rerun()


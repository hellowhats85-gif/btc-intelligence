import time, sqlite3, os, threading
import numpy as np
import pandas as pd
import requests
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import brier_score_loss

DB=os.getenv('BTC_PRO_DB','btc_pro.sqlite')
def connect():
    c=sqlite3.connect(DB,timeout=15); c.execute('CREATE TABLE IF NOT EXISTS depth(t INTEGER, price INTEGER, side TEXT, usd REAL, PRIMARY KEY(t,price,side))'); return c

def snapshot(book,mid):
    t=int(time.time()//5)*5; rows={}
    for side,levels in book.items():
        for p,q in levels.items():
            if abs(p-mid)<=mid*.005:
                k=(t,int(p//10)*10,side); rows[k]=rows.get(k,0)+p*q
    with connect() as c:
        c.executemany('INSERT OR IGNORE INTO depth VALUES(?,?,?,?)',[(*k,v) for k,v in rows.items()]); c.execute('DELETE FROM depth WHERE t<?',(t-7*86400,))

def heatmap():
    with connect() as c: d=pd.read_sql_query('SELECT * FROM depth WHERE t>? ORDER BY t',c,params=(time.time()-1800,))
    if d.empty:return d
    d['usd']=d.usd.where(d.side=='buy',-d.usd); d['t']=pd.to_datetime(d.t,unit='s',utc=True); return d.pivot_table(index='price',columns='t',values='usd',aggfunc='sum')

def features(d):
    c=d.close; r=np.log(c).diff(); x=pd.DataFrame(index=d.index)
    for n in (1,3,5,15,30,60):
        x[f'r{n}']=np.log(c/c.shift(n)); x[f'v{n}']=r.rolling(max(n,3)).std()
    x['volume']=d.volume/d.volume.rolling(30).median().replace(0,np.nan); x['range']=(d.high-d.low)/c
    return x.replace([np.inf,-np.inf],np.nan)

def download(days=7):
    end=int(time.time()//60)*60; blocks=[]
    for i in range(int(np.ceil(days*1440/300))):
        a=end-(i+1)*300*60; b=end-i*300*60
        r=requests.get('https://api.exchange.coinbase.com/products/BTC-USD/candles',params={'granularity':60,'start':pd.Timestamp(a,unit='s',tz='UTC').isoformat(),'end':pd.Timestamp(b,unit='s',tz='UTC').isoformat()},timeout=10); r.raise_for_status(); blocks.extend(r.json()); time.sleep(.15)
    d=pd.DataFrame(blocks,columns=['time','low','high','open','close','volume']).drop_duplicates('time').sort_values('time'); return d[d.time<end].reset_index(drop=True)

def train(d):
    x=features(d); out={}
    for h in (15,60):
        # direction of future close, not target-specific settlement
        future=d.close.shift(-h); valid=x.notna().all(axis=1)&future.notna()&(d.time.shift(-h)-d.time==h*60)&(d.time-d.time.shift(60)==3600)
        X=x[valid]; y=(future[valid]>d.close[valid]).astype(int); cut=int(len(X)*.75)
        if cut<300 or len(X)-cut<100 or y.iloc[:cut-h].nunique()<2:continue
        models=[make_pipeline(StandardScaler(),LogisticRegression(max_iter=500)),HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,random_state=42),RandomForestClassifier(n_estimators=80,max_depth=6,n_jobs=1,random_state=42)]
        for m in models:m.fit(X.iloc[:cut-h],y.iloc[:cut-h])
        p=np.mean([m.predict_proba(X.iloc[cut:])[:,1] for m in models],axis=0); truth=y.iloc[cut:]; base=float(y.iloc[:cut-h].mean()); loss=brier_score_loss(truth,p); baseline=brier_score_loss(truth,np.full(len(truth),base)); out[h]=(models,{'accuracy':float(((p>=.5)==truth).mean()),'brier':loss,'baseline_brier':baseline,'test_rows':len(truth),'passes':loss<baseline})
    return out

class Trainer:
    def __init__(self): self.models={}; self.status='Waiting'; self.updated=0; self.lock=threading.Lock(); threading.Thread(target=self.run,daemon=True).start()
    def run(self):
        while True:
            try:
                self.status='Training'; candidates=train(download()); self.models=candidates; self.updated=time.time(); self.status='Evaluated candidate models'
            except Exception as e:self.status=f'Training unavailable: {e}'
            time.sleep(21600)

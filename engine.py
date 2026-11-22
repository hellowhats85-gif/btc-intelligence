import math
import pandas as pd
import requests

def candles(minutes):
    # Coinbase native granularities; aggregate 1m for 1m, native for others.
    r=requests.get('https://api.exchange.coinbase.com/products/BTC-USD/candles',params={'granularity':minutes*60},timeout=5); r.raise_for_status()
    d=pd.DataFrame(r.json(),columns=['time','low','high','open','close','volume']).sort_values('time').drop_duplicates('time')
    import time
    return d[d.time+minutes*60<=time.time()].reset_index(drop=True)

def analyze(d):
    if len(d)<201: raise ValueError('Insufficient candles')
    c=d.close; p=float(c.iloc[-1]); e=lambda n:c.ewm(span=n,adjust=False).mean(); diff=c.diff(); gain=diff.clip(lower=0).ewm(alpha=1/14,adjust=False).mean(); loss=(-diff.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean(); rsi=float((100*gain/(gain+loss)).iloc[-1]); rsi=rsi if math.isfinite(rsi) else 50
    mac=e(12)-e(26); hist=float((mac-mac.ewm(span=9,adjust=False).mean()).iloc[-1]); tail=d.tail(20); vwap=float((((tail.high+tail.low+tail.close)/3)*tail.volume).sum()/tail.volume.sum()) if tail.volume.sum()>0 else p
    mean=float(c.tail(20).mean()); sd=float(c.tail(20).std()); sign=lambda x: 1 if x>0 else -1 if x<0 else 0
    votes=[sign(float((e(9)-e(21)).iloc[-1])),sign(float((e(50)-e(200)).iloc[-1])),sign(hist),sign(p-vwap),sign(p-mean),1 if 52<rsi<75 else -1 if 25<rsi<48 else 0]
    return dict(score=sum(votes)/len(votes),RSI=rsi,MACD=hist,VWAP=vwap,EMA9=float(e(9).iloc[-1]),EMA21=float(e(21).iloc[-1]),EMA50=float(e(50).iloc[-1]),EMA200=float(e(200).iloc[-1]),support=float(tail.low.min()),resistance=float(tail.high.max()),lower=mean-2*sd,upper=mean+2*sd,vol=float(c.pct_change().tail(60).std()),last_time=float(d.time.iloc[-1]))

def forecast(price,target,remaining,signals,flow,book):
    weights={1:.4,5:.3,15:.2,60:.1} if remaining<=1200 else {1:.15,5:.25,15:.35,60:.25}
    chart=sum(weights[k]*signals[k]['score'] for k in weights)
    score=.55*chart+.35*flow+.10*book
    sigma=max(signals[1]['vol'],.00001)*math.sqrt(max(remaining,1)/60)*price
    projected=price+score*sigma*.65
    margin=(projected-target)/max(sigma,1e-9)
    return ('UP / ABOVE' if margin>0 else 'DOWN / BELOW' if margin<0 else 'AT TARGET', projected, margin, score, sigma)

def advanced(d):
    a=analyze(d); c=d.close; h=d.high; l=d.low; v=d.volume
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    atr=tr.ewm(alpha=1/14,adjust=False).mean(); up=h.diff(); dn=-l.diff()
    plus=100*up.where((up>dn)&(up>0),0).ewm(alpha=1/14,adjust=False).mean()/atr.replace(0,float('nan'))
    minus=100*dn.where((dn>up)&(dn>0),0).ewm(alpha=1/14,adjust=False).mean()/atr.replace(0,float('nan'))
    adx=(100*(plus-minus).abs()/(plus+minus).replace(0,float('nan'))).ewm(alpha=1/14,adjust=False).mean()
    p=float(c.iloc[-1]); scale=max(float(atr.iloc[-1]),p*1e-6)
    clip=lambda x:max(-1,min(1,float(x)))
    momentum=clip((p-c.iloc[-6])/(3*scale)); trend=clip((a['EMA9']-a['EMA21'])/scale)
    breakout=clip((p-float(c.tail(20).mean()))/(2*scale)); strength=float(adx.iloc[-1]); strength=strength if math.isfinite(strength) else 0
    a.update(ATR=scale,ADX=strength,relative_volume=float(v.iloc[-1]/max(v.tail(30).median(),1e-9)),momentum=momentum,regime='Trending' if strength>=25 else 'Range / weak trend')
    a['score']=.35*a['score']+.30*trend+.20*momentum+.15*breakout
    return a

def advanced_forecast(price,target,remaining,signals,flows,book):
    weights={1:.45,5:.30,15:.17,60:.08} if remaining<=900 else {1:.15,5:.25,15:.35,60:.25}
    chart=sum(weights[k]*signals[k]['score'] for k in weights)
    flow=sum(w*flows[k] for k,w in [(10,.15),(30,.35),(60,.50)])
    score=.55*chart+.35*flow+.10*book
    # Approximate average-price horizon; Brownian variance for final 60 seconds.
    t=max(remaining,1); a=min(t,60); effective=max(t-2*a/3,1)
    sigma=max(signals[1]['vol'],.00001)*math.sqrt(effective/60)*price
    projected=price+score*sigma*.65
    margin=(projected-target)/sigma
    label='UP / ABOVE' if margin>0 else 'DOWN / BELOW' if margin<0 else 'AT TARGET'
    return label,projected,margin,score,sigma,chart,flow

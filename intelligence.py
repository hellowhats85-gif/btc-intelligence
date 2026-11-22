import math, sqlite3, time
import numpy as np
import pandas as pd

def simulation(price,target,seconds,score,vol,seed=42):
    # Conditional scenario, not calibrated. Simulate the final averaging window.
    steps=max(1,min(3600,int(math.ceil(seconds)))); dt=seconds/steps
    rng=np.random.default_rng(seed); sigma=max(vol,1e-6)/math.sqrt(60)
    drift=score*sigma*.65/math.sqrt(max(seconds,1))
    paths=np.cumsum(rng.normal((drift-.5*sigma*sigma)*dt,sigma*math.sqrt(dt),(512,steps)),axis=1)
    avg=(price*np.exp(paths[:,-max(1,min(steps,math.ceil(60/dt))):])).mean(axis=1)
    return float(np.mean(avg>=target)),np.quantile(avg,[.1,.5,.9])

def footprint(d):
    a=d.copy(); a['level']=(a.price/10).round()*10
    return a.pivot_table(index='level',columns='side',values='usd',aggfunc='sum',fill_value=0).reindex(columns=['buy','sell'],fill_value=0).sort_index(ascending=False)

def log_forecast(expiry,target,remaining,prob,price):
    if remaining<=60 or not math.isfinite(prob): return
    bucket=next((b for b in [900,600,300,180,60] if b<=remaining<b+8),None)
    if bucket is None:return
    with sqlite3.connect('predictions.db',timeout=3) as db:
        db.execute('CREATE TABLE IF NOT EXISTS forecasts(expiry REAL,target REAL,bucket INTEGER,created REAL,estimate REAL,price REAL,outcome INTEGER,PRIMARY KEY(expiry,target,bucket))')
        db.execute('INSERT OR IGNORE INTO forecasts VALUES(?,?,?,?,?,?,NULL)',(expiry,target,bucket,time.time(),prob,price))

def history():
    with sqlite3.connect('predictions.db',timeout=3) as db:
        db.execute('CREATE TABLE IF NOT EXISTS forecasts(expiry REAL,target REAL,bucket INTEGER,created REAL,estimate REAL,price REAL,outcome INTEGER,PRIMARY KEY(expiry,target,bucket))')
        return pd.read_sql_query('SELECT * FROM forecasts ORDER BY created DESC LIMIT 1000',db)

def settle(expiry,target,outcome):
    if expiry>time.time():raise ValueError('Market has not expired')
    with sqlite3.connect('predictions.db') as db:db.execute('UPDATE forecasts SET outcome=? WHERE expiry=? AND target=? AND outcome IS NULL',(outcome,expiry,target))

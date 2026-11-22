import time, json, threading, base64, math
import websocket
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

class BRTI:
    def __init__(self, key_id, pem):
        self.lock=threading.Lock(); self.tick={}; self.status='Connecting'
        self.key_id=key_id
        self.key=serialization.load_pem_private_key(pem.encode(),password=None)
        threading.Thread(target=self.run,daemon=True).start()
    def headers(self):
        ts=str(int(time.time()*1000)); signature=self.key.sign((ts+'GET/trade-api/ws/v2').encode(),padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=32),hashes.SHA256())
        return {'KALSHI-ACCESS-KEY':self.key_id,'KALSHI-ACCESS-TIMESTAMP':ts,'KALSHI-ACCESS-SIGNATURE':base64.b64encode(signature).decode()}
    def ingest(self, raw):
        d=json.loads(raw); m=d.get('msg',{})
        with self.lock:
            if d.get('type')=='error': self.status='Subscription rejected; check entitlement'; self.tick={}; return
            if d.get('type')!='cfbenchmarks_value' or m.get('index_id')!='BRTI': return
            r=json.loads(m['data']); p=float(r['value']); ts=float(r['time'])/1000
            if r.get('id')!='BRTI' or not math.isfinite(p) or p<=0 or abs(time.time()-ts)>15: return
            if ts<=self.tick.get('ts',0): return
            self.tick={'price':p,'ts':ts,'avg':m.get('avg_60s_data',{}),'quarter':m.get('last_60s_windowed_average_15min',{})}; self.status='Live'
    def run(self):
        while True:
            def opened(ws): ws.send(json.dumps({'id':1,'cmd':'subscribe','params':{'channels':['cfbenchmarks_value'],'index_ids':['BRTI']}}))
            def message(ws,raw):
                try: self.ingest(raw)
                except (ValueError,TypeError,KeyError): pass
            def closed(*args):
                with self.lock: self.tick={}; self.status='Disconnected or unauthorized'
            ws=websocket.WebSocketApp('wss://external-api-ws.kalshi.com/trade-api/ws/v2',header=self.headers,on_open=opened,on_message=message,on_error=closed,on_close=closed)
            try: ws.run_forever(ping_interval=20,ping_timeout=10)
            except Exception: pass
            closed(); time.sleep(10)
    def read(self):
        with self.lock: return self.status,dict(self.tick) if time.time()-self.tick.get('ts',0)<5 else {}

import pandas as pd

def merge_live(history,trades,start,end,minutes):
    step=minutes*60
    first=(int(start)//step+1)*step
    frame=pd.DataFrame(trades,columns=['ts','venue','price','qty','side']).sort_values('ts')
    frame['time']=(frame.ts//step*step).astype('int64')
    frame=frame[(frame.time>=first)&(frame.time+step<=end)]
    if frame.empty: return history
    live=frame.groupby('time').agg(low=('price','min'),high=('price','max'),open=('price','first'),close=('price','last'),volume=('qty','sum')).reset_index()
    return pd.concat([history,live],ignore_index=True).drop_duplicates('time',keep='last').sort_values('time').reset_index(drop=True)

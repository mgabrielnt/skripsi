import sys, os
from datetime import date, timedelta
import pandas as pd
import yfinance as yf
from utils.db import get_conn

START = '2019-01-01'  # ubah sesuai kebutuhan

def load_tickers(path='conf/tickers.txt'):
    with open(path,'r',encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip() and not l.startswith('#')]

def normalize_yf(df: pd.DataFrame) -> pd.DataFrame:
    # Jika MultiIndex (kadang muncul di yfinance), flatten kolom
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    # Standarkan nama kolom 'Adj Close'
    rename_map = {'AdjClose':'Adj Close', 'Adj Close*':'Adj Close'}
    df = df.rename(columns=rename_map)
    # Jika tetap tidak ada 'Adj Close', pakai 'Close' sebagai fallback
    if 'Adj Close' not in df.columns and 'Close' in df.columns:
        df['Adj Close'] = df['Close']
    return df

def upsert_prices(df, sid, conn):
    sql = '''
    INSERT INTO prices (sid, date, open, high, low, close, adj_close, volume, ret1d, rv20d)
    VALUES (%(sid)s, %(date)s, %(Open)s, %(High)s, %(Low)s, %(Close)s, %(Adj Close)s, %(Volume)s, %(ret1d)s, %(rv20d)s)
    ON CONFLICT (sid, date) DO UPDATE
    SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
        adj_close=EXCLUDED.adj_close, volume=EXCLUDED.volume, ret1d=EXCLUDED.ret1d, rv20d=EXCLUDED.rv20d;
    '''
    with conn.cursor() as cur:
        rows = df.to_dict('records')
        for r in rows:
            r['sid'] = sid
            cur.execute(sql, r)
    conn.commit()

def main():
    tickers = load_tickers()
    end = date.today() + timedelta(days=1)
    conn = get_conn()
    for sid in tickers:
        print('Downloading', sid, '...')
        df = yf.download(
            sid,
            start=START,
            end=str(end),
            progress=False,
            auto_adjust=False,   # penting: pastikan 'Adj Close' ada
            actions=False,
            group_by='column',
            threads=True
        )
        if df is None or df.empty:
            print('! no data for', sid); continue
        df = normalize_yf(df).reset_index()  # Date -> column 'Date'
        # hitung ret1d dari Adj Close (fallback sudah disiapkan)
        df['ret1d'] = df['Adj Close'].pct_change().fillna(0.0)
        # rv20d (stdev rolling 20) atas ret1d
        df['rv20d'] = df['ret1d'].rolling(20).std().fillna(0.0)
        # rapikan kolom & tipe
        df = df.rename(columns={'Date':'date'})
        keep = ['date','Open','High','Low','Close','Adj Close','Volume','ret1d','rv20d']
        missing = [c for c in keep if c not in df.columns]
        if missing:
            print(f'! missing columns for {sid}:', missing); 
            continue
        df = df[keep].copy()
        upsert_prices(df, sid, conn)
        print('OK', sid, len(df), 'rows')
    conn.close()

if __name__ == '__main__':
    main()

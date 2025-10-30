import pandas as pd, numpy as np, yaml, os
from datetime import timedelta
from sqlalchemy import create_engine, text
from utils.db import get_conn
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD
from ta.volatility import AverageTrueRange

PARAMS = yaml.safe_load(open('conf/params.yaml','r',encoding='utf-8'))

def ensure_table(conn):
    sql = '''
    CREATE TABLE IF NOT EXISTS features_daily(
      sid varchar(16) NOT NULL,
      date date NOT NULL,
      -- teknikal
      logret_1d numeric(12,6), logret_5d numeric(12,6), logret_20d numeric(12,6),
      rv20d numeric(12,6),
      rsi14 numeric(8,4),
      atr14 numeric(12,6),
      macd numeric(12,6), macd_signal numeric(12,6), macd_hist numeric(12,6),
      stoch_k numeric(12,6), stoch_d numeric(12,6),
      vol_z60 numeric(12,6),
      dow smallint, eom boolean,
      -- fundamental (as-of)
      roe numeric(12,6), roa numeric(12,6), npm numeric(12,6),
      der numeric(12,6), dar numeric(12,6),
      per numeric(12,6), pbv numeric(12,6), eps numeric(18,6),
      sales_growth numeric(12,6), profit_growth numeric(12,6),
      -- primary key
      CONSTRAINT pk_features_daily PRIMARY KEY (sid, date)
    );
    '''
    with conn.cursor() as cur:
        cur.execute(sql); conn.commit()

def load_prices(conn, sid):
    q = '''SELECT date, open, high, low, close, adj_close, volume, rv20d
           FROM prices WHERE sid=%s ORDER BY date;'''
    return pd.read_sql(q, conn, params=[sid], parse_dates=['date'])

def add_technical(df):
    df = df.copy()
    df['logret_1d']  = np.log(df['adj_close']).diff().fillna(0.0)
    df['logret_5d']  = np.log(df['adj_close']).diff(5)
    df['logret_20d'] = np.log(df['adj_close']).diff(20)

    # TA lib expects caps columns; provide series
    rsi = RSIIndicator(close=df['close'], window=PARAMS['features']['rsi_period'])
    df['rsi14'] = rsi.rsi()

    atr = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=PARAMS['features']['atr_period'])
    df['atr14'] = atr.average_true_range()

    macd = MACD(close=df['close'],
                window_slow=PARAMS['features']['macd_slow'],
                window_fast=PARAMS['features']['macd_fast'],
                window_sign=PARAMS['features']['macd_signal'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    df['macd_hist'] = macd.macd_diff()

    st = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'], window=PARAMS['features']['stoch_k'], smooth_window=PARAMS['features']['stoch_d'])
    df['stoch_k'] = st.stoch()
    df['stoch_d'] = st.stoch_signal()

    df['vol_z60'] = (df['volume'] - df['volume'].rolling(PARAMS['features']['vol_z_window']).mean()) \
                      / (df['volume'].rolling(PARAMS['features']['vol_z_window']).std() + 1e-9)

    df['dow'] = df['date'].dt.dayofweek.astype(int)  # 0=Mon
    df['eom'] = (df['date'] + pd.offsets.Day(1)).dt.day == 1

    return df

def load_funda_asof(conn, sid, dates, lag_days):
    q = '''SELECT sid, period_end, announce_date,
                  roe, roa, npm, der, dar, per, pbv, eps, sales_growth, profit_growth
           FROM fundamentals WHERE sid=%s ORDER BY announce_date;'''
    f = pd.read_sql(q, conn, params=[sid], parse_dates=['announce_date','period_end'])
    if f.empty:
        return pd.DataFrame({'date': dates})
    # eligible date = feature date - lag_days
    aux = pd.DataFrame({'date': dates})
    aux['elig'] = aux['date'] - pd.to_timedelta(lag_days, unit='D')
    f = f.sort_values('announce_date')
    # merge_asof: untuk setiap elig, ambil baris funda dgn announce_date <= elig
    m = pd.merge_asof(aux.sort_values('elig'), f.rename(columns={'announce_date':'ts'}).sort_values('ts'),
                      left_on='elig', right_on='ts', direction='backward')
    # keep only needed cols + original date
    cols = ['date','roe','roa','npm','der','dar','per','pbv','eps','sales_growth','profit_growth']
    return m[cols]

def upsert_features(conn, df_out, sid):
    sql = '''
    INSERT INTO features_daily
    (sid, date, logret_1d, logret_5d, logret_20d, rv20d, rsi14, atr14, macd, macd_signal, macd_hist,
     stoch_k, stoch_d, vol_z60, dow, eom,
     roe, roa, npm, der, dar, per, pbv, eps, sales_growth, profit_growth)
    VALUES
    (%(sid)s, %(date)s, %(logret_1d)s, %(logret_5d)s, %(logret_20d)s, %(rv20d)s, %(rsi14)s, %(atr14)s, %(macd)s, %(macd_signal)s, %(macd_hist)s,
     %(stoch_k)s, %(stoch_d)s, %(vol_z60)s, %(dow)s, %(eom)s,
     %(roe)s, %(roa)s, %(npm)s, %(der)s, %(dar)s, %(per)s, %(pbv)s, %(eps)s, %(sales_growth)s, %(profit_growth)s)
    ON CONFLICT (sid, date) DO UPDATE SET
      logret_1d=EXCLUDED.logret_1d, logret_5d=EXCLUDED.logret_5d, logret_20d=EXCLUDED.logret_20d,
      rv20d=EXCLUDED.rv20d, rsi14=EXCLUDED.rsi14, atr14=EXCLUDED.atr14,
      macd=EXCLUDED.macd, macd_signal=EXCLUDED.macd_signal, macd_hist=EXCLUDED.macd_hist,
      stoch_k=EXCLUDED.stoch_k, stoch_d=EXCLUDED.stoch_d,
      vol_z60=EXCLUDED.vol_z60, dow=EXCLUDED.dow, eom=EXCLUDED.eom,
      roe=EXCLUDED.roe, roa=EXCLUDED.roa, npm=EXCLUDED.npm, der=EXCLUDED.der, dar=EXCLUDED.dar,
      per=EXCLUDED.per, pbv=EXCLUDED.pbv, eps=EXCLUDED.eps,
      sales_growth=EXCLUDED.sales_growth, profit_growth=EXCLUDED.profit_growth;
    '''
    with conn.cursor() as cur:
        for r in df_out.to_dict('records'):
            r['sid'] = sid
            cur.execute(sql, r)
    conn.commit()

def main():
    lag = int(PARAMS['features']['funda_lag_days'])
    with get_conn() as conn:
        ensure_table(conn)
        # tickers dari conf
        sids = [l.strip() for l in open('conf/tickers.txt','r',encoding='utf-8') if l.strip() and not l.startswith('#')]
        for sid in sids:
            print('features for', sid)
            px = load_prices(conn, sid)
            if px.empty: 
                print('  no prices, skip'); continue
            df = px.rename(columns=str.lower)
            df = add_technical(df)
            # as-of fundamental
            f_asof = load_funda_asof(conn, sid, df['date'], lag)
            out = df.merge(f_asof, on='date', how='left')
            # pilih & cast kolom
            cols = ['date','logret_1d','logret_5d','logret_20d','rv20d','rsi14','atr14','macd','macd_signal','macd_hist',
                    'stoch_k','stoch_d','vol_z60','dow','eom',
                    'roe','roa','npm','der','dar','per','pbv','eps','sales_growth','profit_growth']
            out = out[cols].copy()
            upsert_features(conn, out, sid)
            print('  upserted', sid, len(out), 'rows')
    print('DONE')

if __name__ == '__main__':
    main()

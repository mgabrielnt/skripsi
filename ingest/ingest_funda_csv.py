import sys, os, glob
import pandas as pd
from utils.db import get_conn

REQ = ['sid','period_end','announce_date','roe','roa','npm','der','dar','per','pbv','eps','sales_growth','profit_growth']

def upsert(df, conn):
    sql = '''
    INSERT INTO fundamentals
    (sid, period_end, announce_date, roe, roa, npm, der, dar, per, pbv, eps, sales_growth, profit_growth)
    VALUES (%(sid)s, %(period_end)s, %(announce_date)s, %(roe)s, %(roa)s, %(npm)s, %(der)s, %(dar)s, %(per)s, %(pbv)s, %(eps)s, %(sales_growth)s, %(profit_growth)s)
    ON CONFLICT (sid, period_end) DO UPDATE
    SET announce_date=EXCLUDED.announce_date,
        roe=EXCLUDED.roe, roa=EXCLUDED.roa, npm=EXCLUDED.npm, der=EXCLUDED.der, dar=EXCLUDED.dar,
        per=EXCLUDED.per, pbv=EXCLUDED.pbv, eps=EXCLUDED.eps,
        sales_growth=EXCLUDED.sales_growth, profit_growth=EXCLUDED.profit_growth;
    '''
    with conn.cursor() as cur:
        for r in df.to_dict('records'):
            cur.execute(sql, r)
    conn.commit()

def main():
    folder = 'data/fundamental'
    os.makedirs(folder, exist_ok=True)
    files = glob.glob(os.path.join(folder, '*.csv'))
    if not files:
        print(f'Letakkan CSV fundamental di {folder} lalu jalankan ulang.')
        return
    conn = get_conn()
    for f in files:
        print('Loading', f)
        df = pd.read_csv(f)
        missing = [c for c in REQ if c not in df.columns]
        if missing:
            print('! kolom wajib hilang:', missing); continue
        # normalisasi tipe
        df['period_end'] = pd.to_datetime(df['period_end']).dt.date
        df['announce_date'] = pd.to_datetime(df['announce_date']).dt.date
        num_cols = [c for c in df.columns if c not in ['sid','period_end','announce_date']]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
        upsert(df, conn)
    conn.close()
    print('DONE')

if __name__ == '__main__':
    main()

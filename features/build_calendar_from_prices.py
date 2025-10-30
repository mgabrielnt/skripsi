import pandas as pd
from utils.db import get_conn

def main():
    conn = get_conn()
    q = 'SELECT DISTINCT date AS trading_date FROM prices ORDER BY 1;'
    df = pd.read_sql(q, conn, parse_dates=['trading_date'])
    with conn, conn.cursor() as cur:
        for d in df['trading_date'].dt.date:
            cur.execute('INSERT INTO calendar(trading_date,is_open) VALUES (%s,true) ON CONFLICT (trading_date) DO NOTHING;', (d,))
    print('calendar upserted:', len(df))
    conn.close()

if __name__ == '__main__':
    main()

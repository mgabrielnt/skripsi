import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
from dateutil import parser as dtparser
from zoneinfo import ZoneInfo

from utils.db import get_conn

ID_TZ = ZoneInfo('Asia/Jakarta')

def load_lines(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [l.strip() for l in f if l.strip() and not l.startswith('#')]

def load_map(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def upsert_news(conn, ts, source, title, body, url):
    sql = '''
    INSERT INTO news_raw(ts, source, lang, title, body, url)
    VALUES (%s, %s, 'id', %s, %s, %s)
    ON CONFLICT (url) DO NOTHING
    RETURNING id;
    '''
    with conn.cursor() as cur:
        cur.execute(sql, (ts, source, title[:8000], (body[:20000] if body else None), url))
        res = cur.fetchone()
        conn.commit()
        return res[0] if res else None

def link_news(conn, news_id, sid, conf=1.0):
    sql = '''
    INSERT INTO news_link(sid, news_id, ent_conf)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    '''
    with conn.cursor() as cur:
        cur.execute(sql, (sid, news_id, conf))
    conn.commit()

def parse_ts(entry):
    # published/updated -> datetime; fallback now(UTC)
    for key in ('published', 'updated'):
        if key in entry:
            try:
                dt = dtparser.parse(getattr(entry, key))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(tz=timezone.utc)

def main():
    feeds = load_lines('conf/rss_feeds.txt')
    tmap  = load_map('conf/ticker_map.json')
    conn  = get_conn()
    inserted = 0
    linked = 0

    for feed in feeds:
        print('Feed:', feed)
        parsed = feedparser.parse(feed)
        for e in parsed.entries:
            ts = parse_ts(e)
            src = urlparse(getattr(e, 'link', feed)).netloc
            title = getattr(e, 'title', '(no-title)')
            summary = getattr(e, 'summary', None)
            url = getattr(e, 'link', None)

            news_id = upsert_news(conn, ts, src, title, summary, url)
            if not news_id:
                continue
            inserted += 1

            text = f"{title} {summary or ''}".lower()  # <-- fixed: no backslashes

            # simple keyword mapping to tickers
            for sid, keys in tmap.items():
                if any(k.lower() in text for k in keys):
                    link_news(conn, news_id, sid, conf=1.0)
                    linked += 1
                    break  # link ke satu ticker cukup

    conn.close()
    print(f'Inserted news: {inserted}, Linked: {linked}')

if __name__ == '__main__':
    main()

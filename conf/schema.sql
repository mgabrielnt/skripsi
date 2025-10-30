-- ====== SETUP DASAR ======
SET TIME ZONE 'Asia/Jakarta';

-- Schema namespaces (biar rapi)
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS feature_store;
CREATE SCHEMA IF NOT EXISTS labeling;
CREATE SCHEMA IF NOT EXISTS modeling;
CREATE SCHEMA IF NOT EXISTS towers;

-- ================= core =================

-- Kalender trading (sementara: Mon-Fri = open; libur nasional bisa diupdate nanti)
CREATE TABLE IF NOT EXISTS core.calendar (
  trading_date date PRIMARY KEY,
  is_open boolean NOT NULL DEFAULT TRUE
);

-- Harga harian
CREATE TABLE IF NOT EXISTS core.prices (
  sid text NOT NULL,
  dt date NOT NULL,
  open numeric(18,6),
  high numeric(18,6),
  low  numeric(18,6),
  close numeric(18,6),
  adj_close numeric(18,6),
  volume bigint,
  ret1d numeric(12,6),
  rv20d numeric(12,6),
  CONSTRAINT pk_prices PRIMARY KEY (sid, dt)
);
CREATE INDEX IF NOT EXISTS idx_prices_dt ON core.prices(dt);

-- Fundamental (as-of by announce_date + lag)
CREATE TABLE IF NOT EXISTS core.fundamentals (
  sid text NOT NULL,
  period_end date NOT NULL,
  announce_date date NOT NULL,
  per numeric(12,4),
  pbv numeric(12,4),
  roe numeric(12,4),
  roa numeric(12,4),
  npm numeric(12,4),
  der numeric(12,4),
  dar numeric(12,4),
  eps numeric(14,6),
  revenue_growth numeric(12,4),
  earnings_growth numeric(12,4),
  CONSTRAINT pk_funda PRIMARY KEY (sid, period_end)
);
CREATE INDEX IF NOT EXISTS idx_funda_announce ON core.fundamentals(sid, announce_date);

-- ================= news / sentimen =================

-- Raw news
CREATE TABLE IF NOT EXISTS core.news_raw (
  news_id bigserial PRIMARY KEY,
  ts timestamptz NOT NULL,              -- timestamp publish (UTC/WIB ok; simpan tz)
  source text,
  lang text,
  title text,
  body text,
  url text UNIQUE
);

-- Linking news -> ticker (NER/regex)
CREATE TABLE IF NOT EXISTS core.news_link (
  sid text NOT NULL,
  news_id bigint NOT NULL REFERENCES core.news_raw(news_id) ON DELETE CASCADE,
  ent_conf numeric(6,3),                -- confidence link ke emiten
  CONSTRAINT pk_news_link PRIMARY KEY (sid, news_id)
);
CREATE INDEX IF NOT EXISTS idx_news_link_sid ON core.news_link(sid);

-- Skor IndoBERT per berita (hasil infer tower sentimen)
CREATE TABLE IF NOT EXISTS towers.sentiment_news_score (
  news_id bigint PRIMARY KEY REFERENCES core.news_raw(news_id) ON DELETE CASCADE,
  proba_up numeric(6,5),                -- P(naik)
  logit numeric(10,6),
  entropy numeric(10,6)
);

-- Agregasi harian sentimen per (sid, dt)
CREATE TABLE IF NOT EXISTS feature_store.sentiment_daily (
  sid text NOT NULL,
  dt date NOT NULL,
  proba_up_mean numeric(6,5),
  proba_up_median numeric(6,5),
  proba_up_max numeric(6,5),
  entropy_mean numeric(10,6),
  n_news int,
  last_news_ts timestamptz,
  source_tier_share numeric(6,4),
  CONSTRAINT pk_sent_daily PRIMARY KEY (sid, dt)
);

-- ================= labeling =================

-- Label triple-barrier per (sid, dt) dengan konfigurasi barrier
CREATE TABLE IF NOT EXISTS labeling.triple_barrier_labels (
  sid text NOT NULL,
  dt date NOT NULL,                      -- tanggal sinyal (entry di open t+1)
  horizon int NOT NULL,                  -- N (hari)
  tp_mult numeric(8,4) NOT NULL,         -- kelipatan volatilitas utk TP
  sl_mult numeric(8,4) NOT NULL,         -- kelipatan volatilitas utk SL
  label int,                             -- 1 (kena TP), 0 (kena SL), NULL/2 (time-bar)
  exit_dt date,                          -- kapan keluar (kena barrier/timebar)
  gross_ret numeric(12,6),               -- gross return
  net_ret numeric(12,6),                 -- after-cost (diisi saat backtest)
  CONSTRAINT pk_tbl PRIMARY KEY (sid, dt, horizon, tp_mult, sl_mult)
);
CREATE INDEX IF NOT EXISTS idx_tbl_sid_dt ON labeling.triple_barrier_labels(sid, dt);

-- ================= modeling =================

-- Fitur gabungan (teknikal + fundamental + sentimen) level harian
CREATE TABLE IF NOT EXISTS feature_store.daily_features (
  sid text NOT NULL,
  dt date NOT NULL,
  -- contoh subset kolom teknikal
  r_1d numeric(12,6),
  r_5d numeric(12,6),
  r_20d numeric(12,6),
  vol_20d numeric(12,6),
  rsi_14 numeric(8,4),
  macd numeric(12,6),
  stoch_k numeric(8,4),
  vol_z numeric(12,6),
  -- contoh subset fundamental (as-of)
  per numeric(12,4),
  pbv numeric(12,4),
  roe numeric(12,4),
  -- contoh subset sentimen harian
  s_proba_mean numeric(6,5),
  s_proba_max numeric(6,5),
  s_entropy_mean numeric(10,6),
  s_n_news int,
  -- flags regime
  is_earning_week boolean,
  CONSTRAINT pk_daily_features PRIMARY KEY (sid, dt)
);

-- Set prediksi OOF tiap tower (hindari leakage; untuk stacking)
CREATE TABLE IF NOT EXISTS modeling.oof_preds (
  sid text NOT NULL,
  dt date NOT NULL,
  tower text NOT NULL,                   -- 'tech','fund','sent'
  proba_up numeric(6,5),
  CONSTRAINT pk_oof PRIMARY KEY (sid, dt, tower)
);

-- Dataset meta-label (y_base & y_meta)
CREATE TABLE IF NOT EXISTS modeling.meta_dataset (
  sid text NOT NULL,
  dt date NOT NULL,
  y_base int,                             -- arah dasar (contoh 1/0)
  y_meta int,                             -- 1 jika trade profitable after-cost
  p_base numeric(6,5),                    -- proba base (optional)
  features jsonb,                         -- fitur meta (boleh disimpan eksplisit juga)
  CONSTRAINT pk_meta_ds PRIMARY KEY (sid, dt)
);

-- ==================== AS-OF JOIN VIEW (fundamental) ====================

-- Anggap lag konservatif 10 hari kalender: data fundamental baru dianggap "tersedia"
-- pada available_date = announce_date + 10 hari.
DROP MATERIALIZED VIEW IF EXISTS feature_store.fundamentals_asof CASCADE;
CREATE MATERIALIZED VIEW feature_store.fundamentals_asof AS
WITH f AS (
  SELECT sid, period_end, announce_date,
         (announce_date + INTERVAL '10 days')::date AS available_date,
         per, pbv, roe, roa, npm, der, dar, eps, revenue_growth, earnings_growth
  FROM core.fundamentals
),
d AS (
  SELECT c.trading_date AS dt
  FROM core.calendar c
  WHERE c.is_open = TRUE
)
SELECT
  p.sid,
  d.dt,
  (SELECT per  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS per,
  (SELECT pbv  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS pbv,
  (SELECT roe  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS roe,
  (SELECT roa  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS roa,
  (SELECT npm  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS npm,
  (SELECT der  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS der,
  (SELECT dar  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS dar,
  (SELECT eps  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS eps,
  (SELECT revenue_growth  FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS revenue_growth,
  (SELECT earnings_growth FROM f WHERE f.sid=p.sid AND f.available_date<=d.dt ORDER BY available_date DESC LIMIT 1) AS earnings_growth
FROM core.prices p
JOIN d ON TRUE
GROUP BY p.sid, d.dt;

CREATE UNIQUE INDEX IF NOT EXISTS idx_funda_asof ON feature_store.fundamentals_asof(sid, dt);

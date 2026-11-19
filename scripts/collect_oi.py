#!/usr/bin/env python3
"""바이낸스 USDT 무기한 선물 OI(미결제약정) 기록을 data/oi/<SYMBOL>.json 에 쌓는다.

- 출처: Binance 공식 공개 데이터 저장소 data.binance.vision 의 일별 metrics 파일 (5분 간격 OI)
  · fapi.binance.com 은 미국 IP(GitHub Actions 서버)를 막지만, 이 저장소는 막지 않는다.
  · 파일은 하루 단위로 다음 날 올라온다 → 하루 두 번 실행이면 충분.
- 대시보드가 쓰는 4시간·1일봉 OI만 만든다 (5분·15분·1시간봉은 바이낸스 API 30일 안에 500봉이 다 들어감).
- 처음 실행 때 최근 BACKFILL_DAYS 일을 한꺼번에 채우고, 이후에는 빠진 날만 받는다.
- 표준 라이브러리만 사용 (설치할 것 없음).
"""
import csv, datetime as dt, io, json, os, sys, urllib.error, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor

BASE = os.environ.get('OI_BASE', 'https://data.binance.vision/data/futures/um/daily/metrics')
OUT = os.environ.get('OI_OUT', 'data/oi')
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ZECUSDT', 'HYPEUSDT', 'ENAUSDT', 'PUMPUSDT',
           'NEARUSDT', 'ONDOUSDT', 'TAOUSDT', 'WLDUSDT', 'PENGUUSDT', 'DOGEUSDT']
BACKFILL_DAYS = int(os.environ.get('OI_BACKFILL', '500'))
KEEP = 1000                       # 시간대별 최대 보관 봉 수
PERIOD = {'4h': 4 * 3600_000, '1d': 86400_000}
RETRY_RECENT = 3                  # 최근 3일 안의 파일이 아직 없으면 다음 실행 때 다시 시도
UTC = dt.timezone.utc


def fetch(url):
    """파일 내용(bytes). 없으면 None. 그 밖의 오류는 예외."""
    req = urllib.request.Request(url, headers={'User-Agent': 'vol-dashboard-oi-collector'})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if attempt == 2:
                raise
        except Exception:
            if attempt == 2:
                raise


def to_ms(v):
    v = v.strip()
    if v.isdigit():
        n = int(v)
        return n if n > 1e12 else n * 1000
    return int(dt.datetime.strptime(v[:19], '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC).timestamp() * 1000)


def parse(blob):
    """metrics.zip → [(ts_ms, oi, oi_value_usd)] (열 이름으로 찾음)"""
    z = zipfile.ZipFile(io.BytesIO(blob))
    out = []
    for name in z.namelist():
        if not name.endswith('.csv'):
            continue
        rows = list(csv.reader(io.StringIO(z.read(name).decode('utf-8'))))
        if not rows:
            continue
        head = [h.strip().lower() for h in rows[0]]
        try:
            it, io_, iv = head.index('create_time'), head.index('sum_open_interest'), head.index('sum_open_interest_value')
            body = rows[1:]
        except ValueError:          # 머리줄이 없는 파일: 기본 열 순서
            it, io_, iv, body = 0, 2, 3, rows
        for r in body:
            try:
                out.append((to_ms(r[it]), float(r[io_]), float(r[iv])))
            except (ValueError, IndexError):
                continue
    return out


def buckets(points):
    """5분 점들 → 4시간·1일 구간 시작 시각의 OI (구간 안 가장 이른 점)"""
    res = {tf: {} for tf in PERIOD}
    for ts, oi, val in sorted(points):
        for tf, p in PERIOD.items():
            b = ts - ts % p
            if b not in res[tf]:
                res[tf][b] = [b, round(oi, 3), round(val)]
    return res


def run_symbol(sym, today):
    path = os.path.join(OUT, f'{sym}.json')
    try:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}
    store = {tf: {r[0]: r for r in data.get(tf, [])} for tf in PERIOD}
    start = today - dt.timedelta(days=BACKFILL_DAYS)
    if data.get('last_day'):
        start = max(start, dt.date.fromisoformat(data['last_day']) + dt.timedelta(days=1))
    days = [start + dt.timedelta(days=i) for i in range((today - start).days)]   # 어제까지
    if not days:
        return sym, 0, data.get('last_day')

    def one(d):
        return d, fetch(f'{BASE}/{sym}/{sym}-metrics-{d.isoformat()}.zip')

    with ThreadPoolExecutor(max_workers=12) as ex:
        got = list(ex.map(one, days))
    last, added = data.get('last_day'), 0
    for d, blob in got:                       # 날짜 순서대로: 최근 파일이 아직 없으면 거기서 멈춤
        if blob is None:
            if (today - d).days <= RETRY_RECENT:
                break
            last = d.isoformat()              # 오래된 날의 파일 없음 = 그 무렵 상장 전 → 건너뜀
            continue
        for tf, rows in buckets(parse(blob)).items():
            for b, r in rows.items():
                store[tf][b] = r
                added += 1
        last = d.isoformat()
    out = {'symbol': sym, 'source': 'data.binance.vision futures/um daily metrics',
           'updated': dt.datetime.now(UTC).strftime('%Y-%m-%dT%H:%MZ'), 'last_day': last,
           'fields': ['open_time_ms', 'open_interest', 'open_interest_value_usd']}
    for tf in PERIOD:
        out[tf] = [store[tf][k] for k in sorted(store[tf])][-KEEP:]
    os.makedirs(OUT, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, separators=(',', ':'))
    return sym, added, last


def main():
    today = dt.datetime.now(UTC).date()
    failed = []
    for sym in SYMBOLS:
        try:
            s, n, last = run_symbol(sym, today)
            print(f'{s}: +{n} 구간 · 마지막 날짜 {last}')
        except Exception as e:                # 한 종목 실패가 전체를 막지 않게
            failed.append(sym)
            print(f'{sym}: 실패 — {e}', file=sys.stderr)
    if len(failed) == len(SYMBOLS):
        sys.exit('모든 종목 실패 — 네트워크·주소 확인 필요')


if __name__ == '__main__':
    main()

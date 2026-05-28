from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import os
import json
import time
import gc
import ctypes

# Linux環境でPythonがOSにメモリを返却するためのヘルパー
try:
    _libc = ctypes.cdll.LoadLibrary('libc.so.6')
    def _malloc_trim():
        _libc.malloc_trim(0)
except Exception:
    def _malloc_trim():
        pass
import threading
import requests as http_requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

WATCHLIST_FILE    = 'watchlist.json'
METADATA_FILE     = 'metadata.json'
DEFAULT_WATCHLIST = ['AAPL', 'NVDA', '7203', '1605']

# ── スキャン対象銘柄リスト & 名前辞書 ─────────────────────────────────────────
SCAN_STOCKS = [
    # 日本株
    '7203', '6758', '8306', '9984', '7974', '6861', '8035', '9433', '9432',
    '7751', '6954', '8766', '4661', '7267', '7201', '6752', '6902', '5401',
    '8411', '8316', '4568', '4523', '6367', '6301', '6326', '6501', '6503',
    '7741', '8801', '3382', '6920', '4502', '2914', '2802', '9022', '9020',
    '4519', '8604', '6702', '8002', '8031', '8058', '9843', '6098', '4755',
    '6645', '7832', '9766', '8267', '7550', '1605', '5020', '7309', '4578',
    '6178', '2413', '4704',
    # 米国株
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'JPM', 'V',
    'JNJ', 'WMT', 'PG', 'MA', 'UNH', 'HD', 'CVX', 'LLY', 'ABBV', 'MRK',
    'KO', 'PEP', 'XOM', 'BAC', 'AVGO', 'COST', 'ORCL', 'CSCO', 'AMD',
    'CRM', 'DIS', 'NFLX',
]

STOCK_NAMES = {
    # 日本株
    '7203': 'トヨタ自動車', '6758': 'ソニーグループ', '8306': '三菱UFJ FG',
    '9984': 'ソフトバンクグループ', '7974': '任天堂', '6861': 'キーエンス',
    '8035': '東京エレクトロン', '9433': 'KDDI', '9432': '日本電信電話(NTT)',
    '7751': 'キヤノン', '6954': 'ファナック', '8766': '東京海上HD',
    '4661': 'オリエンタルランド', '7267': 'ホンダ', '7201': '日産自動車',
    '6752': 'パナソニックHD', '6902': 'デンソー', '5401': '日本製鉄',
    '8411': 'みずほFG', '8316': '三井住友FG', '4568': '第一三共',
    '4523': 'エーザイ', '6367': 'ダイキン工業', '6301': 'コマツ',
    '6326': 'クボタ', '6501': '日立製作所', '6503': '三菱電機',
    '7741': 'HOYA', '8801': '三井不動産', '3382': 'セブン&アイHD',
    '6920': 'レーザーテック', '4502': '武田薬品工業', '2914': '日本たばこ産業',
    '2802': '味の素', '9022': 'JR東海', '9020': 'JR東日本',
    '4519': '中外製薬', '8604': '野村HD', '6702': '富士通',
    '8002': '丸紅', '8031': '三井物産', '8058': '三菱商事',
    '9843': 'ニトリHD', '6098': 'リクルートHD', '4755': '楽天グループ',
    '6645': 'オムロン', '7832': 'バンダイナムコHD', '9766': 'コナミグループ',
    '8267': 'イオン', '7550': 'ゼンショーHD', '1605': 'INPEX',
    '5020': 'ENEOSホールディングス', '7309': 'シマノ', '4578': '大塚HD',
    '6178': '日本郵政', '2413': 'エムスリー', '4704': 'トレンドマイクロ',
    # 米国株
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'NVDA': 'NVIDIA',
    'GOOGL': 'Alphabet (Google)', 'AMZN': 'Amazon', 'META': 'Meta',
    'TSLA': 'Tesla', 'JPM': 'JPMorgan Chase', 'V': 'Visa',
    'JNJ': 'Johnson & Johnson', 'WMT': 'Walmart', 'PG': 'Procter & Gamble',
    'MA': 'Mastercard', 'UNH': 'UnitedHealth', 'HD': 'Home Depot',
    'CVX': 'Chevron', 'LLY': 'Eli Lilly', 'ABBV': 'AbbVie',
    'MRK': 'Merck', 'KO': 'Coca-Cola', 'PEP': 'PepsiCo',
    'XOM': 'ExxonMobil', 'BAC': 'Bank of America', 'AVGO': 'Broadcom',
    'COST': 'Costco', 'ORCL': 'Oracle', 'CSCO': 'Cisco',
    'AMD': 'AMD', 'CRM': 'Salesforce', 'DIS': 'Disney', 'NFLX': 'Netflix',
}
SCAN_CACHE_TTL = 12 * 3600  # 12時間

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')


def _sb_headers():
    return {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
    }


def _sb_load(key):
    """Supabaseのsettingsテーブルからvalueを取得する"""
    try:
        res = http_requests.get(
            f'{SUPABASE_URL}/rest/v1/settings?key=eq.{key}&select=value',
            headers=_sb_headers(), timeout=5
        )
        data = res.json()
        if data:
            return data[0]['value']
    except Exception as e:
        print(f'Supabase load error ({key}): {e}')
    return None


def _sb_save(key, value):
    """Supabaseのsettingsテーブルにvalueをupsertする"""
    try:
        headers = _sb_headers()
        headers['Prefer'] = 'resolution=merge-duplicates'
        http_requests.post(
            f'{SUPABASE_URL}/rest/v1/settings',
            json={'key': key, 'value': value},
            headers=headers, timeout=5
        )
        return True
    except Exception as e:
        print(f'Supabase save error ({key}): {e}')
    return False


def load_watchlist():
    if SUPABASE_URL and SUPABASE_KEY:
        data = _sb_load('watchlist')
        if data is not None:
            return data
    # ローカル fallback
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_WATCHLIST.copy()


def save_watchlist(wl):
    if SUPABASE_URL and SUPABASE_KEY:
        if _sb_save('watchlist', wl):
            return
    with open(WATCHLIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(wl, f, ensure_ascii=False)


def load_metadata():
    if SUPABASE_URL and SUPABASE_KEY:
        data = _sb_load('metadata')
        if data is not None:
            return data
    # ローカル fallback
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_metadata(meta):
    if SUPABASE_URL and SUPABASE_KEY:
        if _sb_save('metadata', meta):
            return
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def enrich_with_metadata(result: dict) -> dict:
    entry = load_metadata().get(result['ticker'], {})
    result['custom_name'] = entry.get('custom_name', '')
    result['memo']        = entry.get('memo', '')
    return result


def normalize_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if t.endswith('.T'):
        return t
    if t.isdigit():
        return t + '.T'
    return t


def display_ticker(symbol: str) -> str:
    return symbol[:-2] if symbol.endswith('.T') else symbol


def strip_tz(index):
    try:
        return index.tz_convert(None)
    except TypeError:
        return index


def calculate_macd(prices, fast=12, slow=26, signal=9):
    ema_fast = prices.ewm(span=fast, min_periods=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, min_periods=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, min_periods=signal, adjust=False).mean()
    hist = macd - sig
    return macd, sig, hist


def get_signal(macd, sig):
    valid_m = macd.dropna()
    valid_s = sig.dropna()
    if len(valid_m) < 2 or len(valid_s) < 2:
        return '様子見', 'neutral'

    prev_m, curr_m = float(valid_m.iloc[-2]), float(valid_m.iloc[-1])
    prev_s, curr_s = float(valid_s.iloc[-2]), float(valid_s.iloc[-1])

    if prev_m <= prev_s and curr_m > curr_s:
        return '買い', 'buy'
    if prev_m >= prev_s and curr_m < curr_s:
        return '売り', 'sell'
    if curr_m > curr_s:
        return '上昇トレンド', 'bullish'
    if curr_m < curr_s:
        return '下降トレンド', 'bearish'
    return '様子見', 'neutral'


def scan_stock_data(ticker: str) -> dict:
    """スキャン専用の軽量取得（stock.infoをスキップして高速化）"""
    symbol = normalize_ticker(ticker)
    stock  = yf.Ticker(symbol)

    start_str = (datetime.now() - timedelta(days=365 * 10 + 90)).strftime('%Y-%m-%d')
    raw = stock.history(start=start_str, interval='1wk')
    if raw.empty:
        raise ValueError(f'データが見つかりません: {symbol}')

    hist = raw.resample('MS').agg(
        Close=('Close', 'last'),
    ).dropna(subset=['Close'])

    disp      = display_ticker(symbol)
    is_jp     = symbol.endswith('.T')
    currency  = 'JPY' if is_jp else 'USD'
    curr_price = float(hist['Close'].iloc[-1])
    prev_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else curr_price

    # fast_info だけ使う（info より大幅に速い）
    try:
        fi = stock.fast_info
        currency   = getattr(fi, 'currency', currency) or currency
        lp = getattr(fi, 'last_price', None)
        pc = getattr(fi, 'previous_close', None)
        if lp: curr_price = float(lp)
        if pc: prev_close = float(pc)
    except Exception:
        pass

    change     = curr_price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0.0

    close = hist['Close']
    macd, sig, _ = calculate_macd(close)
    signal_text, signal_type = get_signal(macd, sig)

    last_gc = last_dc = None
    m_vals = macd.dropna()
    s_vals = sig.reindex(m_vals.index).dropna()
    common = m_vals.index.intersection(s_vals.index)
    for i in range(1, len(common)):
        pm, cm = float(m_vals[common[i-1]]), float(m_vals[common[i]])
        ps, cs = float(s_vals[common[i-1]]), float(s_vals[common[i]])
        if pm <= ps and cm > cs:
            last_gc = common[i].strftime('%Y-%m')
        elif pm >= ps and cm < cs:
            last_dc = common[i].strftime('%Y-%m')

    # ミニチャート用: 直近18ヶ月のデータ
    chart_n = min(18, len(hist))
    chart_close = [round(float(v), 4) for v in hist['Close'].iloc[-chart_n:].tolist()]
    macd_hist_full = (macd - sig).fillna(0)
    chart_macd_hist = [round(float(v), 6) for v in macd_hist_full.iloc[-chart_n:].tolist()]

    return {
        'ticker':          disp,
        'symbol':          symbol,
        'name':            STOCK_NAMES.get(disp, disp),
        'currency':        currency,
        'current_price':   curr_price,
        'change':          change,
        'change_pct':      change_pct,
        'signal':          signal_text,
        'signal_type':     signal_type,
        'last_gc':         last_gc,
        'last_dc':         last_dc,
        'custom_name':     '',
        'memo':            '',
        'chart_close':     chart_close,
        'chart_macd_hist': chart_macd_hist,
        'next_earnings':   None,  # _run_scan_thread で順次取得
    }


def fetch_stock_data(ticker: str) -> dict:
    symbol = normalize_ticker(ticker)
    stock = yf.Ticker(symbol)

    # Yahoo Finance caps monthly interval at ~7 years; fetch weekly and resample
    start_str = (datetime.now() - timedelta(days=365 * 10 + 90)).strftime('%Y-%m-%d')
    raw = stock.history(start=start_str, interval='1wk')
    if raw.empty:
        raise ValueError(f'データが見つかりません: {symbol}')

    hist = raw.resample('MS').agg(
        Open=('Open', 'first'),
        High=('High', 'max'),
        Low=('Low', 'min'),
        Close=('Close', 'last'),
        Volume=('Volume', 'sum'),
    ).dropna(subset=['Close'])

    name = symbol
    currency = 'USD'
    current_price = float(hist['Close'].iloc[-1])
    prev_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current_price

    try:
        info = stock.info or {}
        name = info.get('longName') or info.get('shortName') or symbol
        currency = info.get('currency') or currency
        current_price = float(
            info.get('currentPrice') or info.get('regularMarketPrice') or current_price
        )
        prev_close = float(info.get('previousClose') or prev_close)
    except Exception:
        try:
            fi = stock.fast_info
            currency = getattr(fi, 'currency', currency) or currency
            current_price = float(getattr(fi, 'last_price', None) or current_price)
            prev_close = float(getattr(fi, 'previous_close', None) or prev_close)
        except Exception:
            pass

    change = current_price - prev_close
    change_pct = (change / prev_close * 100) if prev_close else 0.0

    close = hist['Close']
    macd, sig, hist_vals = calculate_macd(close)
    signal_text, signal_type = get_signal(macd, sig)

    idx = strip_tz(hist.index)
    all_dates = idx.strftime('%Y-%m').tolist()

    def _last(series):
        d = series.dropna()
        return round(float(d.iloc[-1]), 6) if not d.empty else 0.0

    def padded(series):
        """Align series with all_dates; use None where NaN (MACD warmup)."""
        return [None if pd.isna(v) else round(float(v), 6) for v in series]

    # ゴールデンクロス・デッドクロスの発生日を検出
    last_gc = last_dc = None
    m_vals = macd.dropna()
    s_vals = sig.reindex(m_vals.index).dropna()
    common = m_vals.index.intersection(s_vals.index)
    for i in range(1, len(common)):
        pm, cm = float(m_vals[common[i-1]]), float(m_vals[common[i]])
        ps, cs = float(s_vals[common[i-1]]), float(s_vals[common[i]])
        if pm <= ps and cm > cs:
            last_gc = common[i].strftime('%Y-%m')
        elif pm >= ps and cm < cs:
            last_dc = common[i].strftime('%Y-%m')

    return {
        'ticker': display_ticker(symbol),
        'symbol': symbol,
        'name': name,
        'currency': currency,
        'current_price': current_price,
        'change': change,
        'change_pct': change_pct,
        'signal': signal_text,
        'signal_type': signal_type,
        'macd_value': _last(macd),
        'signal_value': _last(sig),
        'histogram_value': _last(hist_vals),
        'last_gc': last_gc,
        'last_dc': last_dc,
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'chart': {
            'dates':     all_dates,
            'close':     [round(float(x), 2) for x in hist['Close']],
            'macd':      padded(macd),
            'signal':    padded(sig),
            'histogram': padded(hist_vals),
        },
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/watchlist', methods=['GET'])
def get_watchlist():
    return jsonify({'watchlist': load_watchlist()})


@app.route('/api/watchlist/all')
def get_all_stocks():
    """Fetch all watchlist stocks in parallel and return together."""
    wl = load_watchlist()
    if not wl:
        return jsonify({'watchlist': [], 'data': {}, 'errors': {}})

    def safe_fetch(ticker):
        try:
            return ticker, fetch_stock_data(ticker), None
        except Exception as e:
            return ticker, None, str(e)

    data, errors = {}, {}
    with ThreadPoolExecutor(max_workers=min(len(wl), 6)) as ex:
        for ticker, result, err in ex.map(safe_fetch, wl):
            if err:
                errors[ticker] = err
            else:
                data[ticker] = enrich_with_metadata(result)

    return jsonify({'watchlist': wl, 'data': data, 'errors': errors})


@app.route('/api/watchlist', methods=['POST'])
def add_to_watchlist():
    body = request.get_json(silent=True) or {}
    raw = body.get('ticker', '').strip().upper()
    if not raw:
        return jsonify({'error': 'ティッカーを入力してください'}), 400

    disp = display_ticker(normalize_ticker(raw))
    wl = load_watchlist()
    if disp in wl:
        return jsonify({'error': 'すでにリストにあります'}), 400

    try:
        h = yf.Ticker(normalize_ticker(disp)).history(period='5d', interval='1d')
        if h.empty:
            return jsonify({'error': f'銘柄が見つかりません: {raw}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    wl.append(disp)
    save_watchlist(wl)
    meta = load_metadata()
    if disp not in meta:
        meta[disp] = {'custom_name': '', 'memo': ''}
        save_metadata(meta)
    return jsonify({'success': True, 'watchlist': wl})


@app.route('/api/watchlist/order', methods=['PUT'])
def reorder_watchlist():
    body = request.get_json(silent=True) or {}
    new_order = body.get('order', [])
    wl = load_watchlist()
    if not isinstance(new_order, list) or set(new_order) != set(wl):
        return jsonify({'error': '銘柄リストが一致しません'}), 400
    save_watchlist(new_order)
    return jsonify({'success': True, 'watchlist': new_order})


@app.route('/api/watchlist/<ticker>', methods=['DELETE'])
def remove_from_watchlist(ticker):
    t = ticker.upper()
    wl = load_watchlist()
    if t not in wl:
        return jsonify({'error': '見つかりません'}), 404
    wl.remove(t)
    save_watchlist(wl)
    return jsonify({'success': True, 'watchlist': wl})


@app.route('/api/metadata', methods=['GET'])
def get_metadata():
    return jsonify(load_metadata())


@app.route('/api/metadata/<ticker>', methods=['PUT'])
def update_metadata(ticker):
    body  = request.get_json(silent=True) or {}
    disp  = display_ticker(normalize_ticker(ticker))
    meta  = load_metadata()
    entry = meta.get(disp, {})
    if 'custom_name' in body:
        entry['custom_name'] = str(body['custom_name']).strip()
    if 'memo' in body:
        entry['memo'] = str(body['memo'])
    meta[disp] = entry
    save_metadata(meta)
    return jsonify({'success': True, 'ticker': disp, **entry})


# ── スキャン バックグラウンドスレッド管理 ─────────────────────────────────────
_scan_lock  = threading.Lock()
_scan_state = {'status': 'idle', 'results': None, 'updated_at': 0, 'phase': ''}

def load_auto_scan_list():
    """Supabaseから自動取得済み値上がり銘柄リストを返す (stocks, updated_at)"""
    if SUPABASE_URL and SUPABASE_KEY:
        data = _sb_load('auto_scan_list')
        if isinstance(data, dict):
            return data.get('stocks', []), data.get('updated_at', 0)
    return [], 0


def save_auto_scan_list(stocks):
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('auto_scan_list', {'stocks': stocks, 'updated_at': time.time()})


def fetch_next_earnings(symbol: str) -> str | None:
    """yfinance calendar から次回決算日を取得（認証を内部処理・軽量）"""
    try:
        stock = yf.Ticker(symbol)
        cal = stock.calendar
        if not cal:
            return None
        dates = cal.get('Earnings Date', [])
        if not isinstance(dates, list):
            dates = [dates] if dates else []
        today = datetime.now().date()
        for d in sorted(dates):
            try:
                # datetime.date オブジェクトの場合
                if hasattr(d, 'strftime'):
                    if d > today:
                        return d.strftime('%Y-%m-%d')
                else:
                    ts = pd.Timestamp(d)
                    if ts.tzinfo:
                        ts = ts.tz_convert(None)
                    if ts.date() > today:
                        return ts.strftime('%Y-%m-%d')
            except Exception:
                pass
        return None
    except Exception:
        return None


def fetch_top_gainers(limit=100):
    """Yahoo Finance スクリーナーで値上がり上位銘柄を取得（JP + US）"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'application/json',
    }
    results = []
    seen = set()
    for region in ['JP', 'US']:
        try:
            r = http_requests.get(
                'https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved',
                params={
                    'scrIds': 'day_gainers',
                    'count': 60,
                    'region': region,
                    'formatted': 'false',
                    'corsDomain': 'finance.yahoo.com',
                },
                headers=headers,
                timeout=15,
            )
            quotes = r.json()['finance']['result'][0]['quotes']
            for q in quotes:
                sym = q.get('symbol', '')
                if not sym:
                    continue
                disp = display_ticker(sym)
                if disp not in seen:
                    seen.add(disp)
                    results.append(disp)
        except Exception as e:
            print(f'fetch_top_gainers [{region}] error: {e}')
    return results[:limit]


def load_scan_list():
    """デフォルト + 自動取得 + 手動追加 を重複排除してマージ"""
    custom = []
    auto_stocks = []
    if SUPABASE_URL and SUPABASE_KEY:
        d = _sb_load('scan_list')
        if isinstance(d, list):
            custom = d
        auto_data, _ = load_auto_scan_list()
        if isinstance(auto_data, list):
            auto_stocks = auto_data

    default_disps = set(display_ticker(normalize_ticker(t)) for t in SCAN_STOCKS)
    seen = set(default_disps)

    extra_auto = []
    for t in auto_stocks:
        if t not in seen:
            seen.add(t)
            extra_auto.append(t)

    extra_custom = []
    for t in custom:
        if t not in seen:
            seen.add(t)
            extra_custom.append(t)

    return SCAN_STOCKS + extra_auto + extra_custom, custom


def save_scan_list(custom):
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_list', custom)


def _invalidate_scan_cache():
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_cache', {'results': [], 'updated_at': 0})
    with _scan_lock:
        _scan_state['status'] = 'idle'


def _run_scan_thread():
    with _scan_lock:
        _scan_state['phase'] = 'prices'
    all_stocks, _ = load_scan_list()
    results = []
    def safe(t):
        try: return scan_stock_data(t)
        except: return None
    with ThreadPoolExecutor(max_workers=12) as ex:
        for data in ex.map(safe, all_stocks):
            if data:
                results.append(data)

    # ── フェーズ1完了: 価格/MACDデータを先に保存 ──────────────────────────────
    # 決算フェーズでクラッシュしても価格データは確実に見えるようにする
    prices_at = time.time()
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_cache', {'results': results, 'updated_at': prices_at})
    with _scan_lock:
        _scan_state.update({'status': 'done', 'phase': 'prices_done',
                            'results': list(results), 'updated_at': prices_at})

    # ── フェーズ2: 決算日を順次取得 ───────────────────────────────────────────
    # 並列スキャン直後はレート制限リセット待ち（90秒クールダウン）
    with _scan_lock:
        _scan_state['phase'] = 'cooldown'
    time.sleep(90)

    with _scan_lock:
        _scan_state['phase'] = 'earnings'

    # 買い・上昇トレンドを優先して最大60銘柄に絞る（OOM対策）
    EARN_RANK = {'買い': 0, '上昇トレンド': 1, '様子見': 2, '下降トレンド': 3, '売り': 4}
    earnings_targets = sorted(results, key=lambda r: EARN_RANK.get(r.get('signal', ''), 5))[:60]
    print(f'[scan] 決算日取得対象: {len(earnings_targets)}/{len(results)}銘柄')

    earnings_ok = 0
    for r in earnings_targets:
        try:
            e = fetch_next_earnings(normalize_ticker(r['ticker']))
            r['next_earnings'] = e
            if e:
                earnings_ok += 1
        except Exception:
            pass
        gc.collect()
        _malloc_trim()   # Pythonのfreeメモリを即座にOSへ返却
        time.sleep(2)

    print(f'[scan] 決算日取得: {earnings_ok}/{len(earnings_targets)}件')
    updated_at = time.time()
    if SUPABASE_URL and SUPABASE_KEY:
        _sb_save('scan_cache', {'results': results, 'updated_at': updated_at})
    with _scan_lock:
        _scan_state.update({'status': 'done', 'phase': 'done',
                            'results': results, 'updated_at': updated_at})


@app.route('/api/scan')
def scan_signals():
    """スキャン状態を返す（バックグラウンドで実行してポーリング方式）"""
    force = request.args.get('force', 'false') == 'true'

    # Supabaseキャッシュ確認
    if not force and SUPABASE_URL and SUPABASE_KEY:
        cached = _sb_load('scan_cache')
        if cached and time.time() - cached.get('updated_at', 0) < SCAN_CACHE_TTL:
            with _scan_lock:
                _scan_state.update({'status': 'done',
                                    'results': cached['results'],
                                    'updated_at': cached['updated_at']})
            return jsonify({'status': 'done',
                            'results': cached['results'],
                            'updated_at': cached['updated_at']})

    # メモリ上の状態確認
    with _scan_lock:
        state = dict(_scan_state)

    if state['status'] == 'done' and not force:
        return jsonify({'status': 'done',
                        'results': state['results'],
                        'updated_at': state['updated_at']})

    # バックグラウンドスレッド起動（まだ動いていない場合）
    if state['status'] != 'running':
        with _scan_lock:
            _scan_state['status'] = 'running'
        # force scan時はSupabaseキャッシュを即座に無効化（古い結果が表示されないよう）
        if force and SUPABASE_URL and SUPABASE_KEY:
            _sb_save('scan_cache', {'results': [], 'updated_at': 0})
        t = threading.Thread(target=_run_scan_thread, daemon=True)
        t.start()

    return jsonify({'status': 'running'})


@app.route('/api/scan-list', methods=['GET'])
def get_scan_list():
    all_stocks, custom = load_scan_list()
    auto_stocks, auto_updated_at = load_auto_scan_list()
    return jsonify({
        'default_count':   len(SCAN_STOCKS),
        'custom':          custom,
        'auto':            auto_stocks,
        'auto_updated_at': auto_updated_at,
        'total':           len(all_stocks),
    })


@app.route('/api/auto-scan-list/refresh', methods=['POST'])
def refresh_auto_scan_list():
    """値上がり上位を取得してスキャンリストを自動更新"""
    # 除外セット：ウォッチリスト + デフォルト + 手動追加
    wl = load_watchlist()
    wl_set = set(wl)
    default_set = set(display_ticker(normalize_ticker(t)) for t in SCAN_STOCKS)
    _, custom = load_scan_list()
    custom_set = set(custom)

    gainers = fetch_top_gainers(limit=150)

    # フィルタリング（ウォッチリスト・デフォルト・手動追加は除外）
    filtered = [
        t for t in gainers
        if t not in wl_set and t not in default_set and t not in custom_set
    ][:100]

    save_auto_scan_list(filtered)
    _invalidate_scan_cache()

    auto_stocks, auto_updated_at = load_auto_scan_list()
    all_stocks, _ = load_scan_list()
    return jsonify({
        'success':         True,
        'auto':            filtered,
        'auto_updated_at': auto_updated_at,
        'count':           len(filtered),
        'total':           len(all_stocks),
    })


@app.route('/api/scan-list', methods=['POST'])
def add_to_scan_list():
    body = request.get_json(silent=True) or {}
    raw  = body.get('ticker', '').strip().upper()
    if not raw:
        return jsonify({'error': 'ティッカーを入力してください'}), 400

    disp = display_ticker(normalize_ticker(raw))
    default_disps = [display_ticker(normalize_ticker(t)) for t in SCAN_STOCKS]
    _, custom = load_scan_list()

    if disp in default_disps or disp in custom:
        return jsonify({'error': 'すでにスキャンリストに含まれています'}), 400

    try:
        h = yf.Ticker(normalize_ticker(disp)).history(period='5d', interval='1d')
        if h.empty:
            return jsonify({'error': f'銘柄が見つかりません: {raw}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    custom.append(disp)
    save_scan_list(custom)
    _invalidate_scan_cache()

    return jsonify({'success': True, 'custom': custom, 'total': len(SCAN_STOCKS) + len(custom)})


@app.route('/api/scan-list/<ticker>', methods=['DELETE'])
def remove_from_scan_list(ticker):
    disp = display_ticker(normalize_ticker(ticker.upper()))
    _, custom = load_scan_list()
    if disp not in custom:
        return jsonify({'error': 'ユーザー追加銘柄にしか削除できません'}), 400
    custom.remove(disp)
    save_scan_list(custom)
    _invalidate_scan_cache()
    return jsonify({'success': True, 'custom': custom, 'total': len(SCAN_STOCKS) + len(custom)})


@app.route('/api/search')
def search_ticker():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'results': []})
    try:
        res = http_requests.get(
            'https://query1.finance.yahoo.com/v1/finance/search',
            params={
                'q': q,
                'quotesCount': 8,
                'newsCount': 0,
                'enableFuzzyQuery': 'false',
                'quotesQueryId': 'tss_match_phrase_query',
            },
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
            timeout=5
        )
        quotes = res.json().get('quotes', [])
        results = [
            {
                'symbol':   item.get('symbol', ''),
                'name':     item.get('longname') or item.get('shortname') or item.get('symbol', ''),
                'exchange': item.get('exchange', ''),
            }
            for item in quotes
            if item.get('quoteType') in ('EQUITY', 'ETF')
        ]
        return jsonify({'results': results})
    except Exception as e:
        return jsonify({'results': [], 'error': str(e)})


@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        return jsonify(enrich_with_metadata(fetch_stock_data(ticker)))
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': f'データ取得エラー: {e}'}), 500



@app.route('/api/debug/earnings/<ticker>')
def debug_one_earnings(ticker):
    """一時デバッグ: 1銘柄の決算日取得テスト（生レスポンス付き）"""
    symbol = normalize_ticker(ticker)
    result = {'symbol': symbol}
    try:
        # JSON API の生レスポンスを確認
        r = http_requests.get(
            f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}',
            params={'modules': 'calendarEvents'},
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=10,
        )
        result['status_code'] = r.status_code
        data = r.json()
        result['raw_top'] = str(data)[:600]

        # yfinance calendar も並行確認
        stock = yf.Ticker(symbol)
        try:
            cal = stock.calendar
            result['calendar'] = str(cal)[:300] if cal is not None else 'None'
        except Exception as e2:
            result['calendar_error'] = str(e2)

        result['next_earnings'] = fetch_next_earnings(symbol)
    except Exception as e:
        result['error'] = str(e)
    return jsonify(result)


@app.route('/api/debug/scan-status')
def debug_scan_status():
    """一時デバッグ: スキャン進行フェーズとキャッシュ内の決算日を確認"""
    with _scan_lock:
        state = dict(_scan_state)

    earnings_sample = []
    earnings_count = 0
    if state.get('results'):
        for r in state['results'][:5]:
            earnings_sample.append({'ticker': r['ticker'], 'next_earnings': r.get('next_earnings')})
        earnings_count = sum(1 for r in state['results'] if r.get('next_earnings'))

    # Supabaseキャッシュも確認
    sb_earnings_count = 0
    sb_updated_at = None
    if SUPABASE_URL and SUPABASE_KEY:
        cached = _sb_load('scan_cache')
        if cached and cached.get('results'):
            sb_earnings_count = sum(1 for r in cached['results'] if r.get('next_earnings'))
            sb_updated_at = cached.get('updated_at')

    return jsonify({
        'scan_status':      state.get('status'),
        'scan_phase':       state.get('phase'),
        'memory_total':     len(state['results']) if state.get('results') else 0,
        'memory_earnings':  earnings_count,
        'memory_sample':    earnings_sample,
        'supabase_earnings': sb_earnings_count,
        'supabase_updated_at': sb_updated_at,
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port, threaded=True)

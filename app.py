from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

WATCHLIST_FILE = 'watchlist.json'
METADATA_FILE  = 'metadata.json'
DEFAULT_WATCHLIST = ['AAPL', 'NVDA', '7203', '1605']


def load_watchlist():
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_WATCHLIST.copy()


def save_watchlist(wl):
    with open(WATCHLIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(wl, f, ensure_ascii=False)


def load_metadata():
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_metadata(meta):
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def enrich_with_metadata(result: dict) -> dict:
    entry = load_metadata().get(result['ticker'], {})
    result['custom_name'] = entry.get('custom_name', '')
    result['memo']        = entry.get('memo', '')
    return result


watchlist = load_watchlist()


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
    return jsonify({'watchlist': watchlist})


@app.route('/api/watchlist/all')
def get_all_stocks():
    """Fetch all watchlist stocks in parallel and return together."""
    if not watchlist:
        return jsonify({'watchlist': [], 'data': {}, 'errors': {}})

    def safe_fetch(ticker):
        try:
            return ticker, fetch_stock_data(ticker), None
        except Exception as e:
            return ticker, None, str(e)

    data, errors = {}, {}
    with ThreadPoolExecutor(max_workers=min(len(watchlist), 6)) as ex:
        for ticker, result, err in ex.map(safe_fetch, watchlist):
            if err:
                errors[ticker] = err
            else:
                data[ticker] = enrich_with_metadata(result)

    return jsonify({'watchlist': watchlist, 'data': data, 'errors': errors})


@app.route('/api/watchlist', methods=['POST'])
def add_to_watchlist():
    body = request.get_json(silent=True) or {}
    raw = body.get('ticker', '').strip().upper()
    if not raw:
        return jsonify({'error': 'ティッカーを入力してください'}), 400

    disp = display_ticker(normalize_ticker(raw))
    if disp in watchlist:
        return jsonify({'error': 'すでにリストにあります'}), 400

    try:
        h = yf.Ticker(normalize_ticker(disp)).history(period='5d', interval='1d')
        if h.empty:
            return jsonify({'error': f'銘柄が見つかりません: {raw}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 400

    watchlist.append(disp)
    save_watchlist(watchlist)
    meta = load_metadata()
    if disp not in meta:
        meta[disp] = {'custom_name': '', 'memo': ''}
        save_metadata(meta)
    return jsonify({'success': True, 'watchlist': watchlist})


@app.route('/api/watchlist/order', methods=['PUT'])
def reorder_watchlist():
    body = request.get_json(silent=True) or {}
    new_order = body.get('order', [])
    if not isinstance(new_order, list) or set(new_order) != set(watchlist):
        return jsonify({'error': '銘柄リストが一致しません'}), 400
    watchlist.clear()
    watchlist.extend(new_order)
    save_watchlist(watchlist)
    return jsonify({'success': True, 'watchlist': watchlist})


@app.route('/api/watchlist/<ticker>', methods=['DELETE'])
def remove_from_watchlist(ticker):
    t = ticker.upper()
    if t not in watchlist:
        return jsonify({'error': '見つかりません'}), 404
    watchlist.remove(t)
    save_watchlist(watchlist)
    return jsonify({'success': True, 'watchlist': watchlist})


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


@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    try:
        return jsonify(enrich_with_metadata(fetch_stock_data(ticker)))
    except ValueError as e:
        return jsonify({'error': str(e)}), 404
    except Exception as e:
        return jsonify({'error': f'データ取得エラー: {e}'}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)

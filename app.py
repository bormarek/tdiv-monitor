from flask import Flask, render_template, jsonify
import openpyxl
import yfinance as yf
import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import os
import time

app = Flask(__name__)

VANECK_URL = 'https://www.vaneck.com/pl/pl/investments/dividend-etf/downloads/holdings/'
VANECK_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'pl-PL,pl;q=0.9',
}
CACHE_TTL = 900  # 15 minut

_cache = {}

# Ręczne nadpisania dla tickerów których yfinance nie rozpoznaje po nazwie
TICKER_OVERRIDES = {
    'DBS SP':    'D05.SI',   # DBS Group - SGX symbol
    'OCBC SP':   'O39.SI',   # OCBC Bank - SGX symbol
    'UOB SP':    'U11.SI',   # United Overseas Bank - SGX symbol
    'KEP SP':    'BN4.SI',   # Keppel Corp - SGX symbol
    'WIL SP':    'F34.SI',   # Wilmar International - SGX symbol
    'NOVOB DC':  'NOVO-B.CO', # Novo Nordisk B shares - Copenhagen
    'EDP PL':    'EDP.LS',   # EDP - Lisbon
    'LUMI IT':   'LUMI.TA',  # Bank Leumi - Tel Aviv
    'MZTF IT':   'MZTF.TA',  # Mizrahi Tefahot - Tel Aviv
}

# Mapowanie kodów giełd Bloomberg -> sufiksy yfinance
EXCHANGE_MAP = {
    'US': '',
    'SW': '.SW',
    'LN': '.L',
    'FP': '.PA',
    'GR': '.DE',
    'SM': '.MC',
    'DC': '.CO',
    'NA': '.AS',
    'IM': '.MI',
    'AU': '.AX',
    'HK': '.HK',
    'JP': '.T',
    'BB': '.BR',
    'SS': '.ST',
    'NO': '.OL',
    'SP': '.SI',
    'CN': '.TO',
    'PW': '.WA',
    'AV': '.VI',
    'PL': '.LS',
    'IT': '.TA',  # Israel Tel Aviv
    'SJ': '.JO',
}

def ticker_to_yfinance(ticker_str):
    """Konwertuje ticker Bloomberg (np. 'XOM US') do formatu yfinance (np. 'XOM')."""
    key = ticker_str.strip()
    if key in TICKER_OVERRIDES:
        return TICKER_OVERRIDES[key]

    parts = key.split()
    if len(parts) < 2:
        return None

    symbol = parts[0]
    exchange = parts[-1]

    if exchange == '--' or symbol == '--':
        return None

    # Zamień "/" w środku symbolu na "-" (np. BT/A -> BT-A), usuń na końcu (BP/ -> BP)
    if '/' in symbol:
        if symbol.endswith('/'):
            symbol = symbol.rstrip('/')
        else:
            symbol = symbol.replace('/', '-')

    suffix = EXCHANGE_MAP.get(exchange, '')

    # Hong Kong: dopełnij do 4 cyfr zerami (np. 66 -> 0066)
    if exchange == 'HK' and symbol.isdigit():
        symbol = symbol.zfill(4)

    return symbol + suffix


def fetch_excel_bytes():
    """Pobiera aktualny plik Excel z VanEck. Zwraca bytes."""
    r = requests.get(VANECK_URL, headers=VANECK_HEADERS, timeout=30)
    r.raise_for_status()
    return r.content


def load_holdings(excel_bytes=None):
    """Wczytuje holdings z bytes (live) lub lokalnego pliku (fallback)."""
    try:
        if excel_bytes is None:
            excel_bytes = fetch_excel_bytes()
        wb = openpyxl.load_workbook(io.BytesIO(excel_bytes))
    except Exception:
        # Fallback: lokalny plik jeśli URL niedostępny
        local = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'TDIV_nadzień_20260305.xlsx')
        wb = openpyxl.load_workbook(local)

    ws = wb.active
    holdings = []
    for row in ws.iter_rows(min_row=4, values_only=True):
        if row[0] is None or not isinstance(row[0], int):
            continue
        name = row[1]
        ticker_raw = row[2]
        weight_str = row[6]
        if not name or not ticker_raw:
            continue
        yf_ticker = ticker_to_yfinance(str(ticker_raw))
        if not yf_ticker:
            continue
        holdings.append({
            'number': row[0],
            'name': name,
            'ticker_raw': str(ticker_raw).strip(),
            'ticker': yf_ticker,
            'weight': weight_str,
        })
    return holdings


def analyze_series(series):
    """Zwraca słownik z analizą cenową dla danej serii danych."""
    result = {
        'price': None,
        'daily_change': None,
        'daily_change_pct': None,
        'daily_trend': 'unknown',
        'ma5': None,
        'ma5_trend': 'unknown',
    }
    if series is None or len(series) < 2:
        return result

    last = float(series.iloc[-1])
    prev = float(series.iloc[-2])

    result['price'] = round(last, 2)
    result['daily_change'] = round(last - prev, 4)
    result['daily_change_pct'] = round((last - prev) / prev * 100, 2)
    result['daily_trend'] = 'up' if last >= prev else 'down'

    if len(series) >= 6:
        ma5_current = float(series.iloc[-5:].mean())
        ma5_prev = float(series.iloc[-6:-1].mean())
        result['ma5'] = round(ma5_current, 2)
        result['ma5_trend'] = 'up' if ma5_current > ma5_prev else 'down'
    elif len(series) >= 5:
        result['ma5'] = round(float(series.iloc[-5:].mean()), 2)

    return result


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/data')
def get_data():
    global _cache
    now = time.time()

    if 'result' in _cache and now - _cache.get('timestamp', 0) < CACHE_TTL:
        return jsonify(_cache['result'])

    # Pobierz aktualny Excel z VanEck
    try:
        excel_bytes = fetch_excel_bytes()
        holdings = load_holdings(excel_bytes)
        holdings_source = 'live'
    except Exception as e:
        holdings = load_holdings(None)  # fallback na lokalny plik
        holdings_source = f'local (błąd VanEck: {e})'

    tickers = [h['ticker'] for h in holdings]

    end_date = datetime.now()
    start_date = end_date - timedelta(days=25)

    try:
        raw = yf.download(
            tickers,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    # Normalizacja: zawsze MultiIndex z Close
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw['Close']
    else:
        # Pojedynczy ticker
        close = raw[['Close']]
        close.columns = tickers

    results = []
    for h in holdings:
        ticker = h['ticker']
        entry = {
            'number': h['number'],
            'name': h['name'],
            'ticker': h['ticker_raw'],
            'yf_ticker': ticker,
            'weight': h['weight'],
        }
        try:
            if ticker in close.columns:
                series = close[ticker].dropna()
                entry.update(analyze_series(series))
            else:
                entry.update(analyze_series(None))
        except Exception as e:
            entry.update(analyze_series(None))
            entry['error'] = str(e)

        results.append(entry)

    up_count = sum(1 for r in results if r.get('daily_trend') == 'up')
    down_count = sum(1 for r in results if r.get('daily_trend') == 'down')

    response = {
        'data': results,
        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'holdings_source': holdings_source,
        'summary': {
            'up': up_count,
            'down': down_count,
            'unknown': len(results) - up_count - down_count,
        }
    }

    _cache['result'] = response
    _cache['timestamp'] = now

    return jsonify(response)


@app.route('/api/chart/<path:ticker>')
def get_chart(ticker):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)  # 6 mies. żeby MA20 miała dane od początku

    try:
        raw = yf.download(
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if raw.empty:
        return jsonify({'error': 'Brak danych dla tego tickera'}), 404

    # Obsługa MultiIndex (yfinance zwraca MultiIndex nawet dla 1 tickera)
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw['Close'].iloc[:, 0].dropna()
    else:
        close = raw['Close'].dropna()

    ma5  = close.rolling(5).mean()
    ma20 = close.rolling(20).mean()

    def fmt(v):
        return round(float(v), 2) if not pd.isna(v) else None

    return jsonify({
        'ticker': ticker,
        'dates':  [d.strftime('%Y-%m-%d') for d in close.index],
        'prices': [fmt(p) for p in close.values],
        'ma5':    [fmt(v) for v in ma5],
        'ma20':   [fmt(v) for v in ma20],
    })


@app.route('/api/refresh')
def refresh_cache():
    """Wymuś odświeżenie cache."""
    _cache.clear()
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)

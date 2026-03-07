from flask import Flask, render_template, jsonify, request
import openpyxl
import yfinance as yf
import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import os
import time
from deep_translator import GoogleTranslator

app = Flask(__name__)

# ── Konfiguracja funduszy ─────────────────────────────────────────────────────

FUNDS = {
    'tdiv': {
        'id':       'tdiv',
        'name':     'VanEck Morningstar Dividend Leaders ETF',
        'short':    'TDIV',
        'currency': 'USD',
        'url':      'https://www.vaneck.com/pl/pl/investments/dividend-etf/downloads/holdings/',
        'headers':  {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pl-PL,pl;q=0.9',
        },
        'local_fallback': 'TDIV_nadzień_20260305.xlsx',
    },
    'swig80': {
        'id':       'swig80',
        'name':     'Beta ETF sWIG80TR',
        'short':    'sWIG80',
        'currency': 'PLN',
        'url':      'https://wp00102-api.agiofunds.pl/uploads/funds/Portfolio/archive/PRTF_Beta%20ETF%20sWIG80TR.xlsx',
        'headers':  {'User-Agent': 'Mozilla/5.0'},
        'local_fallback': None,
    },
    'mwig40': {
        'id':       'mwig40',
        'name':     'Beta ETF mWIG40TR',
        'short':    'mWIG40',
        'currency': 'PLN',
        'url':      'https://wp00102-api.agiofunds.pl/uploads/funds/Portfolio/archive/PRTF_Beta%20ETF%20mWIG40TR.xlsx',
        'headers':  {'User-Agent': 'Mozilla/5.0'},
        'local_fallback': None,
    },
    'wig20': {
        'id':       'wig20',
        'name':     'Beta ETF WIG20TR',
        'short':    'WIG20',
        'currency': 'PLN',
        'url':      'https://wp00102-api.agiofunds.pl/uploads/funds/Portfolio/archive/PRTF_Beta%20ETF%20WIG20TR.xlsx',
        'headers':  {'User-Agent': 'Mozilla/5.0'},
        'local_fallback': None,
    },
}

CACHE_TTL      = 900    # 15 minut
INFO_CACHE_TTL = 86400  # 24 godziny

_cache      = {}   # key: fund_id
_info_cache = {}   # key: ticker

# ── Ticker helpers (TDIV / Bloomberg) ────────────────────────────────────────

TICKER_OVERRIDES = {
    'DBS SP':   'D05.SI',
    'OCBC SP':  'O39.SI',
    'UOB SP':   'U11.SI',
    'KEP SP':   'BN4.SI',
    'WIL SP':   'F34.SI',
    'NOVOB DC': 'NOVO-B.CO',
    'EDP PL':   'EDP.LS',
    'LUMI IT':  'LUMI.TA',
    'MZTF IT':  'MZTF.TA',
}

EXCHANGE_MAP = {
    'US': '',   'SW': '.SW',  'LN': '.L',   'FP': '.PA',
    'GR': '.DE','SM': '.MC',  'DC': '.CO',  'NA': '.AS',
    'IM': '.MI','AU': '.AX',  'HK': '.HK',  'JP': '.T',
    'BB': '.BR','SS': '.ST',  'NO': '.OL',  'SP': '.SI',
    'CN': '.TO','PW': '.WA',  'AV': '.VI',  'PL': '.LS',
    'IT': '.TA','SJ': '.JO',
}


def bloomberg_to_yfinance(ticker_str):
    key = ticker_str.strip()
    if key in TICKER_OVERRIDES:
        return TICKER_OVERRIDES[key]
    parts = key.split()
    if len(parts) < 2:
        return None
    symbol, exchange = parts[0], parts[-1]
    if exchange == '--' or symbol == '--':
        return None
    if '/' in symbol:
        symbol = symbol.rstrip('/') if symbol.endswith('/') else symbol.replace('/', '-')
    if exchange == 'HK' and symbol.isdigit():
        symbol = symbol.zfill(4)
    return symbol + EXCHANGE_MAP.get(exchange, '')


# ── Parsery Excel ─────────────────────────────────────────────────────────────

def parse_tdiv(wb):
    ws = wb.active
    holdings = []
    for row in ws.iter_rows(min_row=4, values_only=True):
        if row[0] is None or not isinstance(row[0], int):
            continue
        name, ticker_raw, weight_str = row[1], row[2], row[6]
        if not name or not ticker_raw:
            continue
        yf_ticker = bloomberg_to_yfinance(str(ticker_raw))
        if not yf_ticker:
            continue
        holdings.append({
            'number':     row[0],
            'name':       name,
            'ticker_raw': str(ticker_raw).strip(),
            'ticker':     yf_ticker,
            'weight':     weight_str,
            'sector':     '',
        })
    return holdings


def parse_swig80(wb):
    ws = wb.active
    holdings = []
    header_found = False
    for row in ws.iter_rows(values_only=True):
        if not header_found:
            if row[0] == 'Lp.':
                header_found = True
            continue
        if row[0] is None or not isinstance(row[0], int):
            continue
        name    = row[1]
        isin    = row[2]
        sector  = row[3] or ''
        weight  = row[6]  # ułamek dziesiętny, np. 0.044
        if not name or not isin:
            continue
        weight_str = f"{weight * 100:.2f}%".replace('.', ',') if weight else None
        yf_ticker = POLISH_ISIN_OVERRIDES.get(isin, isin)
        holdings.append({
            'number':     row[0],
            'name':       name,
            'ticker_raw': isin,
            'ticker':     yf_ticker,
            'weight':     weight_str,
            'sector':     sector,
        })
    return holdings


# ISINy które yfinance rozwiązuje na giełdę zagraniczną zamiast GPW
POLISH_ISIN_OVERRIDES = {
    'PLGPW0000017': 'GPW.WA',    # Giełda Papierów Wartościowych
    'PLBUDMX00013': 'BDX.WA',    # Budimex
    'LU2237380790': 'ALE.WA',    # Allegro.eu
    'AU0000198939': 'GRX.WA',    # GreenX Metals
}

PARSERS = {'tdiv': parse_tdiv, 'swig80': parse_swig80, 'mwig40': parse_swig80, 'wig20': parse_swig80}


def load_holdings(fund_id):
    fund = FUNDS[fund_id]
    source = 'live'
    try:
        r = requests.get(fund['url'], headers=fund['headers'], timeout=30)
        r.raise_for_status()
        wb = openpyxl.load_workbook(io.BytesIO(r.content))
    except Exception as e:
        source = f'local (błąd pobierania: {e})'
        if fund['local_fallback']:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), fund['local_fallback'])
            wb = openpyxl.load_workbook(path)
        else:
            return [], source
    return PARSERS[fund_id](wb), source


# ── Analiza cenowa ────────────────────────────────────────────────────────────

def analyze_series(series):
    result = {
        'price': None, 'daily_change': None, 'daily_change_pct': None,
        'daily_trend': 'unknown', 'ma5': None, 'ma5_trend': 'unknown',
    }
    if series is None or len(series) < 2:
        return result
    last, prev = float(series.iloc[-1]), float(series.iloc[-2])
    result['price']            = round(last, 2)
    result['daily_change']     = round(last - prev, 4)
    result['daily_change_pct'] = round((last - prev) / prev * 100, 2)
    result['daily_trend']      = 'up' if last >= prev else 'down'
    if len(series) >= 6:
        ma5_cur  = float(series.iloc[-5:].mean())
        ma5_prev = float(series.iloc[-6:-1].mean())
        result['ma5']       = round(ma5_cur, 2)
        result['ma5_trend'] = 'up' if ma5_cur > ma5_prev else 'down'
    elif len(series) >= 5:
        result['ma5'] = round(float(series.iloc[-5:].mean()), 2)
    return result


# ── Endpointy ─────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html', funds=list(FUNDS.values()))


@app.route('/api/funds')
def get_funds():
    return jsonify(list(FUNDS.values()))


@app.route('/api/data')
def get_data():
    fund_id = request.args.get('fund', 'tdiv')
    if fund_id not in FUNDS:
        return jsonify({'error': f'Nieznany fundusz: {fund_id}'}), 400

    global _cache
    now = time.time()
    if fund_id in _cache and now - _cache[fund_id].get('timestamp', 0) < CACHE_TTL:
        return jsonify(_cache[fund_id]['result'])

    holdings, holdings_source = load_holdings(fund_id)
    tickers = [h['ticker'] for h in holdings]

    end_date   = datetime.now()
    start_date = end_date - timedelta(days=25)
    try:
        raw = yf.download(
            tickers,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            auto_adjust=True, progress=False, threads=True,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if isinstance(raw.columns, pd.MultiIndex):
        close = raw['Close']
    else:
        close = raw[['Close']]
        close.columns = tickers

    results = []
    for h in holdings:
        entry = {
            'number':     h['number'],
            'name':       h['name'],
            'ticker':     h['ticker_raw'],
            'yf_ticker':  h['ticker'],
            'weight':     h['weight'],
            'sector':     h.get('sector', ''),
        }
        try:
            series = close[h['ticker']].dropna() if h['ticker'] in close.columns else None
            entry.update(analyze_series(series))
        except Exception as e:
            entry.update(analyze_series(None))
            entry['error'] = str(e)
        results.append(entry)

    up_count   = sum(1 for r in results if r.get('daily_trend') == 'up')
    down_count = sum(1 for r in results if r.get('daily_trend') == 'down')

    response = {
        'data':            results,
        'updated_at':      datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'holdings_source': holdings_source,
        'fund':            FUNDS[fund_id],
        'summary': {
            'up':      up_count,
            'down':    down_count,
            'unknown': len(results) - up_count - down_count,
        },
    }
    _cache[fund_id] = {'result': response, 'timestamp': now}
    return jsonify(response)


@app.route('/api/info/<path:ticker>')
def get_info(ticker):
    global _info_cache
    now = time.time()
    if ticker in _info_cache and now - _info_cache[ticker].get('ts', 0) < INFO_CACHE_TTL:
        return jsonify(_info_cache[ticker]['data'])
    try:
        info = yf.Ticker(ticker).info
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    description_en = info.get('longBusinessSummary', '')
    description_pl = ''
    if description_en:
        try:
            description_pl = GoogleTranslator(source='en', target='pl').translate(description_en)
        except Exception:
            description_pl = description_en

    data = {
        'name':        info.get('longName') or info.get('shortName', ''),
        'sector':      info.get('sector', ''),
        'industry':    info.get('industry', ''),
        'country':     info.get('country', ''),
        'description': description_pl,
        'website':     info.get('website', ''),
        'employees':   info.get('fullTimeEmployees'),
    }
    if data['description']:
        _info_cache[ticker] = {'data': data, 'ts': now}
    return jsonify(data)


@app.route('/api/chart/<path:ticker>')
def get_chart(ticker):
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=180)
    try:
        raw = yf.download(
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            auto_adjust=True, progress=False,
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    if raw.empty:
        return jsonify({'error': 'Brak danych dla tego tickera'}), 404

    close = raw['Close'].iloc[:, 0].dropna() if isinstance(raw.columns, pd.MultiIndex) else raw['Close'].dropna()
    ma5   = close.rolling(5).mean()
    ma20  = close.rolling(20).mean()
    fmt   = lambda v: round(float(v), 2) if not pd.isna(v) else None

    return jsonify({
        'ticker': ticker,
        'dates':  [d.strftime('%Y-%m-%d') for d in close.index],
        'prices': [fmt(p) for p in close.values],
        'ma5':    [fmt(v) for v in ma5],
        'ma20':   [fmt(v) for v in ma20],
    })


@app.route('/api/refresh')
def refresh_cache():
    fund_id = request.args.get('fund', 'tdiv')
    if fund_id in _cache:
        del _cache[fund_id]
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=False, threaded=True)

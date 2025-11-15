import dash
from dash import html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import yfinance as yf
from functools import lru_cache
from datetime import datetime
from nsepython import nse_eq
import threading
import time


# -------------------------------------------------------------------
# SAFETY WRAPPER
# -------------------------------------------------------------------
def safe_dict(value):
    return value if isinstance(value, dict) else {}


def safe_float(v):
    """Try to convert v to float, return None if not possible (handles 'NA', None, empty)."""
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == "" or s.upper() == "NA":
            return None
        # Remove commas commonly found in large numbers
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return None


def retry_with_backoff(func, symbol: str, max_retries: int = 3, base_delay: float = 1.0):
    """Retry a function with exponential backoff for rate limit errors."""
    for attempt in range(max_retries):
        try:
            return func(symbol)
        except Exception as e:
            if "rate" in str(e).lower() or "429" in str(e):
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"  Rate limited on {symbol}, retrying in {delay:.1f}s...")
                    time.sleep(delay)
                else:
                    print(f"  Max retries exceeded for {symbol}: {e}")
                    return None
            else:
                raise
    return None



# -------------------------------------------------------------------
# 1) Load all NSE symbols
# -------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_symbol_list():
    try:
        # Load symbols from local ticker.csv file
        df = pd.read_csv("nifty100.csv")
        df = df[["ticker"]]
        # Extract symbol without 'NSE:' prefix for API calls
        symbols = [t.replace("NSE:", "") for t in df["ticker"].unique()]
        return sorted(symbols)
    except Exception as e:
        print(f"Error loading ticker.csv: {e}")
        return ["RELIANCE", "TCS", "HDFCBANK", "INFY", "WIPRO"]  # fallback

SYMBOLS = load_symbol_list()


# -------------------------------------------------------------------
# 2) CONSOLIDATED NSE DATA FETCH
# -------------------------------------------------------------------
@lru_cache(maxsize=256)
def get_nse_data(symbol: str):
    """Fetch full NSE data once and cache."""
    try:
        return nse_eq(symbol)
    except Exception as e:
        print(f"NSE Data Fetch Error for {symbol}: {e}")
        return None


# -------------------------------------------------------------------
# 3) FUNDAMENTALS for single stock
# -------------------------------------------------------------------
@lru_cache(maxsize=256)
def get_fundamentals(symbol: str):
    try:
        data = get_nse_data(symbol)
        if not data:
            return None

        meta = safe_dict(data.get("metadata"))
        sec_info = safe_dict(data.get("securityInfo"))
        price_info = safe_dict(data.get("priceInfo"))
        industry = safe_dict(data.get("industryInfo"))

        # Use safe conversions to handle 'NA' and non-numeric values
        pe = safe_float(meta.get("pdSymbolPe") or meta.get("pdSectorPe"))
        last_price = safe_float(price_info.get("lastPrice") or price_info.get("close"))

        eps = None
        try:
            if pe is not None and last_price is not None and pe != 0:
                eps = last_price / pe
        except Exception:
            eps = None

        mcap = None
        issued = sec_info.get("issuedSize") or sec_info.get("issuedShares") or sec_info.get("issuedCapital")
        issued_f = safe_float(issued)
        try:
            if issued_f is not None and last_price is not None:
                mcap = issued_f * last_price
        except Exception:
            mcap = None

        sector = industry.get("industry") or industry.get("sector")
        week = safe_dict(price_info.get("weekHighLow"))

        return {
            "P/E": round(pe, 2) if pe is not None else None,
            "EPS": round(eps, 2) if eps is not None else None,
            "Market Cap": round(mcap, 2) if mcap is not None else None,
            "Sector": sector or "N/A",
            "52W High": week.get("max"),
            "52W Low": week.get("min"),
            "priceInfo": price_info,
            "lastPrice": last_price,
        }

    except Exception as e:
        print("Fundamental Fetch Error:", e)
        return None


# -------------------------------------------------------------------
# 4) VOLUME STATS (with retry backoff for rate limits)
# -------------------------------------------------------------------
@lru_cache(maxsize=256)
def get_volume_stats(symbol: str):
    """Return volume metrics with retry backoff for rate limits."""
    
    def _fetch_volume(sym):
        tk = yf.Ticker(sym + ".NS")
        
        avg_vol = None
        try:
            hist_30 = tk.history(period="30d", interval="1d")
            if "Volume" in hist_30.columns and not hist_30["Volume"].empty:
                avg_vol = float(hist_30["Volume"].mean())
        except Exception as e:
            print(f"  Avg volume fetch error for {sym}: {e}")

        todays_vol = None
        try:
            intraday = tk.history(period="1d", interval="1m")
            if "Volume" in intraday.columns and not intraday["Volume"].empty:
                todays_vol = float(intraday["Volume"].sum())
        except Exception:
            try:
                daily = tk.history(period="1d", interval="1d")
                if "Volume" in daily.columns and not daily["Volume"].empty:
                    todays_vol = float(daily["Volume"].iloc[-1])
            except Exception:
                todays_vol = None

        vol_change_pct = None
        try:
            if avg_vol and todays_vol is not None and avg_vol != 0:
                vol_change_pct = (todays_vol - avg_vol) / avg_vol * 100.0
        except Exception:
            pass

        return {
            "avg_volume": avg_vol,
            "todays_volume": todays_vol,
            "volume_change_pct": vol_change_pct,
        }
    
    # Retry with backoff on rate limits
    return retry_with_backoff(_fetch_volume, symbol, max_retries=3, base_delay=0.5) or {
        "avg_volume": None,
        "todays_volume": None,
        "volume_change_pct": None,
    }




# -------------------------------------------------------------------
# 5) FETCH DATA FOR ALL SYMBOLS (for table)
# -------------------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_all_stocks_data(symbols, batch_size: int = 50, workers: int = 5):
    """Fetch data for multiple stocks in parallel using ThreadPoolExecutor.

    - symbols: iterable of symbol strings
    - batch_size: number of symbols to process per batch (limits concurrent pressure)
    - workers: max threads per batch (reduced to minimize rate limiting)

    Returns a list of stock_data dicts (same shape as before).
    """
    all_data = []

    def _fetch_one(symbol):
        try:
            fund = get_fundamentals(symbol)
            vol = get_volume_stats(symbol)
            if not fund:
                return None

            price_info = safe_dict(fund.get('priceInfo', {}))
            # Normalize numeric fields
            last_price = fund.get('lastPrice')
            prev_close = safe_float(price_info.get('previousClose') or price_info.get('close'))
            today_open = safe_float(price_info.get('open'))

            # Calculate price change
            price_change = None
            price_change_pct = None
            try:
                if last_price is not None and prev_close is not None:
                    price_change = safe_float(last_price)
                    if price_change is not None:
                        price_change = price_change - prev_close
                        if prev_close != 0:
                            price_change_pct = (price_change / prev_close) * 100
            except Exception:
                price_change = None
                price_change_pct = None

            stock_data = {
                "SYMBOL": symbol,
                "STOCK_NAME": symbol,
                "INDUSTRIES": fund.get('Sector', 'N/A'),
                "10_DAY_CHART": "",
                "LAST_DAY_CLOSING_PRICE": prev_close,
                "TODAY_PRICE_OPEN": today_open,
                "TODAY_CURRENT_PRICE": last_price,
                "TODAY_CURRENT_PRICE_CHANGE": price_change,
                "TODAY_CURRENT_PRICE_CHANGE_PCT": price_change_pct,
                "TODAY_VOLUME_AVERAGE": vol.get('avg_volume'),
                "TODAY_VOLUME": vol.get('todays_volume'),
                "VOL_CHANGE_PCT": vol.get('volume_change_pct'),
                "10": "",
                "100": "",
                "5_DAY_PCT": "",
                "30_DAY_PCT": "",
                "MARKET_CAP_CR": fund.get('Market Cap'),
                "PE": fund.get('P/E'),
                "52WEEK_HIGH": fund.get('52W High'),
                "52WEEK_LOW": fund.get('52W Low'),
            }
            return stock_data
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None

    # Process in batches to avoid too much pressure on APIs
    symbols = list(symbols)
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        print(f"\n[Batch {batch_num}/{total_batches}] Fetching {len(batch)} symbols...")
        
        with ThreadPoolExecutor(max_workers=min(workers, len(batch))) as ex:
            futures = {ex.submit(_fetch_one, s): s for s in batch}
            for fut in as_completed(futures):
                res = None
                try:
                    res = fut.result()
                except Exception as e:
                    # _fetch_one already handles errors, but catch any executor errors here
                    s = futures.get(fut)
                    print(f"Executor error for {s}: {e}")
                if res:
                    all_data.append(res)
        
        # Add delay between batches to reduce API pressure
        if i + batch_size < len(symbols):
            print(f"  Batch {batch_num} complete. Waiting 2s before next batch...")
            time.sleep(2)

    return all_data


# -------------------------------------------------------------------
# 6) DASH APP SETUP
# -------------------------------------------------------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
server = app.server

app.layout = dbc.Container([
    
    html.H2("📊 AKS Market - NIFTY100 (NSE)", className="text-center my-4", style={"color": "#00D4FF"}),
    
    dbc.Row([
        dbc.Col([
            dbc.Button(
                "⟳ Refresh Data",
                id="refresh-all-btn",
                color="info",
                size="lg",
                style={
                    "fontWeight": "700",
                    "borderRadius": "5px",
                    "padding": "8px 16px",
                    "fontSize": "1rem",
                    "border": "none",
                    "background": "linear-gradient(135deg, #00D4FF 0%, #0099CC 100%)",
                    "color": "#000",
                    "cursor": "pointer",
                }
            )
        ], width=2),
        dbc.Col([
            html.Div(id="last-update-all", style={"textAlign": "right", "color": "#00D4FF", "fontSize": "0.9rem", "fontWeight": "600", "marginTop": "8px"})
        ], width=10),
    ], className="mb-3"),
    
    dcc.Interval(id="refresh-all", interval=180_000, n_intervals=0),
    dcc.Store(id="refresh-trigger-all", data=0),
    
    dbc.Row([
        dbc.Col([
            html.Div(id="stocks-table-container", style={"overflowX": "auto"})
        ], width=12),
    ])
    
], fluid=True, style={"backgroundColor": "#1a1a1a", "color": "#fff", "padding": "20px"})


# -------------------------------------------------------------------
# 7) CALLBACK → Manual Refresh
# -------------------------------------------------------------------
@app.callback(
    Output("refresh-trigger-all", "data"),
    Input("refresh-all-btn", "n_clicks"),
    prevent_initial_call=True
)
def manual_refresh_all(n_clicks):
    """Clear all caches on manual refresh."""
    if n_clicks:
        get_nse_data.cache_clear()
        get_fundamentals.cache_clear()
        get_volume_stats.cache_clear()
        print(f"[MANUAL REFRESH] All caches cleared at {datetime.now().strftime('%H:%M:%S')}")
    return n_clicks


# -------------------------------------------------------------------
# 8) CALLBACK → Update Timestamp
# -------------------------------------------------------------------
@app.callback(
    Output("last-update-all", "children"),
    Input("refresh-all", "n_intervals"),
    Input("refresh-trigger-all", "data")
)
def update_timestamp_all(intervals, manual_refresh):
    """Display last update timestamp."""
    now = datetime.now().strftime("%H:%M:%S")
    return f"Last updated: {now}"


# -------------------------------------------------------------------
# 9) CALLBACK → Generate Table
# -------------------------------------------------------------------
@app.callback(
    Output("stocks-table-container", "children"),
    Input("refresh-all", "n_intervals"),
    Input("refresh-trigger-all", "data")
)
def generate_table(intervals, manual_refresh):
    """Generate table with all stocks data (HTML table like the old UI)."""

    stocks_data = fetch_all_stocks_data(SYMBOLS)

    if not stocks_data:
        return html.P("No data available", style={"color": "#ff0000"})

    def format_value(val, decimals=2):
        """Format numeric values."""
        if val is None:
            return "-"
        try:
            v = float(val)
            return f"{v:,.{decimals}f}"
        except:
            return str(val)

    def format_pct(val):
        """Format percentage with color."""
        if val is None:
            return "-"
        try:
            v = float(val)
            color = "#00cc66" if v >= 0 else "#ff4d4d"  # Green if positive, red if negative
            symbol = "▲" if v >= 0 else "▼"
            return html.Span(f"{symbol} {v:.2f}%", style={"color": color, "fontWeight": "700"})
        except:
            return str(val)

    def format_currency(val, decimals=2):
        """Format currency."""
        if val is None:
            return "-"
        try:
            v = float(val)
            return f"₹{v:,.{decimals}f}"
        except:
            return str(val)

    def format_marketcap(val):
        """Format market cap in Crores."""
        if val is None:
            return "-"
        try:
            v = float(val)
            cr = v / 1e7
            return f"₹{cr:,.2f}Cr"
        except:
            return str(val)

    # Build table rows
    rows = []

    # Header row
    header_row = html.Tr([
        html.Th("SYMBOL", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "center"}),
        html.Th("INDUSTRIES", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px"}),
        html.Th("LAST CLOSE", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("OPEN", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("CURRENT", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("CHANGE", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("CHANGE %", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("AVG VOLUME", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("TODAY VOLUME", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("VOL CHANGE %", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("MARKET CAP (Cr)", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("P/E", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("52W HIGH", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
        html.Th("52W LOW", style={"backgroundColor": "#00D4FF", "color": "#000", "fontWeight": "700", "padding": "10px", "textAlign": "right"}),
    ])
    rows.append(header_row)

    # Data rows
    for i, stock in enumerate(stocks_data):
        row_bg = "#2a2a2a" if i % 2 == 0 else "#1a1a1a"
        
        row = html.Tr([
            html.Td(stock["SYMBOL"], style={"backgroundColor": row_bg, "padding": "8px", "fontWeight": "700", "color": "#00D4FF"}),
            html.Td(stock["INDUSTRIES"], style={"backgroundColor": row_bg, "padding": "8px", "fontSize": "0.9rem"}),
            html.Td(format_currency(stock["LAST_DAY_CLOSING_PRICE"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_currency(stock["TODAY_PRICE_OPEN"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_currency(stock["TODAY_CURRENT_PRICE"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontWeight": "700"}),
            html.Td(format_currency(stock["TODAY_CURRENT_PRICE_CHANGE"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_pct(stock["TODAY_CURRENT_PRICE_CHANGE_PCT"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_value(stock["TODAY_VOLUME_AVERAGE"], decimals=0), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontSize": "0.85rem"}),
            html.Td(format_value(stock.get("TODAY_VOLUME"), decimals=0), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontSize": "0.85rem"}),
            html.Td(format_pct(stock.get("VOL_CHANGE_PCT")), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_marketcap(stock["MARKET_CAP_CR"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_value(stock["PE"], decimals=2), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_currency(stock["52WEEK_HIGH"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_currency(stock["52WEEK_LOW"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
        ])
        rows.append(row)
    
    # Build table
    table = dbc.Table(
        html.Tbody(rows),
        bordered=False,
        className="table-dark",
        hover=True,
        responsive=True,
        style={"fontSize": "0.9rem", "marginTop": "20px"}
    )
    
    return table


# -------------------------------------------------------------------
# 10) RUN APP
# -------------------------------------------------------------------
if __name__ == "__main__":
    run_func = getattr(app, "run", None) or getattr(app, "run_server", None)
    if run_func is None:
        raise RuntimeError("No compatible Dash run API found on the 'app' object")
    run_func(debug=False, port=8051)  # Run on port 8051 to avoid conflict with main app

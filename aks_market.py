import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import yfinance as yf
# from functools import lru_cache # Replaced by diskcache
from datetime import datetime
from nsepython import nse_eq
import time
from dash.dependencies import ALL
import diskcache
import json
import os

# Initialize persistent cache
cache = diskcache.Cache("./cache_dir")

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
            err_str = str(e).lower()
            # Handle JSON decode error (often means blocking/rate limit) or explicit rate limit
            if "expecting value" in err_str or "rate" in err_str or "429" in err_str:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"  [Retry {attempt+1}/{max_retries}] Issue with {symbol} ({e}), waiting {delay:.1f}s...")
                    time.sleep(delay)
                else:
                    print(f"  Max retries exceeded for {symbol}: {e}")
                    return None
            else:
                # For other errors, just print and return None to avoid crashing
                print(f"  Error fetching {symbol}: {e}")
                return None
    return None


# -------------------------------------------------------------------
# 1) Load pre-generated symbol-industry mapping (FAST!)
# -------------------------------------------------------------------
@cache.memoize(expire=86400)  # Cache for 24 hours
def load_symbols_with_industries():
    """Load symbols and industries from pre-generated CSV - INSTANT LOAD!"""
    try:
        # Load from the pre-generated CSV with industries
        df = pd.read_csv("nifty100_with_industries.csv")
        
        print(f"✓ Loaded {len(df)} symbols with industries from CSV")
        
        # Create symbol -> industry mapping
        symbol_industry_map = dict(zip(df["symbol"], df["industry"]))
        
        # Remove N/A entries if you want
        # symbol_industry_map = {k: v for k, v in symbol_industry_map.items() if v != "N/A"}
        
        print(f"✓ Industry mapping ready with {len(symbol_industry_map)} stocks!")
        
        return symbol_industry_map
        
    except FileNotFoundError:
        print("ERROR: nifty100_with_industries.csv not found!")
        print("Please run fetch_static_data.py first to generate the file.")
        return {}
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return {}


SYMBOL_INDUSTRY_MAP = {}


# -------------------------------------------------------------------
# 2) CONSOLIDATED NSE DATA FETCH
# -------------------------------------------------------------------
@cache.memoize(expire=1800)  # Cache for 30 minutes
def get_nse_data(symbol: str):
    """Fetch full NSE data once and cache."""
    def _fetch(s):
        return nse_eq(s)
    
    return retry_with_backoff(_fetch, symbol, max_retries=3, base_delay=2.0)


# -------------------------------------------------------------------
# 3) FUNDAMENTALS for single stock
# -------------------------------------------------------------------
@cache.memoize(expire=1800)  # Cache for 30 minutes
def get_fundamentals(symbol: str):
    try:
        data = get_nse_data(symbol)
        if not data:
            return None

        meta = safe_dict(data.get("metadata"))
        sec_info = safe_dict(data.get("securityInfo"))
        price_info = safe_dict(data.get("priceInfo"))
        industry = safe_dict(data.get("industryInfo"))

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
# 4) HISTORICAL PRICE COMPARISON
# -------------------------------------------------------------------
@cache.memoize(expire=3600)  # Cache for 1 hour
def get_historical_comparison(symbol: str, days: int):
    """Get price comparison for N days ago."""
    try:
        tk = yf.Ticker(symbol + ".NS")
        
        # Fetch historical data - get extra days to account for weekends/holidays
        hist = tk.history(period=f"{days + 10}d", interval="1d")
        
        if hist.empty or len(hist) < 2:
            return None, None, None
        
        # Get current price (most recent)
        current_price = float(hist['Close'].iloc[-1])
        
        # Get price from N days ago (or closest available)
        if len(hist) >= days:
            old_price = float(hist['Close'].iloc[-days])
        else:
            # Use oldest available if not enough data
            old_price = float(hist['Close'].iloc[0])
        
        # Calculate change
        price_change = current_price - old_price
        price_change_pct = (price_change / old_price * 100) if old_price != 0 else None
        
        return old_price, price_change, price_change_pct
        
    except Exception as e:
        print(f"Historical data error for {symbol}: {e}")
        return None, None, None


# -------------------------------------------------------------------
# 5) VOLUME STATS
# -------------------------------------------------------------------
@cache.memoize(expire=1800)  # Cache for 30 minutes
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
    
    return retry_with_backoff(_fetch_volume, symbol, max_retries=3, base_delay=0.5) or {
        "avg_volume": None,
        "todays_volume": None,
        "volume_change_pct": None,
    }


# -------------------------------------------------------------------
# 6) FETCH DATA FOR SYMBOLS IN SELECTED INDUSTRY
# -------------------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed


def fetch_stocks_data_for_industry(symbols, days_comparison=10, batch_size: int = 10, workers: int = 2):
    """Fetch data for multiple stocks in parallel."""
    all_data = []

    def _fetch_one(symbol):
        try:
            fund = get_fundamentals(symbol)
            vol = get_volume_stats(symbol)
            hist_price, hist_change, hist_change_pct = get_historical_comparison(symbol, days_comparison)
            
            if not fund:
                return None

            price_info = safe_dict(fund.get('priceInfo', {}))
            last_price = fund.get('lastPrice')
            prev_close = safe_float(price_info.get('previousClose') or price_info.get('close'))
            today_open = safe_float(price_info.get('open'))

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
                "LAST_DAY_CLOSING_PRICE": prev_close,
                "TODAY_PRICE_OPEN": today_open,
                "TODAY_CURRENT_PRICE": last_price,
                "TODAY_CURRENT_PRICE_CHANGE": price_change,
                "TODAY_CURRENT_PRICE_CHANGE_PCT": price_change_pct,
                "HISTORICAL_PRICE": hist_price,
                "HISTORICAL_CHANGE": hist_change,
                "HISTORICAL_CHANGE_PCT": hist_change_pct,
                "TODAY_VOLUME_AVERAGE": vol.get('avg_volume'),
                "TODAY_VOLUME": vol.get('todays_volume'),
                "VOL_CHANGE_PCT": vol.get('volume_change_pct'),
                "MARKET_CAP_CR": fund.get('Market Cap'),
                "PE": fund.get('P/E'),
                "EPS": fund.get('EPS'),
                "52WEEK_HIGH": fund.get('52W High'),
                "52WEEK_LOW": fund.get('52W Low'),
            }
            return stock_data
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None

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
                    s = futures.get(fut)
                    print(f"Executor error for {s}: {e}")
                if res:
                    all_data.append(res)
        
        if i + batch_size < len(symbols):
            print(f"  Batch {batch_num} complete. Waiting 2s before next batch...")
            time.sleep(2)

    return all_data


# -------------------------------------------------------------------
# 6) DASH APP SETUP
# -------------------------------------------------------------------
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True
)

server = app.server
# Add custom CSS for dropdown styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .Select-control {
                background-color: #2a2a2a !important;
                border: 1px solid #444 !important;
            }
            .Select-menu-outer {
                background-color: #2a2a2a !important;
                border: 1px solid #444 !important;
                z-index: 9999 !important;
            }
            .Select-option {
                background-color: #2a2a2a !important;
                color: #fff !important;
                padding: 8px 10px !important;
            }
            .Select-option:hover {
                background-color: #00D4FF !important;
                color: #000 !important;
            }
            .Select-value-label {
                color: #fff !important;
            }
            .Select-placeholder {
                color: #aaa !important;
            }
            .Select-input > input {
                color: #fff !important;
            }
            .is-focused .Select-control {
                border-color: #00D4FF !important;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

app.layout = dbc.Container([
    
    html.H2("📊 AKS Market - NIFTY100", className="text-center my-4", style={"color": "#00D4FF"}),
    
    dbc.Row([
        dbc.Col([
            html.Label("Select Industry:", style={"color": "#00D4FF", "fontWeight": "600", "marginBottom": "5px"}),
            dcc.Dropdown(
                id="industry-filter",
                options=[],
                value=None,
                placeholder="Select an industry to load data...",
                style={
                    "backgroundColor": "#2a2a2a",
                    "color": "#fff",
                    "borderRadius": "5px",
                },
                className="custom-dropdown"
            )
        ], width=3),
        dbc.Col([
            html.Label("Days for Comparison:", style={"color": "#00D4FF", "fontWeight": "600", "marginBottom": "5px"}),
            dcc.Input(
                id="days-input",
                type="number",
                placeholder="Enter days (e.g., 10, 50, 200)",
                value=10,
                min=1,
                max=365,
                style={
                    "backgroundColor": "#2a2a2a",
                    "color": "#fff",
                    "border": "1px solid #444",
                    "borderRadius": "5px",
                    "padding": "8px",
                    "width": "100%"
                }
            )
        ], width=2),
        dbc.Col([
            dbc.Button(
                "⟳ Refresh Data",
                id="refresh-btn",
                color="info",
                size="lg",
                disabled=True,
                style={
                    "fontWeight": "700",
                    "borderRadius": "5px",
                    "padding": "8px 16px",
                    "fontSize": "1rem",
                    "border": "none",
                    "background": "linear-gradient(135deg, #00D4FF 0%, #0099CC 100%)",
                    "color": "#000",
                    "cursor": "pointer",
                    "marginTop": "28px"
                }
            )
        ], width=2),
        dbc.Col([
            html.Div(id="update-timestamp", style={"textAlign": "right", "color": "#00D4FF", "fontSize": "0.9rem", "fontWeight": "600", "marginTop": "35px"})
        ], width=5),
    ], className="mb-3"),
    
    # Auto-refresh interval (5 minutes = 300000 ms)
    dcc.Interval(
        id='auto-refresh-interval',
        interval=5*60*1000,  # 5 minutes in milliseconds
        n_intervals=0,
        disabled=True  # Will be enabled when industry is selected
    ),
    
    dcc.Store(id="symbol-industry-map", data={}),
    dcc.Store(id="stocks-data-store", data={}),
    dcc.Store(id="current-days", data=10),
    dcc.Store(id="sort-column", data=None),
    dcc.Store(id="sort-direction", data="asc"),
    
    dbc.Row([
        dbc.Col([
            dcc.Loading(
                id="loading-1",
                type="circle",
                color="#00D4FF",
                children=html.Div(id="table-container", style={"overflowX": "auto", "minHeight": "200px"})
            )
        ], width=12),
    ])
    
], fluid=True, style={"backgroundColor": "#1a1a1a", "color": "#fff", "padding": "20px"})


# -------------------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------------------

# Callback 1: Load initial symbol-industry mapping (INSTANT NOW!)
@app.callback(
    [Output("symbol-industry-map", "data"),
     Output("industry-filter", "options")],
    Input("industry-filter", "id")
)
def initialize_data(_):
    """Load symbol-industry mapping from CSV - INSTANT LOAD!"""
    global SYMBOL_INDUSTRY_MAP
    
    print("Loading pre-generated industry data...")
    SYMBOL_INDUSTRY_MAP = load_symbols_with_industries()
    
    if not SYMBOL_INDUSTRY_MAP:
        print("WARNING: No data loaded! Please run fetch_static_data.py first!")
        return {}, []
    
    industries = set(SYMBOL_INDUSTRY_MAP.values())
    industries.discard("N/A")
    
    # Add "All" option at the beginning
    options = [{"label": "🌐 All Industries", "value": "ALL"}]
    options.extend([{"label": ind, "value": ind} for ind in sorted(industries)])
    
    print(f"✓ Dropdown ready with {len(options)} options (including 'All')")
    
    return SYMBOL_INDUSTRY_MAP, options


# Callback 2: Enable/Disable refresh button and auto-refresh
@app.callback(
    [Output("refresh-btn", "disabled"),
     Output("auto-refresh-interval", "disabled")],
    Input("industry-filter", "value")
)
def toggle_refresh_and_interval(selected_industry):
    """Enable refresh button and auto-refresh only when an industry is selected."""
    is_disabled = selected_industry is None
    return is_disabled, is_disabled


# Callback 3: Fetch data when industry selected, refreshed manually, or auto-refreshed
@app.callback(
    [Output("stocks-data-store", "data"),
     Output("update-timestamp", "children"),
     Output("current-days", "data")],
    [Input("industry-filter", "value"),
     Input("refresh-btn", "n_clicks"),
     Input("auto-refresh-interval", "n_intervals"),
     Input("days-input", "value")],
    State("symbol-industry-map", "data"),
    running=[
        (Output("refresh-btn", "disabled"), True, False),
    ]
)
def fetch_industry_data(selected_industry, manual_clicks, auto_intervals, days_input, symbol_industry_map):
    """Fetch stock data for the selected industry or all industries."""
    
    if not selected_industry or not symbol_industry_map:
        return {}, "", 10
    
    # Use default 10 days if invalid input
    days_comparison = days_input if days_input and days_input > 0 else 10
    
    # Determine if this was triggered by manual refresh or auto-refresh
    ctx = dash.callback_context
    trigger_source = "Initial Load"
    
    if ctx.triggered:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if trigger_id == "refresh-btn":
            trigger_source = "Manual Refresh"
        elif trigger_id == "auto-refresh-interval":
            trigger_source = "Auto Refresh"
        elif trigger_id == "days-input":
            trigger_source = "Days Changed"
    
    # Clear caches on manual refresh or auto refresh
    if manual_clicks or auto_intervals > 0 or trigger_source == "Days Changed":
        # Only clear relevant keys if possible, but for now clear all is safer for consistency
        # cache.clear() # Don't clear everything, just let it expire or overwrite
        print(f"[{trigger_source.upper()}] Refreshing data (using cache if valid)...")
    
    # Get symbols for selected industry or all symbols
    if selected_industry == "ALL":
        symbols_in_industry = list(symbol_industry_map.keys())
        display_name = "All Industries"
    else:
        symbols_in_industry = [symbol for symbol, industry in symbol_industry_map.items() 
                              if industry == selected_industry]
        display_name = selected_industry
    
    if not symbols_in_industry:
        return {}, f"No stocks found for {display_name}", days_comparison
    
    print(f"\n[FETCHING DATA - {trigger_source}] Loading {len(symbols_in_industry)} stocks for: {display_name} (comparing {days_comparison} days)")
    
    # Fetch data with historical comparison
    stocks_data = fetch_stocks_data_for_industry(symbols_in_industry, days_comparison=days_comparison)
    
    # Create timestamp with source indicator
    now = datetime.now().strftime("%H:%M:%S")
    timestamp = f"Last updated: {now} | {len(stocks_data)} stocks | {days_comparison}D comparison | Next refresh: 5 min"
    
    return {selected_industry: stocks_data}, timestamp, days_comparison


# Callback 4: Handle column sorting
@app.callback(
    [Output("sort-column", "data"),
     Output("sort-direction", "data")],
    Input({"type": "sort-button", "column": ALL}, "n_clicks"),
    [State("sort-column", "data"),
     State("sort-direction", "data")],
    prevent_initial_call=True
)
def handle_sort(n_clicks_list, current_column, current_direction):
    ctx = dash.callback_context
    if not ctx.triggered:
        return current_column, current_direction

    # Extract which button triggered
    triggered = ctx.triggered[0]["prop_id"].split(".")[0]
    triggered_id = eval(triggered)   # Convert string dict to actual dict

    column = triggered_id["column"]

    # Toggle direction
    if column == current_column:
        new_direction = "desc" if current_direction == "asc" else "asc"
    else:
        new_direction = "desc"  # default

    return column, new_direction

# Callback 5: Generate table with sorting
@app.callback(
    Output("table-container", "children"),
    [Input("stocks-data-store", "data"),
     Input("industry-filter", "value"),
     Input("current-days", "data"),
     Input("sort-column", "data"),
     Input("sort-direction", "data")]
)
def generate_table(stocks_data_store, selected_industry, days, sort_column, sort_direction):
    """Generate table with stock data and sorting."""

    if not selected_industry:
        return html.Div([
            html.P("👆 Please select an industry from the dropdown to view stock data", 
                   style={"color": "#00D4FF", "fontSize": "1.2rem", "textAlign": "center", "marginTop": "50px"})
        ])

    stocks_data = stocks_data_store.get(selected_industry, [])

    if not stocks_data:
        return html.Div([
            html.P("No data loaded yet. Please wait...", 
                   style={"color": "#888", "fontSize": "1rem", "textAlign": "center", "marginTop": "50px"})
        ])

    # Use days from store (defaults to 10 if not set)
    days = days if days and days > 0 else 10
    
    # Sort data if sort column is specified
    if sort_column and stocks_data:
        # Map column names to data keys
        column_mapping = {
            "SYMBOL": "SYMBOL",
            "INDUSTRIES": "INDUSTRIES",
            "LAST_CLOSE": "LAST_DAY_CLOSING_PRICE",
            "OPEN": "TODAY_PRICE_OPEN",
            "CURRENT": "TODAY_CURRENT_PRICE",
            "1D_CHANGE": "TODAY_CURRENT_PRICE_CHANGE",
            "1D_CHANGE_PCT": "TODAY_CURRENT_PRICE_CHANGE_PCT",
            "ND_PRICE": "HISTORICAL_PRICE",
            "ND_CHANGE": "HISTORICAL_CHANGE",
            "ND_CHANGE_PCT": "HISTORICAL_CHANGE_PCT",
            "52W_HIGH": "52WEEK_HIGH",
            "52W_LOW": "52WEEK_LOW",
            "MARKET_CAP": "MARKET_CAP_CR",
            "PE": "PE",
            "EPS": "EPS",
            "AVG_VOLUME": "TODAY_VOLUME_AVERAGE",
            "TODAY_VOLUME": "TODAY_VOLUME",
            "VOL_CHANGE_PCT": "VOL_CHANGE_PCT",
        }
        
        data_key = column_mapping.get(sort_column)
        if data_key:
            # Sort with None values at the end
            reverse = (sort_direction == "desc")
            stocks_data = sorted(
                stocks_data,
                key=lambda x: (x.get(data_key) is None, x.get(data_key) if x.get(data_key) is not None else 0),
                reverse=reverse
            )
    
    def format_value(val, decimals=2):
        if val is None:
            return "-"
        try:
            v = float(val)
            return f"{v:,.{decimals}f}"
        except:
            return str(val)

    def format_pct(val):
        if val is None:
            return "-"
        try:
            v = float(val)
            color = "#00cc66" if v >= 0 else "#ff4d4d"
            symbol = "▲" if v >= 0 else "▼"
            return html.Span(f"{symbol} {v:.2f}%", style={"color": color, "fontWeight": "700"})
        except:
            return str(val)

    def format_currency(val, decimals=2):
        if val is None:
            return "-"
        try:
            v = float(val)
            return f"₹{v:,.{decimals}f}"
        except:
            return str(val)

    def format_marketcap(val):
        if val is None:
            return "-"
        try:
            v = float(val)
            cr = v / 1e7
            return f"₹{cr:,.2f}Cr"
        except:
            return str(val)

    # Build table
    rows = []
    
    # Helper function to create sortable header
    def create_header(label, column_key, align="center"):
        is_sorted = (sort_column == column_key)
        sort_indicator = " ▼" if sort_direction == "desc" else " ▲" if is_sorted else ""

        return html.Th(
            html.Button(
                label + sort_indicator,
                id={"type": "sort-button", "column": column_key},
                n_clicks=0,
                style={
                    "backgroundColor": "#00D4FF",
                    "color": "#000",
                    "fontWeight": "700",
                    "padding": "10px",
                    "textAlign": align,
                    "border": "none",
                    "cursor": "pointer",
                    "width": "100%",
                    "fontSize": "0.9rem"
                }
            ),
            style={"backgroundColor": "#00D4FF", "padding": "0"}
        )


    # Header
    header_row = html.Tr([
        create_header("SYMBOL", "SYMBOL", "center"),
        create_header("INDUSTRIES", "INDUSTRIES", "left"),
        create_header("LAST CLOSE", "LAST_CLOSE", "right"),
        create_header("OPEN", "OPEN", "right"),
        create_header("CURRENT", "CURRENT", "right"),
        create_header("1D CHANGE", "1D_CHANGE", "right"),
        create_header("1D CHANGE %", "1D_CHANGE_PCT", "right"),
        create_header(f"{days}D PRICE", "ND_PRICE", "right"),
        create_header(f"{days}D CHANGE", "ND_CHANGE", "right"),
        create_header(f"{days}D CHANGE %", "ND_CHANGE_PCT", "right"),
        create_header("52W HIGH", "52W_HIGH", "right"),
        create_header("52W LOW", "52W_LOW", "right"),
        create_header("MARKET CAP (Cr)", "MARKET_CAP", "right"),
        create_header("P/E", "PE", "right"),
        create_header("EPS", "EPS", "right"),
        create_header("AVG VOLUME", "AVG_VOLUME", "right"),
        create_header("TODAY VOLUME", "TODAY_VOLUME", "right"),
        create_header("VOL CHANGE %", "VOL_CHANGE_PCT", "right"),
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
            html.Td(format_currency(stock.get("HISTORICAL_PRICE")), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontSize": "0.9rem"}),
            html.Td(format_currency(stock.get("HISTORICAL_CHANGE")), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontSize": "0.9rem"}),
            html.Td(format_pct(stock.get("HISTORICAL_CHANGE_PCT")), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_currency(stock["52WEEK_HIGH"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_currency(stock["52WEEK_LOW"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_marketcap(stock["MARKET_CAP_CR"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_value(stock["PE"], decimals=2), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_value(stock.get("EPS"), decimals=2), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
            html.Td(format_value(stock["TODAY_VOLUME_AVERAGE"], decimals=0), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontSize": "0.85rem"}),
            html.Td(format_value(stock["TODAY_VOLUME"], decimals=0), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right", "fontSize": "0.85rem"}),
            html.Td(format_pct(stock["VOL_CHANGE_PCT"]), style={"backgroundColor": row_bg, "padding": "8px", "textAlign": "right"}),
        ])
        rows.append(row)

    return html.Table(
        [html.Thead(rows[0]), html.Tbody(rows[1:])],
        style={"width": "100%", "borderCollapse": "collapse", "minWidth": "1200px"}
    )


if __name__ == "__main__":
    app.run(debug=False, port=8051)
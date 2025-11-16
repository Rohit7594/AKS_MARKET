"""
Static Data Fetcher for NSE Stocks
Fetches ticker and industry information once and stores in CSV
Run this script periodically (e.g., weekly) to update static data
"""

import pandas as pd
from nsepython import nse_eq
import time
from datetime import datetime


def safe_dict(value):
    """Return value if dict, else empty dict"""
    return value if isinstance(value, dict) else {}


def fetch_industry_for_symbol(symbol: str, max_retries: int = 3):
    """Fetch industry/sector for a single symbol with retry logic"""
    for attempt in range(max_retries):
        try:
            print(f"  Fetching {symbol}...", end=" ")
            data = nse_eq(symbol)
            
            if data:
                industry_info = safe_dict(data.get("industryInfo"))
                sector = (
                    industry_info.get("industry") or 
                    industry_info.get("sector") or 
                    industry_info.get("basicIndustry") or
                    "N/A"
                )
                print(f"✓ {sector}")
                return sector
            else:
                print("✗ No data")
                return "N/A"
                
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠ Retry {attempt + 1}/{max_retries}")
                time.sleep(1 * (attempt + 1))  # Exponential backoff
            else:
                print(f"✗ Failed: {e}")
                return "N/A"
    
    return "N/A"


def load_and_enrich_tickers(input_csv: str = "nifty100.csv", 
                           output_csv: str = "nifty100_with_industries.csv"):
    """
    Load tickers from CSV, fetch their industries, and save enriched data
    
    Args:
        input_csv: Input CSV file with 'ticker' column
        output_csv: Output CSV file with ticker and industry columns
    """
    
    print("=" * 60)
    print("NSE Static Data Fetcher")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Load existing tickers
    try:
        df = pd.read_csv(input_csv)
        print(f"✓ Loaded {len(df)} tickers from {input_csv}")
    except FileNotFoundError:
        print(f"✗ Error: {input_csv} not found!")
        return
    except Exception as e:
        print(f"✗ Error loading CSV: {e}")
        return
    
    # Extract clean symbols
    if "ticker" not in df.columns:
        print("✗ Error: 'ticker' column not found in CSV!")
        return
    
    df["symbol"] = df["ticker"].str.replace("NSE:", "", regex=False)
    symbols = df["symbol"].unique().tolist()
    
    print(f"✓ Found {len(symbols)} unique symbols")
    print(f"\nFetching industry data...\n")
    
    # Fetch industry for each symbol
    results = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}]", end=" ")
        
        industry = fetch_industry_for_symbol(symbol)
        
        results.append({
            "ticker": f"NSE:{symbol}",
            "symbol": symbol,
            "industry": industry
        })
        
        # Rate limiting: pause after every 10 requests
        if i % 10 == 0:
            print(f"\n  ⏳ Progress: {i}/{len(symbols)} completed. Cooling down...\n")
            time.sleep(2)
    
    # Create DataFrame and save
    result_df = pd.DataFrame(results)
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total symbols processed: {len(result_df)}")
    print(f"Successfully fetched: {len(result_df[result_df['industry'] != 'N/A'])}")
    print(f"Failed/Unknown: {len(result_df[result_df['industry'] == 'N/A'])}")
    
    # Industry distribution
    print("\nIndustry Distribution:")
    print("-" * 60)
    industry_counts = result_df["industry"].value_counts()
    for industry, count in industry_counts.items():
        print(f"  {industry}: {count}")
    
    # Save to CSV
    try:
        result_df.to_csv(output_csv, index=False)
        print(f"\n✓ Data saved to: {output_csv}")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Error saving CSV: {e}")


if __name__ == "__main__":
    # Run the fetcher
    load_and_enrich_tickers(
        input_csv="nifty100.csv",
        output_csv="nifty100_with_industries.csv"
    )
    
    print("\n💡 TIP: Run this script weekly to keep industry data updated!")
    print("💡 Use 'nifty100_with_industries.csv' in your Dash app for instant loading!")
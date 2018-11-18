import pandas
import os
import loaders
import web
import argparse

def build_low_volatility_index():
    listings_df = loaders.get_preprocessed_listings_df()

    # Require at least a 3% dividend yield
    listings_df = listings_df.where(listings_df.dividend_yield >= 0.03)

    index_df = pandas.DataFrame(columns=['symbol'])
    for row in listings_df.itertuples():
        symbol = web.format_symbol_for_alphavantage(row.Ticker)

        prices = loaders.load_prices_from_csv(symbol)
        if prices is None:
            continue

        # scale prices (feature scaling)
        min_price = prices['close'].min()
        max_price = prices['close'].max()
        prices['adjusted close'] = prices['close'].apply(lambda x: (x - min_price) / (max_price - min_price))

        prices['1 day change'] = prices['adjusted close'].diff(periods=1)
        prices['5 day change'] = prices['adjusted close'].diff(periods=5) # 1 week
        prices['20 day change'] = prices['adjusted close'].diff(periods=20) # 1 month
        prices['60 day change'] = prices['adjusted close'].diff(periods=60)  # 6 month

        index_df = index_df.append({
            'symbol': symbol,
            'daily variance': prices['1 day change'].var(),
            'weekly variance': prices['5 day change'].var(),
            '1 month variance': prices['20 day change'].var(),
            '3 month variance': prices['60 day change'].var(),
            'name': row.Name,
            'sector': row.Sector,
            'sub-sector': row._8,
            'dividend yield': round(row.dividend_yield, 3)
        }, ignore_index=True)
        index_df['volatility score'] = \
            (0.1 * index_df['daily variance']) + \
            (0.1 * index_df['weekly variance']) + \
            (0.3 * index_df['1 month variance']) + \
            (0.5 * index_df['3 month variance'])

        index_df = index_df.sort_values(by='volatility score', ascending=True)
    with open('./data/low_vol_index_out.csv', 'w') as outfile:
        index_df.to_csv(outfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Description tools for creation custom indexes of Toronto Stock Exchange listings.')
    parser.add_argument('--download-data', '-d',
                        dest='download_data',
                        action='store_true',
                        help='Downloads listing prices and dividend data from various sources.')
    parser.add_argument('--index', '-i',
                        dest='index',
                        help='The name of the index to build')
    args = parser.parse_args()

    if args.download_data:
        # Add dividend data to listings data
        loaders.add_dividend_payment_data()

        # Download price data for each holding
        loaders.download_all_prices()

        # Calculate dividend yields based on latest prices
        loaders.add_dividend_yield()

    index_name = args.index
    if index_name:
        if index_name == 'low-volatility':
            build_low_volatility_index()

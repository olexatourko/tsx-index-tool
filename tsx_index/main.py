import pandas
import os
import loaders
import web

if __name__ == '__main__':
    listings_df = loaders.get_preprocessed_listings_df()

    df = pandas.DataFrame(columns=['symbol'])
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
        prices['60 day change'] = prices['adjusted close'].diff(periods=20)  # 6 month

        df = df.append({
            'symbol': symbol,
            'daily variance': prices['1 day change'].var(),
            'weekly variance': prices['5 day change'].var(),
            '1 month variance': prices['20 day change'].var(),
            '3 month variance': prices['60 day change'].var(),
            'name': row.Name,
            'sector': row.Sector
        }, ignore_index=True)
        df['volatility score'] = \
            (0.1 * df['daily variance']) + \
            (0.1 * df['weekly variance']) + \
            (0.3 * df['1 month variance']) + \
            (0.5 * df['3 month variance'])

    df = df.sort_values(by='volatility score', ascending=True)

    with open('./data/low_vol_index_out.csv', 'w') as outfile:
        df.to_csv(outfile)

    # import pudb; pudb.set_trace()

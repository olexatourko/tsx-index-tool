import pandas
import os
from os.path import dirname, abspath
import time
import csv
import web

ROOT_PATH = dirname(dirname(abspath(__file__)))

def get_raw_listings_df():
    """
    Gets the raw listing data from the .xlsx file downloaded on the TMX discovery tool page
    :return: DataFrame
    """
    with open(os.path.join(ROOT_PATH, 'data/mig_report.xlsx')) as infile:
        listings_df = pandas.read_excel(infile, dtype={'Ticker': str})
    excluded_sectors = ['Closed-End Funds', 'ETP']
    # Remove any funds
    listings_df = listings_df[~listings_df.Sector.isin(excluded_sectors)]
    # Remove any preferred shares
    listings_df = listings_df[~listings_df['Ticker'].str.contains('\.PR\.', na=False)]
    # Remove stocks with market caps < 100,000,000
    min_cap = 100000000
    listings_df = listings_df[listings_df['QMV($)'] >= min_cap]
    return listings_df


def get_preprocessed_listings_df():
    """
    Gets the preprocessed listing (raw + dividends) data from the saved .csv file
    :return: DataFrane
    """
    with open(os.path.join(ROOT_PATH, 'data/preprocessed_listings.csv'), 'r') as infile:
        listings_df = pandas.read_csv(infile, dtype={'Ticker': str})

    return listings_df


def add_dividend_payment_data():
    """
    Adds dividend data to raw listings and saves to a CSV
    :return: None
    """
    raw_listings_df = get_raw_listings_df()

    def func(row):
        print('Getting dividend info for {}'.format(row.Ticker))
        dividend_amount, dividend_currency, dividend_frequency = web.get_dividend_from_tmx(row.Ticker)
        return pandas.Series((dividend_amount, dividend_currency, dividend_frequency))

    raw_listings_df[['dividend_amount', 'dividend_currency', 'dividend_frequency']] = raw_listings_df.apply(func, axis=1 )

    with open(os.path.join(ROOT_PATH, 'data/preprocessed_listings.csv'), 'w') as outfile:
        raw_listings_df.to_csv(outfile, index=False)


def add_dividend_yield():
    """
    Using the existing dividend payment data, add a column for the dividend yield based on the latest prices.
    :return: None
    """
    listings_df = get_preprocessed_listings_df()
    frequencies = {
        'Monthly': 12,
        'Quarterly': 4,
        'Semi-Annual': 2,
        'Annual': 1
    }

    def func(row):
        symbol = web.format_symbol_for_alphavantage(row.Ticker)
        prices = load_prices_from_csv(symbol)
        if prices is not None:
            last_price = prices.sort_values(by='date').iloc[-1]['close']
            if row.dividend_frequency in frequencies:
                return row.dividend_amount * frequencies[row.dividend_frequency] / last_price

        return None

    listings_df['dividend_yield'] = listings_df.apply(func, axis=1 )
    with open(os.path.join(ROOT_PATH, 'data/preprocessed_listings.csv'), 'w') as outfile:
        listings_df.to_csv(outfile, index=False)


def load_prices_from_csv(symbol):
    file_name = os.path.join(ROOT_PATH, 'data/prices/{}.csv'.format(symbol))
    if not os.path.exists(file_name):
        return None

    with open(file_name, 'r') as infile:
        prices = pandas.read_csv(infile, parse_dates=['date'])

    return prices


def download_all_prices():
    listings_df = get_raw_listings_df()

    # Load historical price data (save to files if they don't already exist)
    for symbol in listings_df.Ticker:
        symbol = web.format_symbol_for_alphavantage(symbol)

        while True:
            print('Getting prices for {}'.format(symbol))
            if os.path.exists('./data/prices/{}.csv'.format(symbol)):
                print('Found file')
                break

            try:
                prices = web.get_prices_from_api(symbol)
                save_json_prices_to_csv(symbol, prices)
                break

            except web.AlphaVantageException as e:
                if e.message != 'API call throttled':
                    print(e)
                    break
                else:
                    print('API throttled, waiting 5 minutes.')
                    time.sleep(60 * 5)



def save_json_prices_to_csv(symbol, json_price_dict):
    file_name = os.path.join(ROOT_PATH, 'data/prices/{}.csv'.format(symbol))
    with open(file_name, 'w') as outfile:
        fieldnames = ['date', 'open', 'high', 'low', 'close', 'adjusted close', 'volume', 'dividend amount', 'split coefficient']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writerow({f: f for f in fieldnames})
        for k in sorted(json_price_dict):
            writer.writerow({
                'date': k,
                'open': json_price_dict[k]['1. open'],
                'high': json_price_dict[k]['2. high'],
                'low': json_price_dict[k]['3. low'],
                'close': json_price_dict[k]['4. close'],
                'adjusted close': json_price_dict[k]['5. adjusted close'],
                'volume': json_price_dict[k]['6. volume'],
                'dividend amount': json_price_dict[k]['7. dividend amount'],
                'split coefficient': json_price_dict[k]['8. split coefficient']
            })
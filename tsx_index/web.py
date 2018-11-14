import re
import requests
import json
import os
import settings
from bs4 import BeautifulSoup


class AlphaVantageException(Exception):
    def __init__(self, message, symbol):
        self.message = message
        self.symbol = symbol

    def __str__(self):
        return '{}: {}'.format(self.message, self.symbol)


def get_prices_from_api(symbol):
    r = requests.get(
        url='https://www.alphavantage.co/query',
        params={
            'function': 'TIME_SERIES_DAILY_ADJUSTED',
            'symbol': symbol,
            'outputsize': 'full',
            'apikey': os.get('AV_API_KEY')
        }
    )
    response = json.loads(r.content)
    if 'Time Series (Daily)' in response:
        return response['Time Series (Daily)']
    elif 'Error Message' in response:
        raise AlphaVantageException(message='Invalid API call', symbol=symbol)
    else:
        raise AlphaVantageException(message='API call throttled', symbol=symbol)


def format_symbol_for_alphavantage(symbol):
    symbol = '{}.TO'.format(symbol)
    symbol = re.sub('\.A', '-A', symbol)
    symbol = re.sub('\.B', '-B', symbol)
    symbol = re.sub('\.UN', '-UN', symbol)
    return symbol


def get_dividend_from_tmx(symbol):
    possible_symbols = [symbol]
    possible_symbols.append(re.sub('\.A', '', symbol))
    possible_symbols.append(re.sub('\.B', '', symbol))
    for symbol in possible_symbols:
        try:
            requests.get('https://web.tmxmoney.com/quote.php?qm_symbol={}')
            r = requests.get(
                url='https://web.tmxmoney.com/quote.php',
                params={
                    'qm_symbol': symbol
                }
            )
            soup = BeautifulSoup(r.content, features='html.parser')
            dividend_amount = soup.find(class_='quote-tabs-content') \
                .find_all(class_='detailed-quote-table')[2] \
                .find_all('tr')[0] \
                .find_all('td')[1] \
                .encode_contents()
            amount_match_obj = re.search('^[0-9]+\.[0-9]+', dividend_amount)
            currency_match_obj = re.search('[a-zA-Z]+$', dividend_amount)

            if amount_match_obj:
                dividend_amount = float(amount_match_obj.group())
            else:
                return None, None, None

            if currency_match_obj:
                dividend_currency = currency_match_obj.group()
            else:
                dividend_currency = None

            dividend_frequency = soup.find(class_='quote-tabs-content') \
                .find_all(class_='detailed-quote-table')[2] \
                .find_all('tr')[1] \
                .find_all('td')[1] \
                .encode_contents()
            return dividend_amount, dividend_currency, dividend_frequency
        except Exception as e:
            continue

    return None, None, None
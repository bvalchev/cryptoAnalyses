import pandas as pd
import re
import csv
import numpy as np
from datetime import timedelta



def csv_writer(path='../data/', outfile='clearedPosts', columns=''):
    targetfile = open(path + outfile + '.csv', mode='w', encoding='utf-8', newline='\n')
    writer = csv.writer(targetfile, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)

    return writer


def get_cleaned_text(text, should_remove_signs):
    # Remove Unicode
    cleaned_text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    # Remove Mentions
    cleaned_text = re.sub(r'@\w+', '', cleaned_text)
    # Remove the numbers
    #cleaned_text = re.sub(r'[0-9]', '', cleaned_text)
    # Remove the doubled space
    cleaned_text = re.sub(r'\s{2,}', ' ', cleaned_text)
    # Remove the doubled comma
    cleaned_text = re.sub(r',{2,}', ', ', cleaned_text)
    # Remove newlines and bad escape symbols
    cleaned_text = re.sub(r'\\u.{4}', '', cleaned_text)
    cleaned_text = re.sub(r'\\n', '', cleaned_text)
    # Remove unnecessary plus symbols
    cleaned_text = re.sub(r'\+', '', cleaned_text)
    cleaned_text = re.sub(r'#', '', cleaned_text)
    cleaned_text = re.sub(r'\(', '', cleaned_text)
    cleaned_text = re.sub(r'\)', '', cleaned_text)
    cleaned_text = re.sub(r':', '', cleaned_text)
    cleaned_text = re.sub(r';', '', cleaned_text)
    cleaned_text = re.sub(r'_', '', cleaned_text)
    cleaned_text = re.sub(r'\\', ' ', cleaned_text)
    cleaned_text = re.sub(r'-', ' ', cleaned_text)
    cleaned_text = re.sub(r'/', ' ', cleaned_text)
    cleaned_text = re.sub(r'\'', '', cleaned_text)
    cleaned_text = re.sub(r'\"', '', cleaned_text)
    cleaned_text = re.sub(r'\.([A-Za-z]{1})', r'. \1', cleaned_text)

    if should_remove_signs:
        cleaned_text = re.sub(r'\?', ' ', cleaned_text)
        cleaned_text = re.sub(r'\.', ' ', cleaned_text)
        cleaned_text = re.sub(r'!', ' ', cleaned_text)

    return cleaned_text

def get_processed_posts(posts):
    for i, row in posts.iterrows():
        currentMessage = row['message']

        if currentMessage is not None and type(currentMessage) == str:
            clearedMessage = get_cleaned_text(currentMessage, False)
            row['message'] = clearedMessage
        else:
            posts.drop(labels=[i], axis=0)

        print(i)

    return posts

def get_coin_info_for_date(coinInfo, date):
    return coinInfo.loc[coinInfo['Date'] == date]['Close'].values[0]


def append_columns(merged, coinInfo):
    for i, row in merged.iterrows():
        rowDate = row['Date']
        previousDay = rowDate - timedelta(days=1)
        nextDay = rowDate + timedelta(days=1)
        afterThreeDaysDate = rowDate + timedelta(days=3)
        movementSincePreviousDay = get_coin_info_for_date(coinInfo, previousDay) - row['Close']
        movementTheDayAfter = row['Close'] - get_coin_info_for_date(coinInfo, nextDay)
        movementThreeDayAfter = row['Close'] - get_coin_info_for_date(coinInfo, afterThreeDaysDate)

        merged.loc[i, ['previous_day_closing_price']] = get_coin_info_for_date(coinInfo, previousDay)
        merged.loc[i, ['next_day_closing_price']] = get_coin_info_for_date(coinInfo, nextDay)
        merged.loc[i, ['after_three_days_closing_price']] = get_coin_info_for_date(coinInfo, afterThreeDaysDate)
        merged.loc[i, ['movement_since_previous_day']] = movementSincePreviousDay
        merged.loc[i, ['movement_the_day_after']] = movementTheDayAfter
        merged.loc[i, ['movement_three_days_after']] = movementThreeDayAfter

    return merged


coinInfo = pd.read_csv('../data/cryptoInfo/coin_Bitcoin.csv')
bitcoinPosts = pd.read_json('../data/groupMessages/group_messages_binance.json')

selectedColumns = ['id', 'date', 'views', 'message', 'out', 'post_author', 'replies']

bitcoinPosts = bitcoinPosts[selectedColumns]
bitcoinNames = ['BTC', 'btc', 'Btc', 'Bitcoin', 'bitcoin', 'bit']


processedPosts = bitcoinPosts #get_processed_posts(bitcoinPosts)


filteredPosts = processedPosts[processedPosts.message.str.contains('|'.join(bitcoinNames), na=False)]
filteredPosts["date"] = pd.to_datetime(filteredPosts["date"], utc=True).apply(lambda t: t.replace(second=0, minute=0, hour=0))

coinInfo["Date"] = pd.to_datetime(coinInfo["Date"], utc=True).apply(lambda t: t.replace(second=0, minute=0, hour=0))
print(coinInfo["Date"].head())
print(filteredPosts["date"].head())

merged = filteredPosts.merge(coinInfo, how='left', left_on='date', right_on='Date')

merged = append_columns(merged, coinInfo)
merged.to_csv('../data/mergedData.csv')





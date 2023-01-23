#All the data from ERCOT comes in an xls file that has a number of columns: date, hour, quarter. to make this into a datetime we use transform_to_timestamp
from datetime import datetime, timedelta

def transform_to_timestamp(date_str, hour, quarter, price):
    date = datetime.strptime(date_str, '%m/%d/%Y')
    hour = hour-1
    quarter_minutes = (quarter - 1) * 15
    timestamp = datetime(date.year, date.month, date.day, hour, quarter_minutes)
    return timestamp, price

import pandas as pd
data = pd.read_csv('ERCOTSummerPrices.csv')
i = 0
timestamps = []
prices = []
while i < len(data):
    date = data.iloc[i]['Date']
    hour = data.iloc[i]['Hour']
    quarter = data.iloc[i]['Quarter']
    price = data.iloc[i]['ERCOT Hub Average Price']
    timestamp , price = transform_to_timestamp(date, hour, quarter, price)
    timestamps.append(timestamp)
    prices.append(price)
    i+=1

df = pd.DataFrame()
df['timestamp'] = timestamps
price_floats = []

#The prices are all strings. And to add insult to injury, prices > $1,000 have a comma in them. So we'll go ahead and fix it...
for string in prices:
    try:
        price_floats.append(float(string))
    except ValueError:
        # remove the comma and convert to float
        price_floats.append(float(string.replace(",", "")))

df['price'] = price_floats

#now, for ease of using data, I will just resample my pricing data to get the average hourly ERCOT hub price.
hourly_df = df.resample('60T', on='timestamp').mean()


#Google's BigData queery pulls Blocktimes in UTC. For this exercize, I want everything in CST (Texas Time). Convert_to_central_time does this for me
def convert_to_central_time(timestamp):
    timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f UTC')
    central_timezone_offset = timedelta(hours=-5)
    return timestamp + central_timezone_offset

blocktimes = pd.read_csv('Blocktimes.csv')
blocktimes['timestamp'] = blocktimes['timestamp'].apply(convert_to_central_time)

blocktimes.set_index('timestamp', inplace=True)

# resample blocktimes to hourly frequency
blocktimes_hourly = blocktimes.resample('H').count()

# merge blocktimes and prices dataframes
merged_df = blocktimes_hourly.merge(hourly_df, on='timestamp')

# group by price and find number of blocks found per hour
merged_df.groupby(['price']).count()

#Over the summer, how many blocks were found per hour? At all prices. The number should be pretty darn close to 6.00000
print('Average number of blocks found per hour this summer:')
print(merged_df['number'].sum() / merged_df['number'].count())


# filter for prices less than 200
less_200 = merged_df.query('price < 200')
print("Number of blocks found per hour when price < 200:")
print(less_200['number'].sum() / less_200['number'].count())

# filter for prices greater than or equal to 200
greater_eq_200 = merged_df.query('price >= 200')
print("Number of blocks found per hour when price >= 200:")
print(greater_eq_200['number'].sum() / greater_eq_200['number'].count())

print('Estimated amount of ERCOT-linked hashrate')
print(1-((greater_eq_200['number'].sum() / greater_eq_200['number'].count()) / (less_200['number'].sum() / less_200['number'].count())))


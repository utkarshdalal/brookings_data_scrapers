import datetime
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import os
import sys


# enter start and end timestamps in YYYY-MM-DD HH:MM:SS format
def write_moving_averages(start_timestamp, end_timestamp):
    host = os.environ['HOST']
    port = int(os.environ['PORT'])
    dbname = os.environ['DB']
    user = os.environ['USER']
    password = os.environ['PASSWORD']

    print("Writing Moving Averages")

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}')
    connection = engine.connect()

    start_datetime = datetime.datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S')
    end_datetime = datetime.datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S')

    range_start_time = (start_datetime - datetime.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S")
    range_end_time = (end_datetime + datetime.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S")

    x = pd.read_sql(f'select 5_min_rounded_timestamp, avg(thermal_generation_corrected) as thermal_generation, \
                    avg(gas_generation_corrected) as gas_generation, \
                    avg(hydro_generation_corrected) as hydro_generation, \
                    avg(nuclear_generation_corrected) as nuclear_generation, \
                    avg(renewable_generation_corrected) as renewable_generation, avg(demand_met) as demand_met \
                    from merit_india_data_rounded_corrected where \
                    timestamp >= "{range_start_time}" and timestamp <= "{range_end_time}" \
                    group by 5_min_rounded_timestamp', engine)
    x['5_min_rounded_timestamp'] = pd.to_datetime(x['5_min_rounded_timestamp'])

    timestamp_index_df = x.set_index('5_min_rounded_timestamp')

    daily_moving_averages = timestamp_index_df.rolling('24h').mean()
    daily_moving_averages[daily_moving_averages.index >= start_datetime]\
        .to_sql(name='merit_india_daily_moving_averages_temp', con=engine, index_label='timestamp',
                if_exists='replace')
    connection.execute('REPLACE INTO merit_india_daily_moving_averages '
                       'select * from merit_india_daily_moving_averages_temp')

    weekly_moving_averages = timestamp_index_df.rolling('7d').mean()
    weekly_moving_averages[weekly_moving_averages.index >= start_datetime]\
        .to_sql(name='merit_india_weekly_moving_averages_temp', con=engine, index_label='timestamp',
                if_exists='replace')
    connection.execute('REPLACE INTO merit_india_weekly_moving_averages '
                       'select * from merit_india_weekly_moving_averages_temp')

    monthly_moving_averages = timestamp_index_df.rolling('30d').mean()
    monthly_moving_averages[monthly_moving_averages.index >= start_datetime]\
        .to_sql(name='merit_india_monthly_moving_averages_temp', con=engine, index_label='timestamp',
                if_exists='replace')
    connection.execute('REPLACE INTO merit_india_monthly_moving_averages '
                       'select * from merit_india_monthly_moving_averages_temp')

    connection.close()


if __name__ == "__main__":
    write_moving_averages(sys.argv[1], sys.argv[2])

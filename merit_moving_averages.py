import datetime
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import os


def write_moving_averages():
    host = os.environ['HOST']
    port = int(os.environ['PORT'])
    dbname = os.environ['DB']
    user = os.environ['USER']
    password = os.environ['PASSWORD']
    hours = int(os.environ['HOURS_BETWEEN_RUNS'])

    print("Writing Moving Averages")

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}')
    connection = engine.connect()

    month_ago_time = (datetime.datetime.utcnow() - datetime.timedelta(days=31)).strftime("%Y-%m-%d %H:%M:%S")
    day_ago_time = (datetime.datetime.utcnow() - datetime.timedelta(hours=hours))

    x = pd.read_sql(f'select 5_min_rounded_timestamp, avg(thermal_generation_corrected) as thermal_generation, \
                    avg(gas_generation_corrected) as gas_generation, \
                    avg(hydro_generation_corrected) as hydro_generation, \
                    avg(nuclear_generation_corrected) as nuclear_generation, \
                    avg(renewable_generation_corrected) as renewable_generation, avg(demand_met) as demand_met \
                    from merit_india_data_rounded_corrected where \
                    timestamp >= "{month_ago_time}" \
                    group by 5_min_rounded_timestamp', engine)
    x['5_min_rounded_timestamp'] = pd.to_datetime(x['5_min_rounded_timestamp'])

    timestamp_index_df = x.set_index('5_min_rounded_timestamp')

    daily_moving_averages = timestamp_index_df.rolling('24h').mean()
    daily_moving_averages[daily_moving_averages.index >= day_ago_time]\
        .to_sql(name='merit_india_daily_moving_averages_temp', con=engine, index_label='timestamp',
                if_exists='replace')
    connection.execute('INSERT IGNORE INTO merit_india_daily_moving_averages '
                       'select * from merit_india_daily_moving_averages_temp')

    weekly_moving_averages = timestamp_index_df.rolling('7d').mean()
    weekly_moving_averages[weekly_moving_averages.index >= day_ago_time]\
        .to_sql(name='merit_india_weekly_moving_averages_temp', con=engine, index_label='timestamp',
                if_exists='replace')
    connection.execute('INSERT IGNORE INTO merit_india_weekly_moving_averages '
                       'select * from merit_india_weekly_moving_averages_temp')

    monthly_moving_averages = timestamp_index_df.rolling('30d').mean()
    monthly_moving_averages[monthly_moving_averages.index >= day_ago_time]\
        .to_sql(name='merit_india_monthly_moving_averages_temp', con=engine, index_label='timestamp',
                if_exists='replace')
    connection.execute('INSERT IGNORE INTO merit_india_monthly_moving_averages '
                       'select * from merit_india_monthly_moving_averages_temp')

    connection.close()


def run():
    write_moving_averages()


def lambda_handler(event, context):
    run()


if __name__ == "__main__":
    run()

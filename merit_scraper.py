import urllib.request
from bs4 import BeautifulSoup
import csv
from datetime import datetime, timedelta
import os
import logging
import pymysql
import helper_methods
import requests


def get_merit_data(conn):
    meritindia_url = 'http://www.meritindia.in'
    current_datetime = datetime.utcnow().replace(microsecond=0).isoformat()
    #page = urllib.request.urlopen(meritindia_url)

    #html_content = BeautifulSoup(page, 'html.parser')
    
    page = requests.get('https://meritindia.in')
    html_content = BeautifulSoup(page.content, 'html5lib')

    column_headings = ['TIMESTAMP']
    row_values = [current_datetime]

    print(f'Running write-to-aws.py at {current_datetime}')

    # Get data headers from website
    data_types = html_content.find_all('div', 'gen_title_sec')
    for data_type in data_types:
        column_headings.append(str(data_type.text.strip()))

    # Get current data values from website
    current_values = html_content.find_all('div', 'gen_value_sec')
    for current_value in current_values:
        data_value = (current_value.find('span', 'counter'))
        row_values.append(data_value.text.strip().replace(',', ''))

    print('Writing data to rds')

    try:
        cursor = conn.cursor()
        cursor2 = conn.cursor()
        data_dict = {}
        demand_met = float(row_values[1])
        thermal_generation = float(row_values[2])
        gas_generation = float(row_values[3])
        nuclear_generation = float(row_values[4])
        hydro_generation = float(row_values[5])
        renewable_generation = float(row_values[6])
        total_generation = thermal_generation + gas_generation + nuclear_generation + hydro_generation + renewable_generation
        utc_timestamp = datetime.strptime(current_datetime, "%Y-%m-%dT%H:%M:%S")
        rounded_timestamp_15 = utc_timestamp - timedelta(minutes=utc_timestamp.minute % 15,
                                                         seconds=utc_timestamp.second,
                                                         microseconds=utc_timestamp.microsecond)
        rounded_timestamp_5 = utc_timestamp - timedelta(minutes=utc_timestamp.minute % 5,
                                                        seconds=utc_timestamp.second,
                                                        microseconds=utc_timestamp.microsecond)
        data_dict['demand_met'] = demand_met
        data_dict['thermal_generation'] = thermal_generation
        data_dict['gas_generation'] = gas_generation
        data_dict['nuclear_generation'] = nuclear_generation
        data_dict['hydro_generation'] = hydro_generation
        data_dict['renewable_generation'] = renewable_generation
        data_dict['thermal_generation_corrected'] = thermal_generation
        data_dict['gas_generation_corrected'] = gas_generation
        data_dict['nuclear_generation_corrected'] = nuclear_generation
        data_dict['hydro_generation_corrected'] = hydro_generation
        data_dict['renewable_generation_corrected'] = renewable_generation
        data_dict['timestamp'] = utc_timestamp
        data_dict['5_min_rounded_timestamp'] = rounded_timestamp_5
        data_dict['15_min_rounded_timestamp'] = rounded_timestamp_15
        data_dict = calculate_corrected_data(cursor2, data_dict, demand_met, gas_generation, hydro_generation,
                                             nuclear_generation, renewable_generation, thermal_generation,
                                             total_generation)

        helper_methods.insert_into_table('merit_india_data_rounded_corrected', data_dict, cursor, conn)

    except Exception as e:
        print(f'Could not write data to rds! {str(e)}')
    finally:
        cursor.close()
        conn.close()

    print('Finished writing data to rds')


def calculate_corrected_data(cursor2, data_dict, demand_met, gas_generation, hydro_generation, nuclear_generation,
                             renewable_generation, thermal_generation, total_generation):
    if total_generation <= 0.95 * demand_met or total_generation >= 1.05 * demand_met:
        cursor2.execute(
            f'select timestamp, thermal_generation_corrected, gas_generation_corrected, hydro_generation_corrected, '
            f'nuclear_generation_corrected, renewable_generation_corrected, demand_met from '
            f'merit_india_data_rounded_corrected where timestamp < "{current_datetime}" '
            f'order by timestamp desc limit 1')
        for r2 in cursor2:
            previous_thermal = r2[1]
            previous_gas = r2[2]
            previous_hydro = r2[3]
            previous_nuclear = r2[4]
            previous_renewable = r2[5]
            previous_demand_met = r2[6]

            current_thermal = thermal_generation
            current_gas = gas_generation
            current_hydro = hydro_generation
            current_nuclear = nuclear_generation
            current_renewable = renewable_generation
            current_demand_met = demand_met

            corrected_thermal = current_thermal
            previous_thermal_ratio = previous_thermal / previous_demand_met
            current_thermal_ratio = current_thermal / current_demand_met
            if abs((current_thermal - previous_thermal) / previous_thermal) >= 0.1:
                corrected_thermal *= previous_thermal_ratio / current_thermal_ratio

            corrected_gas = current_gas
            previous_gas_ratio = previous_gas / previous_demand_met
            current_gas_ratio = current_gas / current_demand_met
            if abs((current_gas - previous_gas) / previous_gas) >= 0.1:
                corrected_gas *= previous_gas_ratio / current_gas_ratio

            corrected_hydro = current_hydro
            previous_hydro_ratio = previous_hydro / previous_demand_met
            current_hydro_ratio = current_hydro / current_demand_met
            if abs((current_hydro - previous_hydro) / previous_hydro) >= 0.1:
                corrected_hydro *= previous_hydro_ratio / current_hydro_ratio

            corrected_nuclear = current_nuclear
            previous_nuclear_ratio = previous_nuclear / previous_demand_met
            current_nuclear_ratio = current_nuclear / current_demand_met
            if abs((current_nuclear - previous_nuclear) / previous_nuclear) >= 0.1:
                corrected_nuclear *= previous_nuclear_ratio / current_nuclear_ratio

            corrected_renewable = current_renewable
            previous_renewable_ratio = previous_renewable / previous_demand_met
            current_renewable_ratio = current_renewable / current_demand_met
            if abs((current_renewable - previous_renewable) / previous_renewable) >= 0.1:
                corrected_renewable *= previous_renewable_ratio / current_renewable_ratio

            data_dict['thermal_generation_corrected'] = corrected_thermal
            data_dict['gas_generation_corrected'] = corrected_gas
            data_dict['nuclear_generation_corrected'] = corrected_nuclear
            data_dict['hydro_generation_corrected'] = corrected_hydro
            data_dict['renewable_generation_corrected'] = corrected_renewable
    return data_dict


def run():
    host = os.environ['HOST']
    port = int(os.environ['PORT'])
    dbname = os.environ['DB']
    user = os.environ['USER']
    password = os.environ['PASSWORD']
    conn = pymysql.connect(host=host, user=user, port=port,
                           passwd=password, db=dbname)
    get_merit_data(conn)


def lambda_handler(event, context):
    run()


if __name__ == "__main__":
    run()

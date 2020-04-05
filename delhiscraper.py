from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup
import time
from csv import writer
import os
import pymysql
import helper_methods


def currentValues():
    page_url = 'http://www.delhisldc.org/Redirect.aspx'
    r = uReq(page_url)
    soup = BeautifulSoup(r.read(), "html.parser")

    data_dict = {}
    
    current_revision = helper_methods.read_datetime_from_span_id(soup, "DynamicData1_LblDate",
                                                                 'DD-MMM-YYYY hh:mm:ss A')
    data_dict['timestamp'] = current_revision.datetime
    delhi_load = helper_methods.read_value_from_span_id(soup, "DynamicData1_LblLoad")
    data_dict['delhi_load'] = delhi_load
    schedule = helper_methods.read_value_from_span_id(soup, "DynamicData1_LblCurrScheduledAllocation")
    data_dict['schedule'] = schedule
    drawl = helper_methods.read_value_from_span_id(soup, "DynamicData1_LblCurrDrawal")
    data_dict['drawl'] = drawl
    delhi_gen = helper_methods.read_value_from_span_id(soup, "DynamicData1_LblCurrGen")
    data_dict['delhi_generation'] = delhi_gen
    freq = helper_methods.read_value_from_span_id(soup, 'DynamicData1_LblFrequency')
    data_dict['frequency_hz'] = freq
    
    return data_dict


def append_data(conn):
    cursor = conn.cursor()
    try:
        delhi_values = currentValues()
        helper_methods.insert_into_table('delhi_data', delhi_values, cursor, conn)
    except Exception as e:
        print(f'Could not fetch delhi data: {e}')


def run():
    host = os.environ['HOST']
    port = int(os.environ['PORT'])
    dbname = os.environ['DB']
    user = os.environ['USER']
    password = os.environ['PASSWORD']
    conn = pymysql.connect(host, user=user, port=port,
                           passwd=password, db=dbname)
    append_data(conn)


def lambda_handler(event, context):
    run()


if __name__ == "__main__":
    run()


#Span IDS - 
#Delhi Load = ContentPlaceHolder3_LblLoad
#Schedule = ContentPlaceHolder3_LblCurrScheduledAllocation
#Drawl = ContentPlaceHolder3_LblCurrDrawal
#Current Revision = ContentPlaceHolder3_LblDate
#Delhi Generation = ContentPlaceHolder3_LblCurrGen
#Max Load = ContentPlaceHolder3_LblMaxToday
#Min Load = ContentPlaceHolder3_LblMinToday
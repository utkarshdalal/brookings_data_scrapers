#!/usr/bin/env python3

# Code generously borrowed from https://github.com/hectorespert/electricitymap/blob/master/parsers/IN_KA.py

from requests import Session
import helper_methods
import pymysql
import os


def fetch_production_by_generator(session):
    html = helper_methods.get_response_soup('http://kptclsldc.com/StateGen.aspx', session)

    india_date_time = helper_methods.read_datetime_from_span_id(html, 'lbldate', 'M/D/YYYY h:mm:ss A')

    generation = {}

    generation['timestamp'] = india_date_time.datetime

    # RTPS Production: https://en.wikipedia.org/wiki/Raichur_Thermal_Power_Station
    rtps_value = helper_methods.read_value_from_span_id(html, 'lblrtptot')
    generation['rtps_generation'] = rtps_value

    # BTPS Production: https://en.wikipedia.org/wiki/Bellary_Thermal_Power_station
    btps_value = helper_methods.read_value_from_span_id(html, 'lblbtptot')
    generation['btps_generation'] = btps_value

    # YTPS Production: https://en.wikipedia.org/wiki/Yermarus_Thermal_Power_Station
    ytps_value = helper_methods.read_value_from_span_id(html, 'ytptot')
    generation['ytps_generation'] = ytps_value

    # UPCL Production: https://en.wikipedia.org/wiki/Udupi_Power_Plant
    upcl_value = helper_methods.read_value_from_span_id(html, 'lblupctot')
    generation['upcl_generation'] = upcl_value

    # Jhelper_methodsDAl Production: https://en.wikipedia.org/wiki/JSW_Vijayanagar_Power_Station
    jindal_value = helper_methods.read_value_from_span_id(html, 'lbljintot')
    generation['jindal_generation'] = jindal_value

    # Coal Production
    coal_value = rtps_value + btps_value + ytps_value + upcl_value + jindal_value
    generation['thermal_generation'] = coal_value

    # Sharavati Production: Sharavati  Hydroelectric
    sharavati_value = helper_methods.read_value_from_span_id(html, 'lblshvytot')
    generation['sharavati_generation'] = sharavati_value

    # Nagjhari Production: Kalinadi-Nagjhari Hydroelectric
    nagjhari_value = helper_methods.read_value_from_span_id(html, 'lblngjtot')
    generation['nagjhari_generation'] = nagjhari_value

    # Varahi Production: https://en.wikipedia.org/wiki/Varahi_River#Varahi_Hydro-electric_Project
    varahi_value = helper_methods.read_value_from_span_id(html, 'lblvrhtot')
    generation['varahi_generation'] = varahi_value

    # Kodsalli Production: Kalinadi Kodasalli Hydroelectric
    kodsalli_value = helper_methods.read_value_from_span_id(html, 'lblkdsltot')
    generation['kodsalli_generation'] = kodsalli_value

    # Kadra Production: https://en.wikipedia.org/wiki/Kadra_Dam
    kadra_value = helper_methods.read_value_from_span_id(html, 'lblkdrtot')
    generation['kadra_generation'] = kadra_value

    # GERUSOPPA production: Gerusoppa Dam
    gerusoppa_value = helper_methods.read_value_from_span_id(html, 'lblgrsptot')
    generation['gerusoppa_generation'] = gerusoppa_value

    # JOG production: https://en.wikipedia.org/wiki/Jog_Falls
    jog_value = helper_methods.read_value_from_span_id(html, 'lbljogtot')
    generation['jog_generation'] = jog_value

    # LPH Production: Linganamakki Dam
    lph_value = helper_methods.read_value_from_span_id(html, 'lbllphtot')
    generation['lph_generation'] = lph_value

    # Supa generation: https://en.wikipedia.org/wiki/Supa_Dam
    supa_value = helper_methods.read_value_from_span_id(html, 'lblsupatot')
    generation['supa_generation'] = supa_value

    # SHIMSHA: https://en.wikipedia.org/wiki/Shimsha#Power_generation
    shimsha_value = helper_methods.read_value_from_span_id(html, 'lblshimtot')
    generation['shimsha_generation'] = shimsha_value

    # SHIVASAMUDRA: https://en.wikipedia.org/wiki/Shivanasamudra_Falls#Power_generation
    shivasamudra_value = helper_methods.read_value_from_span_id(html, 'lblshivtot')
    generation['shivasamudra_generation'] = shivasamudra_value

    # MANIDAM: Mani Dam Hydroelectric
    manidam_value = helper_methods.read_value_from_span_id(html, 'lblmanitot')
    generation['manidam_generation'] = manidam_value

    # MUNRABAD: Munirabad Hydroelectric
    munrabad_value = helper_methods.read_value_from_span_id(html, 'lblmbdtot')
    generation['munrabad_generation'] = munrabad_value

    # BHADRA: https://en.wikipedia.org/wiki/Bhadra_Dam
    bhadra_value = helper_methods.read_value_from_span_id(html, 'lblbdratot')
    generation['bhadra_generation'] = bhadra_value

    # GHATAPRABHA: Ghataprabha Hydroelectric
    ghataprabha_value = helper_methods.read_value_from_span_id(html, 'lblgtprtot')
    generation['ghataprabha_generation'] = ghataprabha_value

    # ALMATTI: https://en.wikipedia.org/wiki/Almatti_Dam
    almatti_value = helper_methods.read_value_from_span_id(html, 'lblalmttot')
    generation['almatti_generation'] = almatti_value

    # CGS (Central Generating Stations) Production
    # TODO: Search CGS production type
    cgs_value = helper_methods.read_value_from_span_id(html, 'lblcgs')
    generation['cgs_generation'] = cgs_value

    ncep_value = helper_methods.read_value_from_span_id(html, 'lblncep')
    generation['ncep_generation'] = ncep_value

    total_value = helper_methods.read_value_from_span_id(html, 'lbltotgen')
    generation['total_generation'] = total_value

    frequency_value = helper_methods.read_value_from_span_id(html, 'lblfreq')
    generation['frequency_hz'] = frequency_value

    # Hydro production
    hydro_value = sharavati_value + nagjhari_value + varahi_value + kodsalli_value \
                  + kadra_value + gerusoppa_value + jog_value + lph_value + supa_value \
                  + shimsha_value + shivasamudra_value + manidam_value + munrabad_value \
                  + bhadra_value + ghataprabha_value + almatti_value
    generation['hydro_generation'] = hydro_value

    return generation


def fetch_ncep_production(session):
    ncep_generation = {}

    # NCEP (Non-Conventional Energy Production)
    ncep_html = helper_methods.get_response_soup('http://kptclsldc.com/StateNCEP.aspx', session)
    ncep_date_time = helper_methods.read_datetime_from_span_id(ncep_html, 'Label1', 'DD/MM/YYYY HH:mm:ss')

    ncep_generation['timestamp'] = ncep_date_time.datetime

    # cogen type is sugarcane bagasee. Proof in Issue #1867
    cogen_value = helper_methods.read_value_from_span_id(ncep_html, 'lbl_tc')
    ncep_generation['cogen_generation'] = cogen_value

    biomass_value = helper_methods.read_value_from_span_id(ncep_html, 'lbl_tb')
    ncep_generation['biomass_generation'] = biomass_value

    # cogen_value is generated from sugarcane bagasse
    biomass_value += cogen_value

    mini_hydro_value = helper_methods.read_value_from_span_id(ncep_html, 'lbl_tm')
    ncep_generation['mini_hydro_generation'] = mini_hydro_value

    wind_value = helper_methods.read_value_from_span_id(ncep_html, 'lbl_tw')
    ncep_generation['wind_generation'] = wind_value

    solar_value = helper_methods.read_value_from_span_id(ncep_html, 'lbl_ts')
    ncep_generation['solar_generation'] = solar_value

    return ncep_generation


def fetch_escom_demand(session):
    escom_demand = {}

    # ESCOM Scheduled & Actual
    escom_html = helper_methods.get_response_soup('http://kptclsldc.com/Snapshot.aspx', session)
    escom_date_time = helper_methods.read_datetime_from_span_id(escom_html, 'Label6', 'DD/MM/YYYY HH:mm:ss')

    escom_demand['timestamp'] = escom_date_time.datetime

    scheduled_bescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label15')
    actual_bescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label10')
    escom_demand['scheduled_bescom_load'] = scheduled_bescom_load
    escom_demand['actual_bescom_load'] = actual_bescom_load

    scheduled_mescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label16')
    actual_mescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label11')
    escom_demand['scheduled_mescom_load'] = scheduled_mescom_load
    escom_demand['actual_mescom_load'] = actual_mescom_load

    scheduled_cesc_load = helper_methods.read_value_from_span_id(escom_html, 'Label17')
    actual_cesc_load = helper_methods.read_value_from_span_id(escom_html, 'Label12')
    escom_demand['scheduled_cesc_load'] = scheduled_cesc_load
    escom_demand['actual_cesc_load'] = actual_cesc_load

    scheduled_gescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label18')
    actual_gescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label13')
    escom_demand['scheduled_gescom_load'] = scheduled_gescom_load
    escom_demand['actual_gescom_load'] = actual_gescom_load

    scheduled_hescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label19')
    actual_hescom_load = helper_methods.read_value_from_span_id(escom_html, 'Label14')
    escom_demand['scheduled_hescom_load'] = scheduled_hescom_load
    escom_demand['actual_hescom_load'] = actual_hescom_load

    scheduled_total_load = helper_methods.read_value_from_span_id(escom_html, 'Label25')
    actual_total_load = helper_methods.read_value_from_span_id(escom_html, 'Label26')
    escom_demand['scheduled_total_load'] = scheduled_total_load
    escom_demand['actual_total_load'] = actual_total_load

    frequency_value = helper_methods.read_value_from_span_id(escom_html, 'Label1')
    escom_demand['frequency_hz'] = frequency_value

    return escom_demand


def fetch_data(session=None, conn=None, target_datetime=None, logger=None):
    cursor = conn.cursor()

    try:
        generation = fetch_production_by_generator(session)
        helper_methods.insert_into_table('kptcl_generation', generation, cursor, conn)
    except Exception as e:
        print(f'Could not fetch kptcl generation data: {e}')

    try:
        ncep_generation = fetch_ncep_production(session)
        helper_methods.insert_into_table('kptcl_ncep_generation', ncep_generation, cursor, conn)
    except Exception as e:
        print(f'Could not fetch kptcl ncep generation data: {e}')

    try:
        escom_demand = fetch_escom_demand(session)
        helper_methods.insert_into_table('kptcl_load', escom_demand, cursor, conn)
    except Exception as e:
        print(f'Could not fetch kptcl load data: {e}')


def run():
    session = Session()
    host = os.environ['HOST']
    port = int(os.environ['PORT'])
    dbname = os.environ['DB']
    user = os.environ['USER']
    password = os.environ['PASSWORD']
    conn = pymysql.connect(host, user=user, port=port,
                           passwd=password, db=dbname)
    fetch_data(session, conn)


def lambda_handler(event, context):
    run()


if __name__ == "__main__":
    run()

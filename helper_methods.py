from requests import Session
from bs4 import BeautifulSoup
from arrow import get, utcnow


def get_response_soup(url, session=None):
    """Get BeautifulSoup response"""
    ses = session or Session()
    response = ses.get(url)
    response_text = response.text
    return BeautifulSoup(response_text, 'html.parser')


def read_datetime_from_span_id(html, span_id, format):
    """Read date time from span with id"""
    date_time_span = html.find('span', {'id': span_id})
    india_date_time = date_time_span.text + ' Asia/Kolkata'
    return get(india_date_time, format + ' ZZZ')


def read_text_from_span_id(html, span_id):
    """Read text from span with id"""
    return html.find('span', {'id': span_id}).text


def read_value_from_span_id(html, span_id):
    """Read value from span with id"""
    html_span = read_text_from_span_id(html, span_id)
    return float(html_span)


def insert_into_table(table, dictionary, cursor, conn):
    placeholders = ', '.join(['%s'] * len(dictionary))
    columns = ', '.join(dictionary.keys())
    sql = "INSERT IGNORE INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
    # valid in Python 3
    cursor.execute(sql, list(dictionary.values()))
    conn.commit()

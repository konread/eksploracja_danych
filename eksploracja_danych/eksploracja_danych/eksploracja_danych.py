import pandas as pd
import re
import apache_log_parser
from pprint import pprint
import csv
import datetime as dt
from datetime import datetime
import arff

def identify_users_full(data):
    data_sorted = data.sort_values('host')

    i = 0
    second = 0
    host = dict()

    while i < len(data_sorted):
        hostname = data_sorted['host'][i]
        request_path = data_sorted['request_path'][i]

        if hostname in host:
            host[hostname]
        else:
            host[hostname] = []

        record = dict()
        
        record['http_version'] = data_sorted['http_version'][i]
        record['method'] = data_sorted['method'][i]
        record['request_path'] = data_sorted['request_path'][i]
        record['response_size'] = data_sorted['response_size'][i]
        record['status'] = data_sorted['status'][i]
        record['date'] = data_sorted['date'][i]
        record['time'] = data_sorted['time'][i]
        record['year'] = data_sorted['year'][i]
        record['month'] = data_sorted['month'][i]
        record['day'] = data_sorted['day'][i]
        record['hour'] = data_sorted['hour'][i]
        record['minute'] = data_sorted['minute'][i]
        record['second'] = data_sorted['second'][i]

        current = dt.datetime(data_sorted['year'][i], data_sorted['month'][i], data_sorted['day'][i], data_sorted['hour'][i], 
                              data_sorted['minute'][i], data_sorted['second'][i])
        i = i + 1
            
        if i < len(data_sorted):
            next = dt.datetime(data_sorted['year'][i], data_sorted['month'][i], data_sorted['day'][i], data_sorted['hour'][i], 
                               data_sorted['minute'][i], data_sorted['second'][i])

            second = (next - current).total_seconds()

            record['time_per_page'] = second
        else:
            record['time_per_page'] = 0

        host[hostname].append(record)

    return host

def identify_users(data):
    data_sorted = data.sort_values('host')

    i = 0
    host = dict()

    while i < len(data_sorted):
        hostname = data_sorted['host'][i]
        request_path = data_sorted['request_path'][i]

        if hostname in host:
            host[hostname].append(request_path)
        else:
            host[hostname] = []
            host[hostname].append(request_path)
        
        i = i + 1

    return host

def identify_page(data):
    data_sorted = data.sort_values('request_path')

    i = 0
    page = dict()
    total = len(data)

    while i < len(data_sorted):
        request_path = data_sorted['request_path'][i]

        if request_path in page:
            page[request_path] = {'count': page[request_path]['count'] + 1, 
                                  'percent': round(page[request_path]['count'] * 100 / total, 2), 
                                  'total': total}
        else:
            page[request_path] = {'count': 1, 
                                  'percent': round(1 * 100 / total, 2), 
                                  'total': total}
        
        i = i + 1

    return page

def selected_page(data):
    selected = [];

    for key in data:
        if data[key]['percent'] > 0.5: 
            selected.append(key)
    
    return selected

def read_log_file_csv(name_file):
     data = pd.read_csv(name_file, sep = ',', names=["host", "http_version", "method", "request_path", "response_size", "status", "date", "time", "year", "month", "day", "hour", "minute", "second"])

     return data

def read_log_file(name_file):
    log = []

    with open(name_file) as file:
        log = file.readlines()

    return log

def parse_log_file(name_file, data):
    line_parser = apache_log_parser.make_parser("%h %l %u %t \"%r\" %>s %b")

    data_parse = []

    with open(name_file, 'w', newline='') as file:
        writer = csv.writer(file)
       
        #...

        #writer.writerow(["host", "http_version", "method", "request_path", "response_size", "status", "date", "time", "year", "month", "day", "hour", "minute", "second"])

        for record in data:
            record_parse = line_parser(record)

            if (record_parse['request_method'] == 'GET' and 
                record_parse['status'] == '200' and 
                record_parse['request_url_path'].lower().find('jpg') < 0 and 
                record_parse['request_url_path'].lower().find('gif') < 0 and 
                record_parse['request_url_path'].lower().find('png') < 0 and
                record_parse['request_url_path'].lower().find('bmp') < 0 and
                record_parse['request_url_path'].lower().find('mpg') < 0 and
                record_parse['request_url_path'].lower().find('xmb') < 0 and 
                record_parse['request_url_path'].lower().find('jpeg') < 0 and
                record_parse['request_url_path'].lower().find('xbm') < 0):

                    record_parse_data = []

                    record_parse_data.append(record_parse['remote_host'])
                    record_parse_data.append(record_parse['request_http_ver'])
                    record_parse_data.append(record_parse['request_method'])
                    record_parse_data.append(record_parse['request_url_path'])
                    record_parse_data.append(record_parse['response_bytes_clf'])
                    record_parse_data.append(record_parse['status'])
        
                    date_time = datetime.strptime(record_parse['time_received_isoformat'], '%Y-%m-%dT%H:%M:%S')

                    record_parse_data.append(str(date_time.date()))
                    record_parse_data.append(str(date_time.time()))

                    record_parse_data.append(str(date_time.year))
                    record_parse_data.append(str(date_time.month))
                    record_parse_data.append(str(date_time.day))
                    record_parse_data.append(str(date_time.hour))
                    record_parse_data.append(str(date_time.minute))
                    record_parse_data.append(str(date_time.second))

                    data_parse.append(record_parse_data)

                    writer.writerow(record_parse_data)
                          
def session(data_host, pages):
    timeout = 10 * 60
    id = 0

    session = {}

    for key in data_host:
        second = 0
        i = 0

        id = id + 1

        data = data_host[key]
        lenght = len(data)

        session_detail = {}

        for page in pages:
            session_detail[page] = 'F'

        session_detail['id'] = id
        session_detail['session_time'] = 0
        session_detail['session_action'] = 0
        session_detail['time_per_page'] = 0

        while i < lenght:
            second += data[i]['time_per_page']

            if second > timeout:
                if id in session:
                    id = id + 1 

                session_detail['session_time'] = second
                session_detail['session_action'] = 1

                second = 0;   
            else:        
                session_detail['session_time'] = second
                session_detail['session_action'] += 1 
            
            if session_detail['session_action'] > 2:
                session_detail['id'] = id
            
                session_detail['time_per_page'] = round(session_detail['session_time'] / (session_detail['session_action'] - 1))

                key_session_detail = data[i]['request_path']

                if key_session_detail in session_detail:
                    session_detail[key_session_detail] = 'T'
            
                session[id] = session_detail

            i = i + 1

    return session

def write_session_csv(filename, data, selected):
    with open(filename, mode = 'w', newline='') as file:
    
        writer = csv.DictWriter(file, fieldnames = ['id', 'session_time', 'session_action', 'time_per_page'] + selected)

        writer.writeheader()
        
        for key in data:    
            writer.writerow(data[key])

def print_user(data):
    for key in data:
        print(key)

import collections

def print_page(data):
    sort = sorted(data.items(), key = lambda x: x[1]['count'], reverse = True)

    for key in sort:
        print(key)

def print_page(data):
    for item in data:
        print(item)

def main():
    #data_log = read_log_file('nasa.log')
    #parse_log_file('nasa_new.csv', data_log)
    data_csv = read_log_file_csv('nasa_new.csv')
    #data_host = identify_users(data_csv)
    data_host = identify_users_full(data_csv)
    

    pages = identify_page(data_csv)
    selected = selected_page(pages)

    #print(len(pages))
    #print(len(selected))
    #print(pages)

    sessions = session(data_host, selected)

    write_session_csv('session.csv', sessions, selected)
    #print_user(data_host)
    #print_page(pages)
    #print_page(selected)

if __name__ == "__main__":
    main()
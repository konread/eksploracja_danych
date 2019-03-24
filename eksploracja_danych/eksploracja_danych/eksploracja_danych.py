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

        if hostname not in host:
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

        host[hostname].append(record)
     
        i = i + 1

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
     data = pd.read_csv(name_file, sep = ',', names=["host", "http_version", "method", "request_path", 
                                                     "response_size", "status", "date", "time", "year", 
                                                     "month", "day", "hour", "minute", "second"])

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

def session(data_host):
    timeout = 10 * 60
    id = 0

    session = {}

    for key in data_host:
        i = 0
        id = id + 1
        data = data_host[key]

        first = dt.datetime(data[0]['year'], data[0]['month'], data[0]['day'], data[0]['hour'], data[0]['minute'], data[0]['second'])

        while i < len(data):
            last = dt.datetime(data[i]['year'], data[i]['month'], data[i]['day'], data[i]['hour'], data[i]['minute'], data[i]['second'])

            second = round((last - first).total_seconds())

            if second > timeout:
                first = last

                if id in session:
                    id = id + 1 
            
            if id not in session:
                session[id] = []

            session[id].append(data[i])

            i = i + 1

    return session

def session_remove_not_popular(data, pages):
    temp = {}

    for key in data:
        i = 0

        while i < len(data[key]):
            request_path = data[key][i]['request_path']
            
            if item_in_list(request_path, pages) == False:
                del data[key][i]

            i = i + 1
        
        if len(data[key]) > 1:
            temp[key] = data[key]

    return temp

def session_attribute(data, pages):
    session = {}

    for key in data:
        session_detail = {}

        for page in pages:
            session_detail[page] = 'F'

        session_detail['id'] = key

        value_first = data[key][0]
        value_last = data[key][len(data[key]) - 1]

        first = dt.datetime(value_first['year'], value_first['month'], value_first['day'], 
                            value_first['hour'], value_first['minute'], value_first['second'])

        last = dt.datetime(value_last['year'], value_last['month'], value_last['day'], 
                           value_last['hour'], value_last['minute'], value_last['second'])
           
        time = round((last - first).total_seconds())
        action = len(data[key])

        session_detail['session_time'] = time
        session_detail['session_action'] = action
        session_detail['time_per_page'] = round(time / action)

        i = 0

        while i < len(data[key]):
            key_session_detail = data[key][i]['request_path']

            if item_in_list(key_session_detail, pages):
                session_detail[key_session_detail] = 'T'

            i = i + 1

        session[key] = session_detail

    return session

def host_attribute(data, pages):
    host = {}

    for key in data:
        host_detail = {}

        for page in pages:
            host_detail[str(page)] = 'F'

        host_detail['host'] = key

        visited = len(data[key])

        host_detail['visited_pages'] = visited
        
        i = 0
        
        while i < len(data[key]):
            key_host_detail = data[key][i]['request_path']

            if item_in_list(key_host_detail, pages):
                host_detail[str(key_host_detail)] = 'T'

            i = i + 1

        host[key] = host_detail

    return host

def item_in_list(item, list):
    for x in list:
        if len(str(x)) == len(str(item)) and x in item:
            return True

    return False

def host_remove_not_popular(data, pages):
    temp = {}

    for key in data:
        i = 0

        while i < len(data[key]):
            request_path = data[key][i]['request_path']

            if item_in_list(request_path, pages) == False:
                del data[key][i]

            i = i + 1
        
        if len(data[key]) > 1:
            temp[key] = data[key]

    return temp

def write_host_csv(filename, data, selected):
    with open(filename, mode = 'w', newline='') as file:
    
        writer = csv.DictWriter(file, fieldnames = ['host', 'visited_pages'] + selected)

        writer.writeheader()
        
        for key in data:    
            writer.writerow(data[key])

def main():
    #data_log = read_log_file('nasa.log')
    #parse_log_file('nasa_new.csv', data_log)
    
    data_csv = read_log_file_csv('nasa_new.csv')
    
    hos = identify_users_full(data_csv)

    pages = identify_page(data_csv)
    popular = selected_page(pages)

    ses = session(hos)

    rem_ses = session_remove_not_popular(ses, popular)
    att_ses = session_attribute(rem_ses, popular)
    write_session_csv('session.csv', att_ses, popular)
    
    rem_hos = host_remove_not_popular(hos, popular)
    att_hos = host_attribute(rem_hos, popular)
    write_host_csv('host.csv', att_hos, popular)

if __name__ == "__main__":
    main()
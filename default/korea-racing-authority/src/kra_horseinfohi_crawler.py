from logging import raiseExceptions
from pyarrow import fs
import requests
import urllib3
import json
import datetime
import xml.etree.ElementTree as et
import argparse
import logging
logging.basicConfig(level=logging.INFO)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DataNotFoundError(Exception):    # Exception을 상속받아서 새로운 예외를 만듦
    def __init__(self):
        super().__init__('data is not exist.')

dt = datetime.datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument('--search-date', required=True)
parser.add_argument('--ignore-nodata-error', action='store_true')
args = parser.parse_args()

logging.info(f'{args.search_date} on request')
search_date_dt = datetime.datetime.strptime(args.search_date, '%Y-%m-%d')
url = 'https://apis.data.go.kr/B551015/horseinfohi/gethorseinfohi'
params ={'serviceKey' : 'RPpsWDlUO4T5OK+MUN9UXZT6wL7CiehEzUh8eT1Dw3wS9/FGnFvufSBm+DTyLDlbW/S37EBAJ8+dTVeFMvHFeA==', 
         'pageNo' : '1', 
         'numOfRows' : '1000000', 
        'reg_dt_fr': f'{search_date_dt.year}0101',
        'reg_dt_to': f'{search_date_dt.year}1231',
        '_type': 'json'
        }

response = requests.get(url, params=params, verify=False)
logging.info(f'url: {response.url}')
logging.info(f'status code: {response.status_code}')
response.raise_for_status()

resultcode = response.json()['response']['header']['resultCode']
if resultcode != '00':
    raise RuntimeError(f'result is not 00. result is {resultcode}') 

total_count = response.json()['response']['body']['totalCount']
if total_count == 0:
    if args.ignore_nodata_error:
        logging.warning('The data does not exist.')
    else:
        raise DataNotFoundError()
else:
    logging.info(f'Number of fetched data: {total_count}')
    data = response.json()['response']['body']['items']['item']
    group = []
    for element in data:
        group.append(json.dumps(element, ensure_ascii=False))

    file_path = f'/lake/red/korea_racing_authority/horse_info_hi/year={search_date_dt.year}.json.gz' 
    hdfs = fs.HadoopFileSystem('192.168.1.7', port=9000, user='root')
    with hdfs.open_output_stream(file_path, compression='gzip') as stream:
        stream.write('\n'.join(group).encode('utf-8'))

    logging.info(f'Data storage path: {file_path}')

print(search_date_dt.year)
# from logging import raiseExceptions
# from pyarrow import fs
# import requests
# import urllib3
# import json
# import datetime
# import xml.etree.ElementTree as et
# import argparse
# import logging
# logging.basicConfig(level=logging.INFO)


# class DataNotFoundError(Exception):    # Exception을 상속받아서 새로운 예외를 만듦
#     def __init__(self):
#         super().__init__('data is not exist.')

# dt = datetime.datetime.now()

# parser = argparse.ArgumentParser()
# parser.add_argument('--search-date', required=True)
# parser.add_argument('--ignore-nodata-error', action='store_true')
# args = parser.parse_args()

# logging.info(f'{args.search_date} on request')
# search_date_dt = datetime.datetime.strptime(args.search_date, '%Y-%m-%d')

# url = 'http://apis.data.go.kr/B551015/API214/RaceDetailResult'
# params ={'serviceKey' : 'RPpsWDlUO4T5OK+MUN9UXZT6wL7CiehEzUh8eT1Dw3wS9/FGnFvufSBm+DTyLDlbW/S37EBAJ8+dTVeFMvHFeA==', 
#          'pageNo' : '1', 
#          'numOfRows' : '1000000', 
#          'rc_month' : f'{search_date_dt.year}{search_date_dt.month:02d}'
#         }

# response = requests.get(url, params=params)
# logging.info(f'url: {response.url}')
# logging.info(f'status code: {response.status_code}')
# response.raise_for_status()
# content = response.text

# root = et.fromstring(content)
# header = root.find('header')
# resultcode = header.find('resultCode')

# if resultcode.text != '00':
#     raise RuntimeError(f'result is not 00. result is {resultcode.text}') 

# body = root.find('body')
# items = body.find('items')

# if len(items) == 0:
#     if args.ignore_nodata_error:
#         logging.warning('The data does not exist.')
#     else:
#         raise DataNotFoundError()
# else:
#     logging.info(f'Number of fetched data: {len(items)}')
#     group = []
#     for item in items.findall('item'):
#         json_dict = {}
#         for item_element in item:
#             tag = item_element.tag
#             text = item.find(tag).text
#             json_dict[tag] = text 
#         group.append(json.dumps(json_dict, ensure_ascii=False))

#     file_path = f'/lake/red/korea_racing_authority/race_detail_result/year={search_date_dt.year}/month={search_date_dt.month}.json.gz' 
#     hdfs = fs.HadoopFileSystem('192.168.1.7', port=9000, user='root')
#     with hdfs.open_output_stream(file_path, compression='gzip') as stream:
#         stream.write('\n'.join(group).encode('utf-8'))

#     logging.info(f'Data storage path: {file_path}')


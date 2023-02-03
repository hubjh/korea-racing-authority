# import datetime
# from dateutil.relativedelta import relativedelta
# import requests
# import urllib3
# import json
# import datetime
# from pyarrow import fs

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # 시작일,종료일 설정
# start = "1966-01-01"
# last = "2021-12-31"

# # 시작일, 종료일 datetime 으로 변환
# start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
# last_date = datetime.datetime.strptime(last, "%Y-%m-%d")

# # 종료일 까지 반복
# while start_date <= last_date:
#     year = start_date.strftime("%Y")
#     servicekey = 'RPpsWDlUO4T5OK+MUN9UXZT6wL7CiehEzUh8eT1Dw3wS9/FGnFvufSBm+DTyLDlbW/S37EBAJ8+dTVeFMvHFeA=='
#     data_type = 'json'

#     url = 'https://apis.data.go.kr/B551015/horseinfohi/gethorseinfohi'
#     response = requests.get(url, params={
#         'serviceKey': servicekey,
#         'pageNo': 1,
#         'numOfRows': 500000,
#         'reg_dt_fr': f'{year}0101',
#         'reg_dt_to': f'{year}1231',
#         '_type': data_type

#     }, verify=False)

#     response.raise_for_status()

#     total_count = response.json()['response']['body']['totalCount']
#     if total_count == 0:
#         print('The data does not exist.')
#     else:
#         data = response.json()['response']['body']['items']['item']

#         group = []
#         for element in data:
#             group.append(json.dumps(element, ensure_ascii=False))

#         # result = '\n'.join(group) #.encode('utf-8')

#         print('year = ', year)
#         print('totalCount = ',total_count)
        
#         file_path = f'/lake/red/korea_racing_authority/horse_info_hi/year={year}.json.gz' 
#         hdfs = fs.HadoopFileSystem('192.168.1.7', port=9000, user='root')
#         with hdfs.open_output_stream(file_path, compression='gzip') as stream:
#             stream.write('\n'.join(group).encode('utf-8'))

#     # 하루 더하기
#     start_date += relativedelta(years=1)
import requests
import pandas as pd 
import json
import concurrent.futures
import re
import queue
import os
from bs4 import BeautifulSoup

from collections import defaultdict
import unicodedata

import logging

logging.basicConfig(filename='tiki_logger.log',
    filemode='w',
    format='[%(filename)s:%(lineno)s - %(funcName)20s() ] [%(levelname)s] : [%(message)s]',
    datefmt='%H:%M:%S')

_category_path = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=100&include=advertisement&aggregations=1&category={}&page={}&urlKey={}"
_book_detail = 'https://tiki.vn/api/v2/products/{}?platform=web&spid={}'

payload={}
headers = {
  'authority': 'tiki.vn',
  'pragma': 'no-cache',
  'cache-control': 'no-cache',
  'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
  'accept': 'application/json, text/plain, */*',
  'sec-ch-ua-mobile': '?0',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-mode': 'cors',
  'sec-fetch-dest': 'empty',
  'accept-language': 'vi,en-US;q=0.9,en;q=0.8,vi-VN;q=0.7'
}

df2 = pd.read_csv("DATA_v2.csv")
df2.dropna(inplace = True) 
df2.columns = ["Thể loại", "Nguồn nhập", "type"]

# Remove all row that not have tiki
df2 = df2[df2['Nguồn nhập'].str.contains("tiki")]
group_f1 = df2.groupby("Thể loại")["Nguồn nhập"].apply(list).to_dict()
group_f2 = df2.groupby("type")["Thể loại"].apply(list).to_dict()

n_dict = defaultdict(dict)
for k, v in group_f2.items():
    for stype in v:
        if (len(group_f1[stype])) == 1:
            n_dict[k][stype] = group_f1[stype][0]
        else:
            for _ in group_f1[stype]:
                n_dict[k][stype] = _

_big_dict = {}

class Crawler():
    def __init__(self, categoryid, urlkey, cname):
        self.categoryid = categoryid
        self.urlkey = urlkey
        self.category_name = cname
        
    def crawl(self):
        _concurrent = self._bookshelf()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # executor.map(self._books, _concurrent)
            results = list(map(lambda x: executor.submit(self._books, x), _concurrent))
            for future in concurrent.futures.as_completed(results):
                logging.info('================================')
                try:
                    print('resutl is', future.result())
                    booksQueue.put(future.result())
                    logging.info('crawl done')
                except Exception as e:
                    print('e is', e, type(e))
                    logging.error(e, exc_info=True)
            
    def _books(self, book_tuple):
        
        book = {}
        
        __res =  requests.request("GET",_book_detail.format(book_tuple[0],book_tuple[1]), headers=headers).json()
        soup = BeautifulSoup(__res['description'] , "html.parser")

        book['path'] = __res['url_path']
        book['Tên Sách'] = __res['name']
        book['Ảnh bìa'] = __res['thumbnail_url']
        for _spec in __res['specifications'][0]['attributes']:
            _value = _spec['value']
            if '<p>' in _spec['value']:
                book[_spec['name']] = re.sub('<[^<]+?>', '', _value)
            else :
                book[_spec['name']] = _value
            
            _big_dict[_spec['name']] = ''
        if 'authors' in __res:
            book['Tác giả'] = ','.join([author['name'] for author in __res['authors']])
        else:
            book['Tác giả'] = ''
        book['Thể loại'] = self.category_name
        book['Giá bìa'] = __res['price']

        description = ''
        for _des in soup.findAll('span'):
            description = description + _des.text + '\n'
        book['Nội dung tóm tắt'] = description
        
        # booksQueue.put(book)
        return book
            
        print(f"Number of products in my pocket {booksQueue.qsize()}")
        
        
    def _bookshelf(self):
        
        _bookshelf = []
        _res = requests.request("GET", _category_path.format(self.categoryid, 1, self.urlkey), headers=headers).json()
        _total = _res['paging']['last_page']+1
        
        for _ in range(_total):
            _ +=1
            print(f"total page of {self.urlkey} : {_total}")
            _res = requests.request("GET", _category_path.format(self.categoryid, _, self.urlkey), headers=headers).json()
            for data in _res['data']:
                _bookshelf.append( (data['id'],data['seller_product_id']) )
        
        return _bookshelf


for _name in n_dict.keys():
    print(_name)
    booksQueue = queue.Queue()
    for k,v in n_dict[_name].items():
        _split = v.split("/")
        t = Crawler(_split[-1][1:],_split[-2], k)
        t.crawl()
    
    # df = pd.DataFrame(columns=['Tên Sách', 'Ảnh bìa','Thể loại','Tác giả','Nội dung tóm tắt','Giá bìa','Công ty phát hành','Nhà xuất bản','Ngày xuất bản','Kích thước',
    #                            'Loại bìa','Số trang','Dịch Giả',],index=[0])
    df = pd.DataFrame()
    while not booksQueue.empty():
        df = df.append(booksQueue.get(),ignore_index=True)

    df.to_csv('tiki/tiki_'+ _name+'.csv',index=False)


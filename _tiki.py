import requests
import pandas as pd 
import json
import concurrent.futures
import re
import queue
import os

from collections import defaultdict
import unicodedata

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
        n_dict[k][stype] = group_f1[stype]

_big_dict = {}

class Crawler():
    def __init__(self, categoryid, urlkey, cname):
        self.categoryid = categoryid
        self.urlkey = urlkey
        self.category_name = cname
        
    def crawl(self):
        _concurrent = self._bookshelf()
        print(_concurrent)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()*3) as executor:
            executor.map(self._books, _concurrent)
            
    def _books(self, book_tuple):
        
        book = {}
        
        __res =  requests.request("GET",_book_detail.format(book_tuple[0],book_tuple[1]), headers=headers).json()
        book['Tên Sách'] = __res['name']
        book['Ảnh bìa'] = __res['thumbnail_url']
        for _spec in __res['specifications'][0]['attributes']:
            book[_spec['name']] = _spec['value']
            
            _big_dict[_spec['name']] = ''
#         book['Nhà phát hành'] = __res['specifications'][0]['attributes'][0]["value"]
#         book['Nhà xuất bản'] = __res['specifications'][0]['attributes'][-1]["value"]
#         book['Dịch Giả'] = __res['specifications'][0]['attributes'][3]["value"]
#         book['Số trang'] = __res['specifications'][0]['attributes'][3]["value"]
#         book['Ngày xuất bản'] = __res['specifications'][0]['attributes'][1]["value"]
        
        book['Tác giả'] = ','.join([author['name'] for author in __res['authors']])
        book['Thể loại'] = self.category_name
        book['Giá bìa'] = __res['price']
#         book['Nội dung tóm tắt'] = __res['short_description']
        book['Nội dung tóm tắt'] = unicodedata.normalize("NFKD",__res['short_description'])
        
        booksQueue.put(book)
            
        print(f"Number of products in my pocket {booksQueue.qsize()}")
        
        
    def _bookshelf(self):
        
        _bookshelf = []
        _res = requests.request("GET", _category_path.format(self.categoryid, 0, self.urlkey), headers=headers).json()
        _total = _res['paging']['last_page']+1
        
        for _ in range(_total):
            print(f"total page of {self.urlkey} : {_total}")
            _res = requests.request("GET", _category_path.format(self.categoryid, _, self.urlkey), headers=headers).json()
            for data in _res['data']:
                _bookshelf.append( (data['id'],data['seller_product_id']) )
        
        return _bookshelf

for _name in n_dict.keys():
    booksQueue = queue.Queue()
    print(_name)
    for k,v in n_dict[_name].items():
        _split = v[0].split("/")
        t = Crawler(_split[-1][1:],_split[-2], k)
        t.crawl()
        
    df = pd.DataFrame(columns=['Tên Sách', 'Ảnh bìa','Thể loại','Tác giả','Nội dung tóm tắt','Giá bìa']+ list(_big_dict.keys()),index=[0])
    while not booksQueue.empty():
        df = df.append(booksQueue.get(),ignore_index=True)
                       
    df.to_csv('tiki_'+ _name+'.csv')
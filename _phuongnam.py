import requests
import pandas as pd 
import json
import concurrent.futures
import re
import queue
import os
from bs4 import BeautifulSoup
import lxml

from collections import defaultdict, deque
import unicodedata
import pickle

df2 = pd.read_csv("DATA_v2.csv")
df2.dropna(inplace = True) 
df2.columns = ["Thể loại", "Nguồn nhập", "type"]

# Remove all row that not have fahasa
df2 = df2[df2['Nguồn nhập'].str.contains("phuongnam")]
group_f1 = df2.groupby("Thể loại")["Nguồn nhập"].apply(list).to_dict()
group_f2 = df2.groupby("type")["Thể loại"].apply(list).to_dict()

n_dict = defaultdict(dict)
for k, v in group_f2.items():
    for stype in v:
        if (len(group_f1[stype])) == 1:
            _url = group_f1[stype][0].split(".html")[0]+"-page-{}.html?items_per_page=128&result_ids=pagination_contents&is_ajax=1"
            
            n_dict[k][stype] = _url
        else:
            for _ in group_f1[stype]:
                _url = _.split(".html")[0]+"-page-{}.html?items_per_page=128&result_ids=pagination_contents&is_ajax=1"
                n_dict[k][stype] = _url


headers = {
  'Connection': 'keep-alive',
  'Pragma': 'no-cache',
  'Cache-Control': 'no-cache',
  'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'X-Requested-With': 'XMLHttpRequest',
  'sec-ch-ua-mobile': '?0',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36',
  'sec-ch-ua-platform': '"Windows"',
  'Sec-Fetch-Site': 'same-origin',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Dest': 'empty',
  'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8,vi-VN;q=0.7',
  'Cookie': 'sid_customer_d577e=c81a4f54e12eb374c53db1cf4d78f94e-C'
}

_book = ['tên sách','ảnh bìa','thể loại','nội dung tóm tắt','giá bìa']
_big_dict = {}
booksQueue = queue.Queue()

class _phuongnam_Crawler():
    def __init__(self, url,category):
        self.url = url
        self.category = category
        
    def crawl(self):
        _concurrent = self._bookshelf()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()*5) as executor:
            executor.map(self._books, _concurrent)
    
    def _books(self, book_url):
        
        book = {}
        
        _res = requests.request("GET", book_url, headers=headers, verify=False)
        soup = BeautifulSoup(_res.text,'html.parser')
        
        book['tên sách'] = soup.find('h1',{'class':'ty-mainbox-title'}).text
        book['ảnh bìa'] = soup.find('a',{'class':'cm-image-previewer cm-previewer ty-previewer'})['href']
        book['thể loại'] = self.category
        book['nội dung tóm tắt'] = soup.find('div',{'id':'content_description'}).text
        
        features = soup.findAll('div',{'class':'ty-product-feature'})
        for _ in features[2:5]:
            _info = _.text.split(':')
            _big_dict[_info[0]] = ''
            
            book[_info[0]] = _info[1]
            book[_info[0]] = _info[1]
            book[_info[0]] = _info[1]
        book['giá bìa'] = soup.find('span',{'class':'ty-price-num'}).text
        
        # print(book)
        
        booksQueue.put(book)
            
        print(f"Number of products in my pocket {booksQueue.qsize()}")
        
    def _bookshelf(self):
        _bookshelf = []
        
        pages=0
        while True:
            print(f"Crawling pages {pages} of {self.category}")
            response = requests.request("GET", self.url.format(pages), headers=headers, verify=False).json()

            if 'html' in response.keys():
                html = BeautifulSoup(response['html']['pagination_contents'])
                book_html=  html.find('div',{'class':'grid-list vs-grid-table-wrapper et-grid-table-wrapper'})
                books = book_html.findAll('div',{'class':'vs-grid vs-grid-table et-grid'})

                for  _ in books:
                    _bookshelf.append(_.a['href'])
            else:
                break
                
            pages +=1
        return _bookshelf

total = 0 
rename_dict = defaultdict()

for _name in n_dict.keys():
	booksQueue = queue.Queue()
	for k,v in n_dict[_name].items():
		print(v)
		t = _phuongnam_Crawler(v, k)
		t.crawl()

	for i in sorted(_big_dict.keys()):
		rename_dict[i] = i.lstrip().lower()

	total += booksQueue.qsize()
	_dict = {}
	df = pd.DataFrame(columns=_book+list(rename_dict.keys()))

	while not booksQueue.empty():
		df = df.append(booksQueue.get(),ignore_index=True)
	# _dict = {**_dict, **booksQueue.get()}
	# _dict.update(booksQueue.get())

	print(_dict)
	# df = pd.DataFrame.from_dict(_dict.items(), columns= _dict.keys())
	df.rename(columns=rename_dict ,inplace=True)
	df.to_csv('phuongnam/phuongnam_'+ _name+'.csv',index=False)
print(f"total : {total}")
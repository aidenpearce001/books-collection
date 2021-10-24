import requests
import pandas as pd 
import json
import concurrent.futures
import re
import queue
import os
from bs4 import BeautifulSoup
import lxml

from collections import defaultdict
import unicodedata

__name = pd.read_csv("allocate/VĂN HỌC.csv")
__name.columns

df2 = pd.read_csv("DATA_v2.csv")
df2.dropna(inplace = True) 
df2.columns = ["Thể loại", "Nguồn nhập", "type"]

# Remove all row that not have fahasa
df2 = df2[df2['Nguồn nhập'].str.contains("vina")]
group_f1 = df2.groupby("Thể loại")["Nguồn nhập"].apply(list).to_dict()
group_f2 = df2.groupby("type")["Thể loại"].apply(list).to_dict()

n_dict = defaultdict(dict)
for k, v in group_f2.items():
    for stype in v:
        if type(group_f1[stype]) == list:
            _url = group_f1[stype][0]
        else:
            _url = group_f1[stype]
            
        n_dict[k][stype] = group_f1[stype][0] + 'page-{}/?sef_rewrite=1'


payload={}
headers = {
  'authority': 'www.vinabook.com',
  'pragma': 'no-cache',
  'cache-control': 'no-cache',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
  'sec-fetch-site': 'none',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-user': '?1',
  'sec-fetch-dest': 'document',
  'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'accept-language': 'vi,en-US;q=0.9,en;q=0.8,vi-VN;q=0.7',
}

booksQueue = queue.Queue()

class Crawler():
    def __init__(self, url, cname):
        self.url = url
        self.category_name = cname
        
    def crawl(self):
        _concurrent = self._bookshelf()
        if _concurrent != None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()*3) as executor:
                executor.map(self._books, _concurrent)
            
    def _books(self, _url,):
        
        book = {}
        __res =  requests.request("GET",_url, headers=headers)
        _soup = BeautifulSoup(__res.text, "html.parser")
        
        book['tên sách'] = _soup.h1.text
        book['ảnh bìa'] = _soup.find("div",{"class":"cm-image-wrap"}).img['src']
        book['thể loại'] = self.category_name
        
        _specific = _soup.find("div",{"class":"product-feature"})
        for _ in _specific.findAll("li"):
            _regex = re.sub('\s+',' ',_.text).split(":")
            book[_regex[0].lower()] = _regex[1]
            
        book['nội dung tóm tắt'] = _soup.find("div",{"class":"full-description"}).text.split("...")[0]
        book['giá bìa'] = _soup.find("span",{"class":"list-price nowrap"}).text
        
        print(book)
        booksQueue.put(book)
        print(f"Number of products in my pocket {booksQueue.qsize()}")
        
    def _bookshelf(self):
        
        _bookshelf = []
        print(self.url.format(48, 1))
        _res = requests.request("GET", self.url.format(1), headers=headers)
        soup = BeautifulSoup(_res.text, "html.parser")
        
        _paging = soup.find("span",{"class":"group-paging-label"})
        if _paging:
            _pages = int(_paging.text.split("/")[1]) + 1

            for _ in range(_pages):
                _ =+1
                _res = requests.request("GET", self.url.format(_), headers=headers)
                soup = BeautifulSoup(_res.text, "html.parser")
                for _ in soup.findAll("p",{"class":"price-info-nd"}):
                    _bookshelf.append(_.a['href'].rstrip())

            return _bookshelf
        else:
            return None

total = 0
for _name in n_dict.keys():
    booksQueue = queue.Queue()
    for k,v in n_dict[_name].items():
        t = Crawler(v, k)
        t.crawl()

    total += booksQueue.qsize()
    df = pd.DataFrame(columns=__name.columns,index=[0])
    while not booksQueue.empty():
        df = df.append(booksQueue.get(),ignore_index=True)

    df.to_csv('vina/vina_'+ _name+'.csv')
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

df2 = pd.read_csv("DATA_v2.csv")
df2.dropna(inplace = True) 
df2.columns = ["Thể loại", "Nguồn nhập", "type"]

# Remove all row that not have fahasa
df2 = pd.read_csv("DATA_v2.csv")
df2.dropna(inplace = True) 
df2.columns = ["Thể loại", "Nguồn nhập", "type"]

# Remove all row that not have fahasa
df2 = df2[df2['Nguồn nhập'].str.contains("fahasa")]
group_f1 = df2.groupby("Thể loại")["Nguồn nhập"].apply(list).to_dict()
group_f2 = df2.groupby("type")["Thể loại"].apply(list).to_dict()

n_dict = defaultdict(dict)
for k, v in group_f2.items():
    for stype in v:
        if type(group_f1[stype]) == list:
            _url = group_f1[stype][0]
        else:
            _url = group_f1[stype]
            
        if "order=num_orders" not in _url:
            print(group_f1[stype])
            group_f1[stype] = _url + '?order=num_orders&limit={}&p={}'
        else:
            group_f1[stype] = _url.split('?')[0] + '?order=num_orders&limit={}&p={}'
        n_dict[k][stype] = group_f1[stype]

payload={}
headers = {
  'authority': 'www.fahasa.com',
  'pragma': 'no-cache',
  'cache-control': 'no-cache',
  'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-user': '?1',
  'sec-fetch-dest': 'document',
#   'referer': 'https://www.fahasa.com/sach-trong-nuoc/nuoi-day-con/cam-nang-lam-cha-me.html?order=num_orders&limit=24&p=1',
  'accept-language': 'vi,en-US;q=0.9,en;q=0.8,vi-VN;q=0.7',
  'cookie': 'BPC2=291be300fef9c427de4cb5d4c311f24a; BPC2Referrer=;'
}

class Crawler():
    def __init__(self, url, cname):
        self.url = url
        self.category_name = cname
        
    def crawl(self):
        _concurrent = self._bookshelf()
        
        print(_concurrent)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()*5) as executor:
            executor.map(self._books, _concurrent)
            
    def _books(self, _url,):
        
        book = {}
        __res =  requests.request("GET",_url, headers=headers)
        _soup = BeautifulSoup(__res.text, "html.parser")
        _books = _soup.find('div',{'class':'product_view_tab_content_ad'})
        table = _books.find('table')
        
        df = pd.read_html(str(table))[0]
        
        book['tên sách'] = re.sub("(?m)^\s+","", _soup.h1.text.rstrip())
        book['ảnh bìa'] = _soup.find("div",{"class":"product-view-image-product"}).img['src']
        book['thể loại'] = self.category_name
        book['tác giả'] = df[1][3]
        
        book['nội dung tóm tắt'] = _soup.find("div",{"id":"desc_content"}).text
        book['giá bìa'] = unicodedata.normalize("NFKD",_soup.findAll("span",{"class":"price"})[-1].text)
        book['path'] = _url


        print(book)
        booksQueue.put(book)
        print(f"Number of products in my pocket {booksQueue.qsize()}")
        
    def _bookshelf(self):
        
        _bookshelf = []
        print(self.url.format(48, 1))
        _res = requests.request("GET", self.url.format(48, 1), headers=headers)
        soup = BeautifulSoup(_res.text, "html.parser")
        _pages = soup.find('div',{'class':'pages'}).findAll('li')
        
        if len(_pages) == 0:
            _total = 1
        else:
            _total = int(re.findall(r'\d+', _pages[-2].text)[0])
        
        for _ in range(_total):
            _ =+1
#             print(f"total page of {self.urlkey} : {_total}")
            _res = requests.request("GET", self.url.format(48, _), headers=headers)
            soup = BeautifulSoup(_res.text, "html.parser")
            for _ in soup.findAll("div",{"class":"product images-container"}):
                _bookshelf.append(_.a['href'])
            
        return _bookshelf

booksQueue = queue.Queue()

for _name in n_dict.keys():
    booksQueue = queue.Queue()
    for k,v in n_dict[_name].items():
        print(k)
        t = Crawler(v, k)
        t.crawl()

    total += booksQueue.qsize()
    df = pd.DataFrame(columns=__name.columns,index=[0])
    while not booksQueue.empty():
        df = df.append(booksQueue.get(),ignore_index=True)

    df.to_csv('fahasa/fahasa_'+ _name+'.csv')
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

__name = pd.read_csv("CHĂM SÓC GIA ĐÌNH.csv")
__name.columns

payload={}
headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.5',
  'Connection': 'keep-alive',
  'Cookie': '_trackity=1082ef4b-85f0-5a23-bda4-d0f85ca59eb4; TOKENS={%22access_token%22:%22UrzZ4hO8H7mnY5MNlw3QCyqDP9VSAjxe%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1793091946598%2C%22guest_token%22:%22UrzZ4hO8H7mnY5MNlw3QCyqDP9VSAjxe%22}; delivery_zone=Vk4wMzQwMjkwMTE=; _gcl_au=1.1.148276762.1635411962; amp_99d374=-hnSJN7KLOKBaRmwRzDaXK...1fj32cb00.1fj33ak5c.2.4.6; _ga=GA1.1.1599687975.1635411964; _gid=GA1.2.1838217135.1635411964; _fbp=fb.1.1635411969911.1818135830; _ga_GSD4ETCY1D=GS1.1.1635411970.1.1.1635412978.0; tiki_client_id=1599687975.1635411964; _hjid=4422e7ba-26a3-4813-87e3-d461db045594; _hjIncludedInSessionSample=0; cto_bundle=X6p7gV91T3B4eVJOTnlDdjlaa0EzQmJKdmlxeFMlMkJnWVZ3OWg2cHhhdW1Cc3ZicXNYWUtaRDVmcTE1MnJIaFBhYjFvRHN2cnRuTDhsUzN1RzgxTmtBanklMkZlRTQ1SmZaWlRVWnlrWVFTckQlMkZUcEVkNzdGZGNoS2JmUEJGQzElMkJUaEpOa1clMkI; __iid=749; __iid=749; __su=0; __su=0',
  'Upgrade-Insecure-Requests': '1',
  'Sec-Fetch-Dest': 'document',
  'Sec-Fetch-Mode': 'navigate',
  'Sec-Fetch-Site': 'none',
  'Sec-Fetch-User': '?1',
  'Pragma': 'no-cache',
  'Cache-Control': 'no-cache'
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
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(self._books, _concurrent)
            
    def _books(self, book_tuple):
        
        book = {}
        
        __res =  requests.request("GET",_book_detail.format(book_tuple[0],book_tuple[1]), headers=headers).json()
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
        book['Nội dung tóm tắt'] = unicodedata.normalize("NFKD",__res['short_description'])
        
        print(book)
        booksQueue.put(book)
            
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

total = 0

for _name in n_dict.keys():
    booksQueue = queue.Queue()
    for k,v in n_dict[_name].items():
        print(v)
        _split = v.split("/")
        t = Crawler(_split[-1][1:],_split[-2], k)
        t.crawl()
    
    total += booksQueue.qsize()
    df = pd.DataFrame(columns=['Tên Sách', 'Ảnh bìa','Thể loại','Tác giả','Nội dung tóm tắt','Giá bìa','Công ty phát hành','Nhà xuất bản','Ngày xuất bản','Kích thước',
                               'Loại bìa','Số trang','Dịch Giả',],index=[0])
    while not booksQueue.empty():
        df = df.append(booksQueue.get(),ignore_index=True)

    df.to_csv('tiki/tiki_'+ _name+'.csv',index=False)

print(f"total: ",total)
# -*- coding: utf-8 -*-
"""
AAstock_chartpx

"""

from bs4 import BeautifulSoup
import requests, datetime, openpyxl, lxml
import pandas as pd
import urllib3

def AA_Chart_Price(ticker):
  #Generate URL
  code, exchange, sec_type = ticker.split()
  exchange = exchange.upper() if exchange.upper() != 'CH' else 'C1' if code[0] == '6' else 'C2'
  exch_dict = {'HK':'HK','C1':'SH','C2':'SZ','US':'US'}
  code = code.upper() + '.' + exch_dict[exchange]
  us_ex = 'us' if exchange == 'US' else ''
  chartdata1_url= 'http://' + us_ex + 'chartdata1.internet.aastocks.com/servlet/iDataServlet/getdaily?id=' + code + '&type=24&market=1&level=1&period=56&encoding=utf8'
  
  #Request Soup
  http = urllib3.PoolManager()
  r = http.request('GET', chartdata1_url)
  soup = BeautifulSoup(r.data, 'lxml')
  soup_text = soup.body.p.text
  
  #Extract Today's Price
  todayy = datetime.datetime.today()
  todayy = str(todayy.strftime('%m/%d/%Y'))
  price = soup_text.split(todayy)[1].split(';')[4]
  return price


#dates
in_date = datetime.datetime.today()
yymmdd = in_date.strftime('%y%m%d')
ddmmmyyyy = in_date.strftime('%d-%b-%Y')

#Start by grabbing ticker file and store data in dataframe
portdf = pd.read_excel('port_tickers.xlsx') #need to leave header in excel
tckr_ls = list(portdf[portdf.columns[0]])

print('Getting prices for ' + ddmmmyyyy) #display

px_dict = {} #stores prices and corresponding ticker
for ticker in tckr_ls:
  try:
    price = AA_Chart_Price(ticker)   #grabs price from AA Charts
    px_dict[ticker]=price
  except:
    print('skipped '+ ticker)

#TDP Loader Definitions
ldr_dict = {} #loader content
tdp_cred = ['#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world','#!MAX_ERROR=1000','#!OPF=TDP_LOADER.import_price'] #loader credentials
#file type header chooser
px_hdr = ['#in_ladder_date','in_ident_type','in_ext_ident','in_value_spec','in_price','in_hilo_ind','in_price_ccy','in_notes'] #headers for this type of loader
fx_hdr = []

ldr_hdr = px_hdr #picked which file/header to use
ldr_col = len(ldr_hdr) #counter and defines the width of the file by # of columns
ldr_row = 0 #counter

#TDP Loader Construction
ldr_dict = {}
tdp_hdr = ['#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world','#!MAX_ERROR=1000','#!OPF=TDP_LOADER.import_price']
px_hdr = ['#in_ladder_date','in_ident_type','in_ext_ident','in_value_spec','in_price','in_hilo_ind','in_price_ccy','in_notes']
ldr_col = len(px_hdr)
ldr_row = 0

for hdr in tdp_hdr:
  ldr_list = [''] * ldr_col
  ldr_list[0] = hdr
  ldr_dict[ldr_row] = ldr_list
  ldr_row += 1
ldr_dict[ldr_row] = ldr_hdr
ldr_row += 1
for i in px_dict:
  ldr_dict[ldr_row] = [ddmmmyyyy,'BB_TCM',i,1,px_dict[i],'','','']
  ldr_row +=1
ldr_df = pd.DataFrame.from_dict(ldr_dict,orient='index')

#Generate PRICE Loader File
filename = 'tdp_loader_PRICE_' + yymmdd + '.xlsx'
print(filename)
ldr_df.to_excel(filename,index=0,header=False)


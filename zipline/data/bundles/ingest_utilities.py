# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 17:49:59 2018

@author: Prodipta
"""

import os
import pandas as pd
import zipfile
import shutil
import requests
from bs4 import BeautifulSoup
import bisect
import numpy as np

def touch(fname, fpath, times=None):
    with open(os.path.join(fpath,fname), 'a'):
        os.utime(os.path.join(fpath,fname), times)

def unzip_to_directory(zippath, extractpath):
    with zipfile.ZipFile(zippath) as z:
        for f in z.namelist():
            if f.endswith('.csv'):
                filename = os.path.basename(f)
                source = z.open(f)
                target = file(os.path.join(extractpath, filename), "wb")
                with source, target:
                    shutil.copyfileobj(source, target)

def copy_to_directory(inputpath, outputpath):
    pass

def clean_up(strpath):
    items = os.listdir(strpath)
    folders = [f for f in items if os.path.isdir(os.path.join(strpath,f))]
    files = [f for f in items if os.path.isfile(os.path.join(strpath,f))]
    for f in files:
        shutil.os.remove(os.path.join(strpath,f))
    for folder in folders:
        shutil.rmtree(os.path.join(strpath,folder))

def if_csvs_in_dir(strpath):
    items = os.listdir(strpath)
    files = [f for f in items if f.endswith(".csv")]
    if len(files) > 0:
        return True
    return False

def get_ohlcv(dfr, ticker=None):
    if ticker:
        dfr = dfr[ticker]
    return dfr.loc[:,['open','high','low','close','volume']]

def split_csvs(dfr, strpath, maps=None, OHLCV=True):
    cols = list(dfr.columns.values)
    #cols.remove('ticker')
    syms = set(dfr['ticker'].tolist())
    for s in syms:
        dfs = dfr.loc[dfr['ticker']==s,cols].set_index('date')
        dfs.index = pd.to_datetime(dfs.index)
        if maps is not None:
            start_date = pd.to_datetime(maps.loc[maps.symbol==s,'start_date'].tolist())[-1]
            end_date = pd.to_datetime(maps.loc[maps.symbol==s,'end_date'].tolist())[-1]
            dfs = dfs[start_date:end_date]
        if OHLCV:
            dfs = get_ohlcv(dfs)

        dfs.to_csv(os.path.join(strpath,s+".csv"))

def update_csvs(dfr,strpath, OHLCV=True):
    items = os.listdir(strpath)
    syms = [f.split(".csv")[0] for f in items if f.endswith(".csv")]
    for s in syms:
        dfs_o = pd.read_csv(os.path.join(strpath,s+".csv"),parse_dates=[0],index_col=0).sort_index()
        dfs_n = dfr.loc[dfr['ticker']==s].set_index('date')
        if OHLCV:
            dfs_n = get_ohlcv(dfs_n)
        dfs = pd.concat([dfs_o,dfs_n])
        dfs.to_csv(os.path.join(strpath,s+".csv"))

def read_big_csv(strpath,tickers, pattern="", header = 0, ticker_col=0):
    items = os.listdir(strpath)
    files = [f for f in items if f.endswith(".csv") and pattern in f]

    ts = [os.stat(os.path.join(strpath,f)).st_mtime for f in files]
    idx = ts.index(max(ts))

    datafile = files[idx]
    print("reading {}".format(datafile))
    reader = pd.read_csv(os.path.join(strpath,datafile), header=header, iterator=True, chunksize=1000)
    dfr = pd.concat([chunk[chunk.iloc[:,ticker_col].isin(tickers)] for chunk in reader])

    print("read total {} rows".format(len(dfr)))

    return dfr

def download_spx_changes(wiki_url):
    req = requests.get(wiki_url)
    soup = BeautifulSoup(req.content, 'lxml')
    table_classes = {"class": ["sortable", "wikitable", "jquery-tablesorter"]}
    wikitables = soup.findAll("table", table_classes)
    tickertable = wikitables[0]
    changetable = wikitables[1]

    # get the current ticker
    rows = [item.get_text() for item in tickertable.find_all('tr')]
    col_names = rows[0].split('\n')[1:-1]
    tickers = pd.DataFrame(columns=col_names)
    for i in range(1,len(rows)):
        row = rows[i].split('\n')[1:-1]
        try:
            tickers.loc[len(tickers)] = tuple(row)
        except:
            pass
            #print(row)
    tickers.columns = ['symbol','name','filing','sector','sub_industry','address','date','CIK', 'Founded']

    # now get the ticker change table
    rows = [item.get_text() for item in changetable.find_all('tr')]
    col_names = rows[1].split('\n')[1:-1]
    running_reason = ''

    tabs = pd.DataFrame(columns=col_names)
    for i in range(2,len(rows)):
        row = rows[i].split('\n')[1:-1]
        if len(row) > 6:
                row = row[:5]
        try:
            dt = pd.to_datetime(row[0],format='%B %d, %Y')
            if len(row) < 6:
                row = row + ["" for f in range(len(row),6)]
                row[-1] = running_reason

            tabs.loc[len(tabs)] = (dt,) + tuple(row[1:])
            running_dt = dt
            running_reason = row[-1]
        except ValueError:
            if len(row) < 5:
                row = row + ["" for f in range(len(row),5)]
                row[-1] = running_reason
            tabs.loc[len(tabs)] = (running_dt,) + tuple(row)

    tabs.columns = ['date','add','name_added','delete','name_deleted','reason']
    tabs = tabs.sort_values('date')
    return {"tickers": tickers, "change":tabs}

def find_interval(x, lst):
    try:
        idx = lst.index(x)
    except:
        idx = max(0,bisect.bisect_left(lst,x)-1)
    return idx

def upsert_pandas(dfr, sym_col, sym, date_col, date, names_dict):
    if sym in dfr[sym_col].tolist():
        dfr.loc[dfr[sym_col]==sym,date_col] = date.strftime("%Y-%m-%d")
    else:
        dfr.loc[len(dfr),:] = sym,names_dict.get(sym,sym),date.strftime("%Y-%m-%d"),date.strftime("%Y-%m-%d")

def update_ticker_change(membership_maps,tickers_list):
    membership_maps['start_date'] = pd.to_datetime(membership_maps['start_date'])
    membership_maps['end_date'] = pd.to_datetime(membership_maps['end_date'])

    if len(tickers_list) == 0:
        return membership_maps

    old_tickers = tickers_list['old'].tolist()
    new_tickers = tickers_list['new'].tolist()
    new_names = tickers_list['name'].tolist()
    ticker_maps = dict(zip(old_tickers,new_tickers))
    names_maps = dict(zip(old_tickers,new_names))

    for t in old_tickers:
        old_entry = membership_maps[membership_maps['symbol']==t]
        new_entry = membership_maps[membership_maps['symbol']==ticker_maps[t]]

        if len(old_entry)==0:
            continue
        if len(old_entry)>1:
            raise ValueError("Duplicate entries in membership maps")
        update_idx = old_entry.index
        membership_maps.loc[membership_maps['symbol']==t,'asset_name'] = names_maps[t]
        membership_maps.loc[membership_maps['symbol']==t,'symbol'] = ticker_maps[t]

        if len(new_entry) ==0:
            continue
        if len(new_entry)>1:
            raise ValueError("Duplicate entries in membership maps")

        remove_idx = new_entry.index
        start_date = min(old_entry['start_date'].values,new_entry['start_date'].values)
        end_date = max(old_entry['end_date'].values,new_entry['end_date'].values)
        membership_maps.iloc[update_idx,2] = start_date
        membership_maps.iloc[update_idx,3] = end_date
        membership_maps.iloc[remove_idx] = np.nan

    membership_maps = membership_maps.dropna()
    return membership_maps

def ensure_data_between_dates(fname,start_date, end_date):
    if not os.path.isfile(fname):
        raise IOError("file does not exists")

    dfr = pd.read_csv(fname,parse_dates=[0], infer_datetime_format=True,
                      index_col=0).sort_index()
    try:
        dfr = dfr[start_date:end_date]
    except:
        print("sym {}, start {}, end {}".format(fname,start_date,end_date))
        os.remove(fname)

    if len(dfr) == 0:
        os.remove(fname)
        return

    dfr.to_csv(fname)

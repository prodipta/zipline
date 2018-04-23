# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 17:49:59 2018

@author: Prodipta
"""

import os
import pandas as pd
import zipfile
import shutil

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

def split_csvs(dfr, strpath, maps=None):
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
        dfs = get_ohlcv(dfs)
        dfs.to_csv(os.path.join(strpath,s+".csv"))
        
def update_csvs(dfr,strpath):
    items = os.listdir(strpath)
    syms = [f.split(".csv")[0] for f in items if f.endswith(".csv")]
    for s in syms:
        dfs_o = pd.read_csv(os.path.join(strpath,s+".csv"),parse_dates=[0],index_col=0).sort_index()
        dfs_n = dfr.loc[dfr['ticker']==s].set_index('date')
        dfs = pd.concat([dfs_o,get_ohlcv(dfs_n)])
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
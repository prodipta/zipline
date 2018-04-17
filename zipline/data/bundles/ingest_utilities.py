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

def split_csvs(dfr, strpath):
    cols = list(dfr.columns.values)
    #cols.remove('ticker')
    syms = set(dfr['ticker'].tolist())
    for s in syms:
        dfs = dfr.loc[dfr['ticker']==s,cols].set_index('date')
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

def read_big_csv(strpath,tickers, pattern=""):
    items = os.listdir(strpath)
    files = [f for f in items if f.endswith(".csv") and pattern in f]

    assert len(files)==1, (
            "Expected only one csv file with historical data, "
            "got {}.\n Files are {}\n"
            "Do not know which one to read".format(len(files),files)
            )
    
    datafile = files[0]
    reader = pd.read_csv(os.path.join(strpath,datafile), iterator=True, chunksize=1000)
    dfr = pd.concat([chunk[chunk['ticker'].isin(tickers)] for chunk in reader])
    print("read total {} rows".format(len(dfr)))
    return dfr
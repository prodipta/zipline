# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 18:37:53 2018

@author: Prodipta
"""

import os
import pandas as pd
import numpy as np
import quandl
import json
import sys
import re
import requests
from StringIO import StringIO
import nsepy

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part


from zipline.data import bundles as bundles_module
from zipline.data.bundles import register
from zipline.data.bundles.XNSE import xnse_equities
from zipline.data.bundles.ingest_utilities import read_big_csv,split_csvs,unzip_to_directory,clean_up,get_ohlcv, find_interval, upsert_pandas, update_ticker_change, if_csvs_in_dir, ensure_data_between_dates

def subset_adjustment(dfr, meta_data):
    meta_data['start_date'] = pd.to_datetime(meta_data['start_date'])
    meta_data['end_date'] = pd.to_datetime(meta_data['end_date'])
    dfr.iloc[:,0] = pd.to_datetime(dfr.iloc[:,0])
    syms = dfr['symbol'].tolist()
    for s in syms:
        start_date = meta_data.loc[meta_data['symbol']==s,'start_date'].iloc[0]
        end_date = meta_data.loc[meta_data['symbol']==s,'end_date'].iloc[0]
        dfr = dfr[~((dfr.symbol == s) & (dfr.iloc[:,0] < start_date))]
        dfr = dfr[~((dfr.symbol == s) & (dfr.iloc[:,0] > end_date))]
    return dfr
        

def load_quandl_tickers(tickers_path):
    df = pd.read_csv(tickers_path, header=None)
    df.columns = ['Ticker','Name']
    df = df[df.Name.str.contains("\(EQ(.+)\) Unadjusted")]
    tickers = [re.search("\(EQ(.+)\) Unadjusted",s).group(1) for s in df.Name.tolist()]
    names = [s.split(' (EQ'+tickers[i])[0] for i,s in enumerate(df.Name.tolist())]
    names = [s.split("Ltd")[0].strip() for s in names]
    quandl_tickers = df.Ticker.tolist()
    df_dict = {'symbol':tickers,'qsymbol':quandl_tickers, 'name':names}
    tickers = pd.DataFrame(df_dict,columns=['symbol','qsymbol','name'])
    return tickers

def nse_to_quandl_tickers(syms,tickers,strip_prefix=False, strip_suffix=False):
    tickers = tickers[~tickers.qsymbol.str.endswith("_1_UADJ")]
    map_dict = dict(zip(tickers.symbol,tickers.qsymbol))
    qsyms = [map_dict[s] for s in syms]
    
    if strip_prefix:
        qsyms = [q.split("XNSE/")[1] for q in qsyms]
    if strip_suffix:
        qsyms = [q.split("_UADJ")[0] for q in qsyms]
    
    return qsyms

def quandl_to_nse_tickers(qsyms,tickers,add_prefix=False, add_suffix=False):
    if add_prefix:
        qsyms = ["XNSE/"+q for q in qsyms]
    if add_suffix:
        qsyms = [q+"_UADJ" for q in qsyms]
    
    map_dict = dict(zip(tickers.qsymbol,tickers.symbol))
    syms = [map_dict[q] for q in qsyms]
    
    return syms

def get_latest_symlist(url):
    try:
        r = requests.get(url)
        syms = pd.read_csv(StringIO(r.content))
    except:
        raise IOError("failed to download the latest NIFTY500 membership")
    return syms['Symbol'].tolist()

def download_data(url, api_key, strpath):
    try:
        r = requests.get(url+api_key, stream=True)
        zipdata = StringIO()
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk:
                zipdata.write(chunk)
        unzip_to_directory(zipdata,strpath)
    except:
        raise IOError("failed to download latest data from Quandl")

def adjustment_classifier(x):
    s = str(int(x))
    order = ['s','m','d']
    s = re.sub(r"[1][7]|[1][8]|[2][0]", "d", s)
    s = re.sub(r"[6]","m",s)
    s = re.sub(r"[1-9]","s",s)
    s = re.sub(r"[0]","",s)
    s = "".join(set(s))
    s = reduce((lambda x,y: x 
                if order.index(x) < order.index(y) else y), s)
    if len(s) != 1:
        raise ValueError('Undefined adjustment type, please check input {}'.format(x))
    return s

class IngestLoop:
    
    def __init__(self, configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            quandl.ApiConfig.api_key = config["QUANDL_API_KEY"]
            self.config_path = configpath
            self.api_key = config["QUANDL_API_KEY"]
            self.quandl_table_name = config["QUANDL_TABLE_NAME"]
            self.bundle_name=config["BUNDLE_NAME"]
            self.bundle_path=config["BUNDLE_PATH"]
            self.calendar_name=config["CALENDAR_NAME"]
            self.calendar_tz=config["CALENDAR_TZ"]
            self.meta_path=config["META_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.download_path=config["DOWNLOAD_PATH"]
            self.bizdays_file=config["BIZDAYLIST"]
            self.symlist_file=config["SYMLIST"]
            self.sym_directory=config["SYM_DIRECTORY"]
            self.sym_url = config['SYMBOLS_DOWNLOAD_URL']
            self.data_url = config['DATA_DOWNLOAD_URL']
            self.code_url = config['CODE_DOWNLOAD_URL']
            self.benchmark_file = config['BENCHMARKDATA']
            self.benchmark_sym = config['BENCHMARK_SYM']
            self.benchmark_download_sym = config['BENCHMARK_DOWNLOAD_SYM']
            self.code_file = config["QUANDLE_TICKER_MAP"]
            self.ticker_change_file = config["TICKER_CHANGE_FILE"]
        self.ensure_codes()
        self.ensure_data()
        self.tickers = load_quandl_tickers(os.path.join(self.meta_path,config["QUANDLE_TICKER_MAP"]))
        self.last_data_dt = self.get_last_data_date()

    def get_last_data_date(self):
        dts = []
        files = os.listdir(self.download_path)
        try:
            dts = [pd.to_datetime(f.split("XNSE_")[1].split(".csv")[0]) for f in files]
        except:
            pass
        if dts:
            return dts[-1].tz_localize(tz=self.calendar_tz)
        
        return None
        
    def ensure_codes(self):
        if not os.path.isfile(os.path.join(self.meta_path,self.code_file)):
            print("code file missing, downloading...")
            download_data(self.code_url,self.api_key,self.meta_path)
            print("code file download complete")
            
    def ensure_data(self):
        items = os.listdir(self.download_path)
        files = [f for f in items if f.endswith(".csv") and "XNSE" in f]
        if not files:
            print("data file missing, downloading...")
            download_data(self.data_url,self.api_key,self.download_path)
            print("data file download complete")
            
    def ensure_latest_data(self, date):
        if date > self.last_data_dt:
            print("data file stale, downloading...")
            download_data(self.data_url,self.api_key,self.download_path)
            print("data file download complete")
            self.last_data_dt = self.get_last_data_date()
        if self.last_data_dt is None:
            raise ValueError("no data available")
            
    def ensure_benchmark(self, date):
        dts = self.get_bizdays()
        if not os.path.isfile(os.path.join(self.meta_path,self.benchmark_file)):
            print("benchmark data missing, downloading...")
            benchmark = nsepy.get_history(symbol=self.benchmark_download_sym, start=dts[0], end=dts[-1],index=True)
            print("benchmark data download complete")
            benchmark.index = pd.to_datetime(benchmark.index)
            benchmark = benchmark.dropna()
            benchmark.to_csv(os.path.join(self.meta_path,self.benchmark_file))
        
        benchmark = pd.read_csv(os.path.join(self.meta_path,self.benchmark_file),index_col=[0], parse_dates=[0])
        last_date = benchmark.index[-1]
        last_date = last_date.tz_localize(tz=self.calendar_tz)
        if date > last_date:
            print("benchmark data stale, updating from {} to {}".format(last_date.strftime("%Y-%m-%d"),date.strftime("%Y-%m-%d")))
            increment = nsepy.get_history(symbol=self.benchmark_download_sym, start=last_date, end=date,index=True)
            print("benchmark data download complete")
            benchmark = pd.concat([benchmark,increment])
            benchmark.index = pd.to_datetime(benchmark.index)
            
        benchmark = benchmark.dropna()
        benchmark = benchmark[~benchmark.index.duplicated(keep='last')]
        benchmark.to_csv(os.path.join(self.meta_path,self.benchmark_file))
        benchmark.columns = benchmark.columns.str.lower()
        benchmark = get_ohlcv(benchmark)
        if len(benchmark) == 0:
            raise ValueError("benchmark data does not exist")
        benchmark.to_csv(os.path.join(self.daily_path,self.benchmark_sym+".csv"))
    
    def _read_symlist(self,strpath):
        sym_list = pd.read_csv(strpath)
        return sym_list
    
    def ensure_latest_sym_list(self,date):        
        dts = [dt.split(".csv")[0].split("members_")[1] for dt in os.listdir(os.path.join(self.meta_path,self.sym_directory))]
        dts = pd.to_datetime(sorted(dts))
        dts = dts.tz_localize(tz=self.calendar_tz)
        
        if date > dts[-1]:
            print("membership data stale, updating...")
            symbols = get_latest_symlist(self.sym_url)
            print("membership data download complete")
            if symbols:
                symfile = "members_"+pd.to_datetime(date,format='%d%m%Y').date().strftime('%Y%m%d')+".csv"
                pd.DataFrame(symbols,columns=['symbol']).to_csv(os.path.join(self.meta_path,self.sym_directory,symfile))
            else:
                raise ValueError("failed to download the symbols list")
    
    def create_bizdays_list(self, dts):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        dts = pd.to_datetime(sorted(set(dts)))
        dts = [d for d in dts if d.weekday()<5]
        bizdays = pd.DataFrame(sorted(set(dts)),columns=['dates'])
        if len(bizdays) == 0:
            raise ValueError("empty business days list")
        bizdays.to_csv(strpathmeta,index=False)
        
    def get_bizdays(self):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        bizdays = pd.read_csv(strpathmeta)
        return pd.to_datetime(bizdays['dates'].tolist())
        
    def ensure_membership_maps(self):
        if os.path.isfile(os.path.join(self.meta_path,self.symlist_file)):
            membership_maps = pd.read_csv(os.path.join(self.meta_path,self.symlist_file))
            last_date = sorted(set(pd.to_datetime(membership_maps.end_date.tolist())))[-1]
        else:
            membership_maps = pd.DataFrame(columns=['symbol','asset_name','start_date','end_date'])
            last_date = pd.to_datetime(0)
        
        dts = [dt.split(".csv")[0].split("members_")[1] for dt in os.listdir(os.path.join(self.meta_path,self.sym_directory))]
        dts = pd.to_datetime(sorted(dts))
        ndts = [d.value/1E9 for d in dts]
        ndate = last_date.value/1E9
        dts = dts[find_interval(ndate,ndts):]
        
        print("updating membership data...")
        names_dict = dict(zip(self.tickers.symbol,self.tickers.name))
        for dt in dts:
            fname = "members_"+dt.date().strftime("%Y%m%d")+".csv"
            print('reading {}'.format(fname))
            syms = pd.read_csv(os.path.join(self.meta_path,self.sym_directory,fname))['symbol'].tolist()
            for sym in syms:
                upsert_pandas(membership_maps, 'symbol', sym, 'end_date', dt, names_dict)
                
        if len(membership_maps) == 0:
            raise ValueError("empty membership data")
        
        print("checking for ticker change")
        tickers_list = pd.read_csv(os.path.join(self.meta_path,self.ticker_change_file))
        membership_maps = update_ticker_change(membership_maps,tickers_list)
        print("updating membership complete")
        membership_maps.to_csv(os.path.join(self.meta_path,self.symlist_file),index=False)
        self.symlist = membership_maps

    def create_csvs(self, date):
        #clean_up(self.daily_path)
        colnames=['ticker','date','open','high','low','close',
                                    'volume','adj_factor','adj_type']
        print("reading data from disk...")
        syms = nse_to_quandl_tickers(self.symlist['symbol'],self.tickers,strip_prefix=True)
        dfr = read_big_csv(self.download_path, syms,"XNSE", header=None)
        dfr.columns = colnames
        qsyms = dfr.ticker.tolist()
        dfr.ticker = quandl_to_nse_tickers(qsyms,self.tickers,add_prefix=True, add_suffix=False)
        print("saving csvs to disk for ingestion...")
        split_csvs(dfr,self.daily_path, maps=self.symlist)
        dts = dfr['date']
        print("updating date lists...")
        self.create_bizdays_list(dts)
        print("creating adjustments data...")
        dfr['prev_close'] = dfr.close.shift(1)
        dfr['match'] = dfr.ticker.eq(dfr.ticker.shift())
        self.make_adjustments_maps(dfr)
        print("adjustment data creation complete.")
        
    def ensure_data_range(self):
        if not if_csvs_in_dir(self.daily_path):
            raise IOError("csv data files are not available")
            
        files = [s for s in os.listdir(self.daily_path) if s.endswith(".csv")]
        
        for f in files:
            sym = f.split('.csv')[0]
            if sym == self.benchmar_symbol:
                continue
            start_date = self.symlist.start_date[self.symlist.symbol==sym].tolist()[0]
            end_date = self.symlist.end_date[self.symlist.symbol==sym].tolist()[0]
            ensure_data_between_dates(os.path.join(self.daily_path,f),
                                      start_date, end_date)
            
    def update_membership_maps(self):
        if not if_csvs_in_dir(self.daily_path):
            raise IOError("csv data files are not available")
            
        syms = [s.split(".csv")[0] for s in os.listdir(self.daily_path) if s.endswith(".csv")]
        membership_maps = self.symlist
        membership_maps = membership_maps[membership_maps.symbol.isin(syms)]
        self.symlist = membership_maps
        membership_maps.to_csv(os.path.join(self.meta_path,self.symlist_file),index=False)
    
    def make_adjustments_maps(self,dfr):
        dts = list(self.get_bizdays())
        meta_data = pd.read_csv(os.path.join(self.meta_path,self.symlist_file))
        adj_dfr = dfr.dropna()
        adj_dfr.adj_type = adj_dfr.adj_type.apply(adjustment_classifier)
        adj_dfr.loc[adj_dfr['match']==False,'prev_close'] = np.nan
        
        splits = adj_dfr.loc[adj_dfr.adj_type=='s',['date','ticker','adj_factor']]
        splits.columns = ['effective_date','symbol','ratio']
        splits.effective_date = pd.to_datetime(splits.effective_date)
        splits = subset_adjustment(splits,meta_data)
        splits = splits.sort_values(by=['effective_date'])
        
        mergers = adj_dfr.loc[adj_dfr.adj_type=='m',['date','ticker','adj_factor']]
        mergers.columns = ['effective_date','symbol','ratio']
        mergers.effective_date = pd.to_datetime(mergers.effective_date)
        mergers = subset_adjustment(mergers,meta_data)
        mergers = mergers.sort_values(by=['effective_date'])
        
        divs = adj_dfr.loc[adj_dfr.adj_type=='d',['date','ticker','adj_factor','prev_close']]
        divs['amount'] = (1 - divs.adj_factor)*divs.prev_close
        divs = divs.drop(['adj_factor','prev_close'],axis=1)
        divs.columns = ['ex_date','symbol','amount']
        divs.ex_date = pd.to_datetime(divs.ex_date)
        divs['declared_date'] = [dts[max(0,dts.index(e)-1)] for e in list(divs.ex_date)]
        divs['record_date'] = [dts[min(len(dts)-1,dts.index(e)+2)] for e in list(divs.ex_date)]
        divs['pay_date'] = divs['record_date']
        divs.declared_date = pd.to_datetime(divs.ex_date)
        divs.record_date = pd.to_datetime(divs.ex_date)
        divs.pay_date = pd.to_datetime(divs.ex_date)
        divs = subset_adjustment(divs,meta_data)
        divs = divs.sort_values(by=['ex_date'])
        
        splits.to_csv(os.path.join(self.meta_path,'splits.csv'),index=False)
        mergers.to_csv(os.path.join(self.meta_path,'mergers.csv'),index=False)
        divs.to_csv(os.path.join(self.meta_path,'dividends.csv'),index=False)
        
        
    def register_bundle(self):
        dts = (self.get_bizdays()).tz_localize(self.calendar_tz)
        register(self.bundle_name, xnse_equities(self.config_path),calendar_name=self.calendar_name,
                 start_session=dts[0],end_session=dts[-1],
                 create_writers=False)
    
    def call_ingest(self):
        clean_up(os.path.join(self.bundle_path,"minute"))
        clean_up(os.path.join(self.bundle_path,"daily"))
        print("calling ingest function")
        self.register_bundle()
        bundles_module.ingest(self.bundle_name,os.environ,pd.Timestamp.utcnow())
        print("ingestion complete")
    
    def run(self, date, update_codes=False):
        if update_codes:
            download_data(self.code_url,self.api_key,self.meta_path)
        self.ensure_latest_sym_list(date)
        self.ensure_membership_maps()
        self.ensure_latest_data(date)
        self.create_csvs(date)
        self.ensure_benchmark(date)
        self.call_ingest()

#config_path = "C:/Users/academy.academy-72/Desktop/dev platform/data/XNSE/meta/config.json"
#ingest_loop = IngestLoop(config_path)
        
def main():
    assert len(sys.argv) == 4, (
            'Usage: python {} <date>'
            ' <path_to_config> <code download flag>'.format(os.path.basename(__file__)))
        
    dt = pd.Timestamp(sys.argv[1],tz='Etc/UTC')
    config_file = sys.argv[2]
    update_codes = sys.argv[3]
    
    ingest_looper = IngestLoop(config_file)
    ingest_looper.run(dt, update_codes)


if __name__ == "__main__":
    main()

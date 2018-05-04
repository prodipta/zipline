# -*- coding: utf-8 -*-
"""
Created on Sat Apr 14 10:14:23 2018

@author: Prodipta
"""
import os
import pandas as pd
import quandl
import json
import sys
import requests
from StringIO import StringIO
from datetime import datetime

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.data import bundles as bundles_module
from zipline.data.bundles import register
from zipline.data.bundles.SEP import sep_equities
from zipline.data.bundles.ingest_utilities import read_big_csv,if_csvs_in_dir,split_csvs,update_csvs, clean_up, find_interval, upsert_pandas, unzip_to_directory, update_ticker_change, ensure_data_between_dates

def process_tickers_convention(tickers):
    tickers = [t.replace("_","") for t in tickers]
    tickers = [t.replace(".","") for t in tickers]
    return tickers

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

class IngestLoop:
    
    def __init__(self, configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            self.config_path = configpath
            quandl.ApiConfig.api_key = config["QUANDL_API_KEY"]
            self.quandl_table_name = config["QUANDL_TABLE_NAME"]
            self.meta_path=config["META_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.download_path=config["DOWNLOAD_PATH"]
            self.bizdays_file=config["BIZDAYLIST"]
            self.wiki_url=config["WIKI_URL"]
            self.sym_directory=config["SYM_DIRECTORY"]
            self.symlist_file=config["SYMLIST"]
            self.calendar_name=config["CALENDAR_NAME"]
            self.calendar_tz=config["CALENDAR_TZ"]
            self.code_file = config["SYMDATA"]
            self.ticker_change_file = config["TICKER_CHANGE_FILE"]
            self.benchmar_symbol=config["BENCHMARK_SYM"]
            self.benchmark_file=config["BENCHMARKDATA"]
            self.bundle_name=config["BUNDLE_NAME"]
            self.bundle_path=config["BUNDLE_PATH"]

    def ensure_codes(self, date):
        if not os.path.isfile(os.path.join(self.meta_path,self.code_file)):
            raise IOError("tickers list file missing")
        
        mtime = pd.to_datetime(datetime.fromtimestamp(os.stat(os.path.join(self.meta_path,self.code_file)).st_mtime))
        time_delta = date - mtime
        
        if time_delta.days > 5:
            raise ValueError("tickers list file is stale, please update")
            
        self.tickers = pd.read_csv(os.path.join(self.meta_path,self.code_file))
        self.tickers = self.tickers[self.tickers['table']=='SEP']
        
    def ensure_latest_sym_list(self,date):     
        self.ensure_codes(date)
        dts = [dt.split(".csv")[0].split("symbols_")[1] for dt in os.listdir(os.path.join(self.meta_path,self.sym_directory))]
        dts = pd.to_datetime(sorted(dts))
        if date > dts[-1]:
            raise ValueError("symbol list in the symbols directory is stale. Please update")
    
    def _read_symlist(self,strpath):
        sym_list = pd.read_csv(strpath)
        return sym_list   
        
    def ensure_membership_maps(self):
        if os.path.isfile(os.path.join(self.meta_path,self.symlist_file)):
            membership_maps = pd.read_csv(os.path.join(self.meta_path,self.symlist_file))
            last_date = sorted(set(pd.to_datetime(membership_maps.end_date.tolist())))[-1]
        else:
            membership_maps = pd.DataFrame(columns=['symbol','asset_name','start_date','end_date'])
            last_date = pd.to_datetime(0)
        
        dts = [dt.split(".csv")[0].split("symbols_")[1] for dt in os.listdir(os.path.join(self.meta_path,self.sym_directory))]
        dts = pd.to_datetime(sorted(dts))
        ndts = [d.value/1E9 for d in dts]
        ndate = last_date.value/1E9
        dts = dts[find_interval(ndate,ndts):]
        
        print("updating membership data...")
        names_dict = dict(zip(self.tickers.ticker,self.tickers.name))
        for dt in dts:
            fname = "symbols_"+dt.date().strftime("%Y%m%d")+".csv"
            print('reading {}'.format(fname))
            syms = pd.read_csv(os.path.join(self.meta_path,self.sym_directory,fname))['symbol'].tolist()
            syms = process_tickers_convention(syms)
            for sym in syms:
                upsert_pandas(membership_maps, 'symbol', sym, 'end_date', dt, names_dict)
                
        if len(membership_maps) == 0:
            raise ValueError("empty membership data")
        
        print("checking for ticker change")
        
        ticker_change = {"old":self.tickers['relatedtickers'].tolist(),"new":self.tickers['ticker'].tolist(),"name":self.tickers['name'].tolist()}
        ticker_change = pd.DataFrame(ticker_change,columns=['old','new','name'])
        ticker_change = ticker_change.dropna()
        
        tickers_list = pd.read_csv(os.path.join(self.meta_path,self.ticker_change_file))
        tickers_list = pd.concat([tickers_list,ticker_change])
        tickers_list = tickers_list[~tickers_list.old.duplicated(keep='last')]
        membership_maps = update_ticker_change(membership_maps,tickers_list)
        
        print("updating membership complete")
        
        membership_maps.to_csv(os.path.join(self.meta_path,self.symlist_file),index=False)
        self.symlist = membership_maps
    
    def _update_bizdays_list(self, dts):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        init_dts = []
        if os.path.isfile(strpathmeta):
            init_dts = pd.read_csv(strpathmeta)
            init_dts = pd.to_datetime(init_dts['dates']).tolist()
        dts = init_dts + list(dts)
        bizdays = pd.DataFrame(sorted(set(dts)),columns=['dates'])
        bizdays.to_csv(strpathmeta,index=False)
        
    def get_bizdays(self):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        bizdays = pd.read_csv(strpathmeta)
        return pd.to_datetime(bizdays['dates'].tolist())

    def _get_quandl_data_today(self,date):
        dfr = quandl.get_table(self.quandl_table_name, date=date, ticker=",".join(self.symlist))
        return dfr 
    
    def create_csvs(self, date):
        if not if_csvs_in_dir(self.daily_path):
            dfr = pd.DataFrame(columns=['ticker','date','open','high','low','close',
                                        'volume','dividends','closeunadj',
                                        'lastupdated'])
            dfr = read_big_csv(self.download_path, self.symlist['symbol'],"SHARADAR_SEP")
            split_csvs(dfr,self.daily_path, OHLCV=False)
            dts = dfr['date']
            self._update_bizdays_list(dts)
            del dfr
        
        self.bizdays = pd.read_csv(os.path.join(self.meta_path,self.bizdays_file), parse_dates=[0])['dates'].tolist()
        start_date = pd.Timestamp(self.bizdays[-1]) + pd.Timedelta("1 days")
        end_date = pd.Timestamp(date)
        
        if not end_date >= start_date:
            print("latest data already available in csv folder")
            return
        
        date_range = {"gte":start_date.date().strftime("%Y-%m-%d"),
                          "lte":end_date.date().strftime("%Y-%m-%d")}
        print(date_range)
        dfr = quandl.get_table(self.quandl_table_name, 
                               date=date_range, ticker=",".join(self.symlist['symbol']))
        bizdays = set(dfr['date'])
        if len(bizdays) > 0:
            self._update_bizdays_list(bizdays)
            update_csvs(dfr,self.daily_path,OHLCV=False)
        print('daily csvs update completed.')
        
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
        
    def ensure_benchmark(self, date):
        if not os.path.isfile(os.path.join(self.meta_path, self.benchmark_file)):
            raise IOError("Benchmark file is missing")
        
        df0 = pd.read_csv(os.path.join(self.meta_path,
                                       self.benchmark_file),parse_dates=[0],index_col=0).sort_index()
        df0 = df0.dropna()
        last_date = pd.to_datetime(df0.index[-1])
        if date <= last_date:
            print("Benchmark file is already updated.")
            df0.to_csv(os.path.join(self.daily_path,self.benchmar_symbol+'.csv'),
                  index_label = 'date')
            return
        
        r = requests.get(
        'https://api.iextrading.com/1.0/stock/{}/chart/5y'.format(self.benchmar_symbol)
        )
        data = json.loads(r.text)
        df1 = pd.DataFrame(data)
        df1.index = pd.DatetimeIndex(df1['date'])
        df1 = df1[['open','high','low','close','volume']]
        df1 = df1.sort_index()
        
        df = pd.concat([df0,df1])
        df = df[~df.index.duplicated(keep='last')]
        df.to_csv(os.path.join(self.meta_path,self.benchmark_file),
                  index_label = 'date')
        df.to_csv(os.path.join(self.daily_path,self.benchmar_symbol+'.csv'),
                  index_label = 'date')
    
    def update_membership_maps(self):
        if not if_csvs_in_dir(self.daily_path):
            raise IOError("csv data files are not available")
            
        syms = [s.split(".csv")[0] for s in os.listdir(self.daily_path) if s.endswith(".csv")]
        membership_maps = self.symlist
        membership_maps = membership_maps[membership_maps.symbol.isin(syms)]
        self.symlist = membership_maps
        membership_maps.to_csv(os.path.join(self.meta_path,self.symlist_file),index=False)
        
    def make_adjustments_maps(self):
        if not if_csvs_in_dir(self.daily_path):
            raise IOError("csv data files are not available")
            
        files = [s for s in os.listdir(self.daily_path) if s.endswith(".csv")]
        
        splits = pd.DataFrame(columns=['effective_date','symbol','ratio'])
        divs = pd.DataFrame(columns=['ex_date','symbol','amount'])
        dts = list(self.get_bizdays())
        
        for f in files:
            s = f.split('.csv')[0]
            if s == self.benchmar_symbol:
                continue
            
            start_date = self.symlist.start_date[self.symlist.symbol==s].tolist()[0]
            end_date = self.symlist.end_date[self.symlist.symbol==s].tolist()[0]
            
            dfr = pd.read_csv(os.path.join(self.daily_path, f),
                           parse_dates=[0],
                           infer_datetime_format=True,
                           index_col=0).sort_index()
            dfr = dfr[start_date:end_date]
            
            if len(dfr) == 0:
                continue
            
            ratio = dfr['closeunadj']/dfr['close']
            dfr['ratio'] = (ratio/ratio.shift(1)).round(3)
            sdfr = dfr[dfr.ratio != 1].dropna()
            split_data = {'effective_date':sdfr.index,'symbol':[s]*len(sdfr),'ratio':sdfr.ratio}
            split_data = pd.DataFrame(split_data,columns=['effective_date','symbol','ratio'])
            splits = pd.concat([splits,split_data])
            
            ddfr = dfr[dfr.dividends != 0].dropna()
            div_data = {'ex_date':ddfr.index,'symbol':[s]*len(ddfr),'amount':ddfr.dividends}
            div_data = pd.DataFrame(div_data,columns=['ex_date','symbol','amount'])
            divs = pd.concat([divs,div_data])
            
        splits.effective_date = pd.to_datetime(splits.effective_date)
        divs.ex_date = pd.to_datetime(divs.ex_date)
        
        divs['declared_date'] = [dts[max(0,dts.index(e)-1)] for e in list(divs.ex_date)]
        divs['record_date'] = [dts[min(len(dts)-1,dts.index(e)+2)] for e in list(divs.ex_date)]
        divs['pay_date'] = divs['record_date']
        
        divs.declared_date = pd.to_datetime(divs.ex_date)
        divs.record_date = pd.to_datetime(divs.ex_date)
        divs.pay_date = pd.to_datetime(divs.ex_date)
        
        splits = splits.sort_index()
        divs = divs.sort_index()
        
        splits.to_csv(os.path.join(self.meta_path,'splits.csv'),index=False)
        divs.to_csv(os.path.join(self.meta_path,'dividends.csv'),index=False)
        
    def register_bundle(self):
        dts = (self.get_bizdays()).tz_localize(self.calendar_tz)
        register(self.bundle_name, sep_equities(self.config_path),calendar_name=self.calendar_name,
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
        self.create_csvs(date)
        self.ensure_data_range()
        self.update_membership_maps()
        self.make_adjustments_maps()
        self.ensure_benchmark(date)
        self.call_ingest()

#config_path = "C:/Users/academy.academy-72/Desktop/dev platform/data/SEP/meta/config.json"
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

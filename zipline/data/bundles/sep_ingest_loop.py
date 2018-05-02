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

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.data import bundles as bundles_module
from zipline.data.bundles import register
from zipline.data.bundles.SEP import sep_equities
from zipline.data.bundles.ingest_utilities import read_big_csv,if_csvs_in_dir,split_csvs,update_csvs, clean_up, find_interval, upsert_pandas

def process_tickers_convention(tickers):
    tickers = [t.replace("_","") for t in tickers]
    return tickers
    

class IngestLoop:
    
    def __init__(self, configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            quandl.ApiConfig.api_key = config["QUANDL_API_KEY"]
            self.quandl_table_name = config["QUANDL_TABLE_NAME"]
            self.meta_path=config["META_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.download_path=config["DOWNLOAD_PATH"]
            self.bizdays_file=config["BIZDAYLIST"]
            self.wiki_url=config["WIKI_URL"]
            self.sym_directory=config["SYM_DIRECTORY"]
            self.sym_file=config["SYMLIST"]
            self.calendar_name=config["CALENDAR_NAME"]
            self.calendar_tz=config["CALENDAR_TZ"]

    def _read_symlist(self,strpath):
        tickers = pd.read_csv(strpath)['symbol'].tolist()
        tickers = process_tickers_convention(tickers)
        return tickers
    
    def _get_quandl_data_today(self,date):
        dfr = quandl.get_table(self.quandl_table_name, date=date, ticker=",".join(self.symlist))
        return dfr
    
    def ensure_latest_sym_list(self,date):        
        pass
        
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
        names_dict = dict(zip(self.tickers.symbol,self.tickers.name))
        for dt in dts:
            fname = "symbols_"+dt.date().strftime("%Y%m%d")+".csv"
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
    
    def _update_bizdays_list(self, dts):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        init_dts = []
        if os.path.isfile(strpathmeta):
            init_dts = pd.read_csv(strpathmeta)
            init_dts = pd.to_datetime(init_dts['dates']).tolist()
        dts = init_dts + list(dts)
        bizdays = pd.DataFrame(sorted(set(dts)),columns=['dates'])
        bizdays.to_csv(strpathmeta,index=False)

    def _update_csvs(self, date):
        if not if_csvs_in_dir(self.daily_path):
            dfr = pd.DataFrame(columns=['ticker','date','open','high','low','close',
                                        'volume','dividends','closeunadj',
                                        'lastupdated'])
            dfr = read_big_csv(self.download_path, self.symlist,"SHARADAR_SEP")
            split_csvs(dfr,self.daily_path)
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
                               date=date_range, ticker=",".join(self.symlist))
        bizdays = set(dfr['date'])
        if len(bizdays) > 0:
            self._update_bizdays_list(bizdays)
        update_csvs(dfr,self.daily_path)
        
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
    
    def run(self, date):
        self.ensure_latest_sym_list(date)
        self.ensure_membership_maps()
        self.ensure_latest_data(date)
        self.create_csvs(date)
        self.ensure_benchmark(date)
        self.call_ingest()

config_path = "C:/Users/academy.academy-72/Desktop/dev platform/data/SEP/meta/config.json"
ingest_loop = IngestLoop(config_path)









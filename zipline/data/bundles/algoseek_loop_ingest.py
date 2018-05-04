# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 10:22:36 2018

@author: Prodipta
"""
import sys
import pandas as pd
import os
import datetime
import requests
import json

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.data.bundles import register
from zipline.data.bundles.algoseek import algoseek_minutedata
from zipline.data.bundles.ingest_utilities import touch, unzip_to_directory, clean_up, download_spx_changes

from zipline.data import bundles as bundles_module

class IngestLoop:
    
    def __init__(self,configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            self.config_path = configpath
            self.bundle_name=config["BUNDLE_NAME"]
            self.input_path=config["INPUT_PATH"]
            self.data_path=config["DATA_PATH"]
            self.meta_path=config["META_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.benchmar_symbol=config["BENCHMARK_SYM"]
            self.benchmark_file=config["BENCHMARKDATA"]
            self.bizdays_file=config["BIZDAYLIST"]
            self.symlist_file=config["SYMLIST"]
            self.calendar_name=config["CALENDAR_NAME"]
            self.sym_directory=config["SYM_DIRECTORY"]
            self.wiki_url=config["WIKI_URL"]
            self.spx_data = download_spx_changes(self.wiki_url)
    
    def update_benchmark(self):
        if not os.path.isfile(os.path.join(self.meta_path, self.benchmark_file)):
            raise IOError("Benchmark file is missing")
        
        df0 = pd.read_csv(os.path.join(self.meta_path,
                                       self.benchmark_file),parse_dates=[0],index_col=0).sort_index()
        df0 = df0.dropna()
        
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

    def update_bizdays(self, strdate):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        dts = []
        if os.path.isfile(strpathmeta):
            dts = pd.read_csv(strpathmeta)
            dts = pd.to_datetime(dts['dates']).tolist()
        dts = dts+ [pd.to_datetime(strdate,format='%Y%m%d')]
        bizdays = pd.DataFrame(sorted(set(dts)),columns=['dates'])
        bizdays.to_csv(strpathmeta,index=False)
        
    def _validate_dropouts(self, syms, spx_changes, cutoff=10):
        if not syms:
            return True
        
        if not spx_changes:
            spx_changes = download_spx_changes(self.wiki_url)
            
        current_sym_list = spx_changes['tickers']['symbol'].tolist()
        deleted_sym_list = spx_changes['change']['delete'].tolist()
        added_sym_list = spx_changes['change']['add'].tolist()
        
        validation_exists = [True if s in current_sym_list else False for s in syms]
        validation_added = [True if s in added_sym_list else False for s in syms]
        validation_deleted = [False if s in deleted_sym_list else True for s in syms]
        
        if len(syms) > cutoff:
            validation_results = [validation_exists[i] or validation_added[i] or validation_deleted[3] for i,e in enumerate(syms)]
        else:
            validation_results = [validation_exists[i] or validation_added[i] for i,e in enumerate(syms)]
        return validation_results
    
    def manage_symlist(self, symbols, date):
        fname = 'symbols_'+date+'.csv'
        
        if not os.path.isfile(os.path.join(self.meta_path,self.symlist_file)):
            pd.DataFrame(symbols,columns=['symbol']).to_csv(os.path.join(self.meta_path,self.symlist_file),index=False)
            pd.DataFrame(symbols,columns=['symbol']).to_csv(os.path.join(self.meta_path,self.sym_directory,fname),index=False)
            return
        
        symlist = pd.read_csv(os.path.join(self.meta_path,self.symlist_file))
        symlist = symlist['symbol'].tolist()
        extra_syms = [s for s in symbols if s not in symlist]
        print("extra symbols {}".format(extra_syms))
        missing_syms = [s for s in symlist if s not in symbols]
        print("missing symbols {}".format(missing_syms))
        
        if missing_syms and len(missing_syms) - len(extra_syms) > 1:
            spx_tickers = self.spx_data['tickers']
            change_data = self.spx_data['change']
            change_data = change_data[change_data['date'] <= pd.to_datetime(date)+pd.Timedelta(days=30)]
            spx_data = {"tickers":spx_tickers , "change":change_data}
            validated_syms = self._validate_dropouts(missing_syms,spx_data)
            #print(dict(zip(missing_syms,validated_syms)))
            print("validattion {}".format(validated_syms))
            for i, s in enumerate(missing_syms):
                if validated_syms[i]:
                    touch(s+".csv",self.data_path)
                    symbols.append(s)
        
        symlist = symbols
        pd.DataFrame(symlist,columns=['symbol']).to_csv(os.path.join(self.meta_path,self.symlist_file),index=False)
        pd.DataFrame(symbols,columns=['symbol']).to_csv(os.path.join(self.meta_path,self.sym_directory,fname),index=False)
        
    def register_bundle(self, start_date, end_date):
        register(self.bundle_name, algoseek_minutedata(self.config_path),calendar_name=self.calendar_name,
                 start_session=None,end_session=None,
                 create_writers=False)
    
    def call_ingest(self, start_date, end_date):
        self.register_bundle(start_date, end_date)
        bundles_module.ingest(self.bundle_name,os.environ,pd.Timestamp.utcnow())
    
    def run(self, start_date, end_date):
        delta = end_date - start_date
        dts = [(start_date + datetime.timedelta(days=x)).strftime('%Y%m%d') for x in range(0, delta.days+1)]
        all_files = os.listdir(self.input_path)
    
        for dt in dts:
            files = [fname for fname in all_files if dt in fname]
            files = list(set(files))
            if(files):
                print("{}:{}".format(dt,"-"*40))
                self.update_bizdays(dt)
                if len(files) > 1:
                    raise ValueError('Expected no more than one archive file')
                for f in files:
                    full_fname = os.path.join(self.input_path, f)
                    unzip_to_directory(full_fname, self.data_path)
                    sfiles = os.listdir(self.data_path)
                    symbols = [s.split('.csv')[0] for s in sfiles if s.endswith('.csv')]
                    self.manage_symlist(symbols, dt)
                    print("calling ingest function...")
                    self.call_ingest(start_date,end_date)
                    print("done, cleaning up...")
                    clean_up(self.data_path)
    

def main():
    assert len(sys.argv) == 4, (
            'Usage: python {} <start_date>'
            ' <end_date> <path_to_config>'.format(os.path.basename(__file__)))
        
    start_date = pd.Timestamp(sys.argv[1],tz='Etc/UTC')
    end_date = pd.Timestamp(sys.argv[2],tz='Etc/UTC')
    config_file = sys.argv[3]
    
    ingest_looper = IngestLoop(config_file)
    ingest_looper.run(start_date, end_date)


if __name__ == "__main__":
    main()






# -*- coding: utf-8 -*-
"""
Created on Fri Apr 06 18:07:12 2018

@author: Prodipta
"""

import pandas as pd
import os
import datetime
import json
import sys
import bisect
import requests
from StringIO import StringIO
import zipfile
import shutil

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.data.bundles import register
from zipline.data.bundles.gdfl import gdfl_minutedata
from zipline.data.bundles.ingest_utilities import clean_up, touch

from zipline.data import bundles as bundles_module

def ticker_cleanup(s):
    try:
        s = s.replace("-EQ","")
        s = s.replace(".CM","")
        s = s.replace(".NFO","")
        s = s.replace(".NSE_IDX","")
        s = s.replace(" ","")
        s = s.replace(".NSE","")
    except:
        s = 'missing'
    return s

def split_csvs(dfr, strpath):
    syms = set(dfr['Ticker'].tolist())
    for s in syms:
        dfs = dfr.loc[dfr['Ticker']==s]
        dfs.to_csv(os.path.join(strpath,s+".csv"), index=False)

def get_latest_symlist(date):
    symbols = []
    syms = []
    x = pd.to_datetime(date,format="%d%m%Y")
    year = x.date().year
    month = x.date().strftime('%b').upper()
    fname = 'fo{}bhav.csv'.format(x.date().strftime('%d%b%Y').upper())
    url = 'https://www.nseindia.com/content/historical/DERIVATIVES/{}/{}/{}.zip'.format(year,month,fname)
    try:
        r = requests.get(url, stream=True)
        zipdata = StringIO()
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                zipdata.write(chunk)
        myzipfile = zipfile.ZipFile(zipdata)
        bhavcopy = pd.read_csv(myzipfile.open(fname))
        myzipfile.close()
        syms = list(set(bhavcopy[bhavcopy.INSTRUMENT=='FUTSTK']['SYMBOL'].tolist()))
    except:
        pass

    if syms:
        futsyms = [s+"-I" for s in syms]
        idxsyms = ['NIFTY50','NIFTYBANK','INDIAVIX','NIFTY-I','NIFTY-II','BANKNIFTY-I', 'BANKNIFTY-II']
        all_syms = idxsyms + syms + futsyms
        names = all_syms
        types = ['IDX']*3 + ['FUT']*4 + ['STK']*len(syms) + ['FUT']*len(futsyms)
        underlyings = ['IDX']*7 + ['STK']*len(syms) + ['STK']*len(futsyms)
        symbols = {'symbol':all_syms,'name':names,'type':types,'underlying':underlyings}
        symbols = pd.DataFrame.from_dict(symbols)

    return symbols

class IngestLoop:

    def __init__(self, configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            self.config_path = configpath
            self.bundle_name=config["BUNDLE_NAME"]
            self.input_path=config["INPUT_PATH"]
            self.data_path=config["DATA_PATH"]
            self.meta_path=config["META_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.benchmar_symbol=config["BENCHMARK_SYM"]
            self.bizdays_file=config["BIZDAYLIST"]
            self.symlist_file=config["SYMLIST"]
            self.sym_directory=config["SYM_DIRECTORY"]
            self.calendar_name=config["CALENDAR_NAME"]

    def _read_sym_list(self, strpathmeta=None):
        if not strpathmeta:
            strpathmeta = os.path.join(self.meta_path,self.symlist_file)
        if not os.path.isfile(strpathmeta):
            raise ValueError('Allow syms list is missing')
        else:
            syms = pd.read_csv(strpathmeta)['symbol'].tolist()
        return syms

    def make_latest_sym_list(self,date):
        destfile = os.path.join(self.meta_path,self.symlist_file)
        try:
            os.remove(destfile)
        except:
            pass

        dts = [dt.split(".csv")[0] for dt in os.listdir(os.path.join(self.meta_path,self.sym_directory))]
        dts = pd.to_datetime(sorted(dts))
        ndts = [d.value/1E9 for d in dts]
        ndate = pd.to_datetime(date,format="%d%m%Y").value/1E9
        try:
            idx = ndts.index(ndate)
        except:
            idx = max(bisect.bisect_left(ndts,ndate)-1,0)
        if ndate < ndts[-1]:
            symfile = datetime.datetime.strftime(dts[idx], '%Y%m%d')+".csv"
            shutil.copyfile(os.path.join(self.meta_path,self.sym_directory,symfile),destfile)
        else:
            symbols = get_latest_symlist(date)
            if not symbols.empty:
                symfile = pd.to_datetime(date,format='%d%m%Y').date().strftime('%Y%m%d')+".csv"
                symbols.to_csv(destfile, index=False)
                symbols.to_csv(os.path.join(self.meta_path,self.sym_directory,symfile), index=False)

    def update_bizdays(self, strdate):
        strpathmeta = os.path.join(self.meta_path,self.bizdays_file)
        dts = []
        if os.path.isfile(strpathmeta):
            dts = pd.read_csv(strpathmeta)
            dts = pd.to_datetime(dts['dates']).tolist()
        dts = dts+ [pd.to_datetime(strdate,format='%d%m%Y')]
        bizdays = pd.DataFrame(sorted(set(dts)),columns=['dates'])
        bizdays.to_csv(strpathmeta,index=False)

    def manage_symlist(self, symbols):
        symlist = self.symlist
        missing_syms = [s for s in symlist if s not in symbols]
        print("missing symbols {}".format(missing_syms))
        for s in missing_syms:
            touch(s+".csv",self.data_path)

    def update_corporate_actions(self, strdate):
        pass

    def register_bundle(self, start_date, end_date):
        register(self.bundle_name, gdfl_minutedata(self.config_path),calendar_name=self.calendar_name,
                 start_session=None,end_session=None,
                 create_writers=False)

    def call_ingest(self, start_date, end_date):
        self.register_bundle(start_date, end_date)
        bundles_module.ingest(self.bundle_name,os.environ,pd.Timestamp.utcnow())


    def run(self, start_date, end_date):
        valid_cols = ['Ticker','Date','Time','Open','High','Low','Close','Volume','Open Interest']
        delta = end_date - start_date
        dts = [(start_date + datetime.timedelta(days=x)).strftime('%d%m%Y') for x in range(0, delta.days+1)]
        all_files = os.listdir(self.input_path)

        for dt in dts:
            files = [f for f in all_files if dt in f and f.endswith(".csv")]
            files = list(set(files))
            if(files):
                print("{}:{}".format(dt,"-"*40))
                assert len(files) > 2 or len(files) < 5, (
                        'Expected between 3 to 4 csv files'
                        'got {}.\n'
                        'files are {}\n'.format(len(files),files)
                        )
                self.update_bizdays(dt)
                self.make_latest_sym_list(dt)
                self.symlist = self._read_sym_list()
                self.update_corporate_actions(dt)
                for f in files:
                    if 'MCX' in f:
                        continue
                    full_fname = os.path.join(self.input_path, f)
                    dfr = pd.read_csv(full_fname).iloc[:,0:9]
                    dfr.columns = valid_cols
                    dfr['Ticker'] = dfr['Ticker'].apply(ticker_cleanup)
                    dfr = dfr[dfr.Ticker.isin(self.symlist)]
                    split_csvs(dfr,self.data_path)

                sfiles = os.listdir(self.data_path)
                symbols = [s.split('.csv')[0] for s in sfiles if s.endswith('.csv')]
                self.manage_symlist(symbols)
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

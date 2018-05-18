# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 12:51:03 2018

@author: Prodipta
"""

import os
import sys
from os import listdir
from os.path import isfile, join
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import datetime
import json

from logbook import Logger, StreamHandler
from pandas import read_csv, Timedelta

from zipline.utils.calendars import deregister_calendar, get_calendar, register_calendar
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendars import ExchangeCalendarFromDate
from zipline.data.minute_bars import BcolzMinuteBarWriter, BcolzMinuteOverlappingData
from zipline.assets import AssetDBWriter
from zipline.data.us_equity_pricing import BcolzDailyBarWriter, SQLiteAdjustmentWriter, BcolzDailyBarReader
from zipline.assets.asset_db_schema import asset_db_table_names


from . import core as bundles

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)


def algoseek_minutedata(configpath=None):
    """
    Generate an ingest function for custom data bundle
    This function can be used in ~/.zipline/extension.py
    to register bundle with custom parameters, e.g. with
    a custom trading calendar.

    Parameters
    ----------
    tframes: tuple, optional
        The data time frames, supported timeframes: 'daily' and 'minute'
    csvdir : string, optional, default: CSVDIR environment variable
        The path to the directory of this structure:
        <directory>/<timeframe1>/<symbol1>.csv
        <directory>/<timeframe1>/<symbol2>.csv
        <directory>/<timeframe1>/<symbol3>.csv
        <directory>/<timeframe2>/<symbol1>.csv
        <directory>/<timeframe2>/<symbol2>.csv
        <directory>/<timeframe2>/<symbol3>.csv

    Returns
    -------
    ingest : callable
        The bundle ingest function

    Examples
    --------
    This code should be added to ~/.zipline/extension.py
    .. code-block:: python
       from zipline.data.bundles import csvdir_equities, register
       register('custom-csvdir-bundle',
                csvdir_equities(["daily", "minute"],
                '/full/path/to/the/csvdir/directory'))
    """

    return CSVDIRBundleALGOSEEK(configpath).ingest


class CSVDIRBundleALGOSEEK:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """
    def _read_config(self, configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            self.data_path=config["DATA_PATH"]
            self.meta_path=config["META_PATH"]
            self.bundle_path=config["BUNDLE_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.asset_db_name=config["ASSET_DB"]
            self.adjustment_db_name=config["ADJUSTMENT_DB"]
            self.metadata_file=config["META_DATA"]
            self.bizdays_file=config["BIZDAYLIST"]
            self.symdata_file=config["SYMDATA"]
            self.symlist_file=config["SYMLIST"]
            self.benchmark_file=config["BENCHMARKDATA"]
            self.benchmar_symbol=config["BENCHMARK_SYM"]
            self.calendar_name=config["CALENDAR_NAME"]
            self.calendar_tz=config["CALENDAR_TZ"]
            self.cal_session_start=config["SESSION_START"]
            self.cal_session_end=config["SESSION_END"]
            self.cal_minutes_per_day=config["MINUTES_PER_DAY"]

    def __init__(self, configpath=None):
        self._read_config(configpath)
        self.bizdays = self._read_bizdays(join(self.meta_path,self.bizdays_file))
        self.calendar = self._create_calendar(
                self.calendar_name,
                self.calendar_tz,
                tuple(self.cal_session_start),
                tuple(self.cal_session_end),
                self.bizdays)
        self.minute_bar_path = join(self.bundle_path,"minute")
        self.daily_bar_path = join(self.bundle_path,"daily")
        self.asset_db_path = join(self.bundle_path,self.asset_db_name)
        self.adjustment_db_path = join(self.bundle_path,self.adjustment_db_name)
        self.benchmark_data = self._read_benchmark_data(join(self.meta_path,self.benchmark_file))
        self.meta_data = self._read_asset_db()
        self.syms = self._read_allowed_syms(join(self.meta_path,self.symdata_file))

    def _read_benchmark_data(self,strpath):
        if not isfile(strpath):
            raise ValueError('Benchmark data missing')
        df = pd.read_csv(strpath,parse_dates=[0],index_col=0).sort_index()
        df = df[~df.index.duplicated(keep='last')]
        return df

    def _read_allowed_syms(self, strpathmeta):
        if not isfile(strpathmeta):
            raise ValueError('Allowed syms list missing')
        else:
            syms = read_csv(strpathmeta)
        syms.loc[syms["Exchange"] == "DELISTED","Exchange"] = syms.loc[syms["Exchange"] == "DELISTED","Delisted.From"]
        return syms

    def _read_bizdays(self, strpathmeta):
        if not isfile(strpathmeta):
            raise ValueError('Business days list missing')
        else:
            dts = read_csv(strpathmeta)
            dts = pd.to_datetime(dts['dates']).tolist()
        return sorted(set(dts))

    def _create_calendar(self, cal_name,tz,session_start,session_end,dts):
        cal = ExchangeCalendarFromDate(cal_name,tz,session_start,session_end,dts)
        try:
            deregister_calendar(self.calendar_name)
            get_calendar(self.calendar_name)
        except:
            register_calendar(self.calendar_name, cal)
        return get_calendar(self.calendar_name)


    def _read_asset_db(self):
        meta_data = pd.DataFrame(columns=['symbol','asset_name','start_date',
                                          'end_date','auto_close_date',
                                          'exchange'])

        query = ("SELECT equity_symbol_mappings.symbol, equities.asset_name, "
                 "equities.start_date, equities.end_date, equities.auto_close_date, "
                 "equities.exchange from equities INNER JOIN equity_symbol_mappings "
                 "ON equities.sid = equity_symbol_mappings.sid")

        engine = sa.create_engine('sqlite:///' + self.asset_db_path)
        conn = engine.connect()
        table_exists = all(engine.dialect.has_table(conn,t) for t in asset_db_table_names)
        if table_exists:
            meta_data = pd.read_sql_query(query, conn)
        else:
            # make sure we do have the benchmark in asset table at SID 0
            start_date = self.bizdays[0]
            end_date = self.bizdays[-1]
            ac_date = (end_date + Timedelta(days=1))
            meta_data.loc[0] = self.benchmar_symbol,"", start_date.value,end_date.value,ac_date.value, ""
        return(meta_data)

    def ingest(self,
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):

        self.calendar = self._create_calendar(
                self.calendar_name,
                self.calendar_tz,
                self.cal_session_start,
                self.cal_session_end,
                self._read_bizdays(join(self.meta_path,self.bizdays_file)))

        algoseek_bundle(environ,
                      asset_db_writer,
                      minute_bar_writer,
                      daily_bar_writer,
                      adjustment_writer,
                      self.calendar,
                      start_session,
                      end_session,
                      cache,
                      show_progress,
                      output_dir,
                      self.data_path,
                      self.minute_bar_path,
                      self.daily_bar_path,
                      self.asset_db_path,
                      self.adjustment_db_path,
                      self.meta_data,
                      self.meta_path,
                      self.syms,
                      self.bizdays,
                      self.cal_minutes_per_day,
                      self.benchmar_symbol,
                      self.daily_path,
                      self.benchmark_data)


@bundles.register("ALGOSEEK",create_writers=False)
def algoseek_bundle(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir,
                  csvdir = None,
                  minute_bar_path = None,
                  daily_bar_path = None,
                  asset_db_path = None,
                  adjustment_db_path = None,
                  meta_data = None,
                  meta_path = None,
                  syms = None,
                  bizdays = None,
                  minutes_per_day = None,
                  benchmark_symbol = None,
                  save_daily_path = None,
                  benchmark_data = None):
    """
    Build a zipline data bundle from the directory with csv files.
    """
    if not csvdir:
        raise ValueError("data input directory missing")

    if not os.path.isdir(csvdir):
        raise ValueError("%s is not a directory" % csvdir)

    if meta_data is None:
        raise ValueError("meta data is missing")

    minute_bar_writer = BcolzMinuteBarWriter(minute_bar_path,
                                             calendar,
                                             calendar.all_sessions[0],
                                             calendar.all_sessions[-1],
                                             minutes_per_day,
                                             benchmark_symbol)
    daily_bar_writer = BcolzDailyBarWriter(daily_bar_path,
                                             calendar,
                                             calendar.all_sessions[0],
                                             calendar.all_sessions[-1])
    asset_db_writer = AssetDBWriter(asset_db_path)

    #write the benchmark data to save_daily_path to ingest
    daily_benchmark = get_equal_sized_df(benchmark_data,bizdays)
    assert len(daily_benchmark) == len(bizdays), (
            'Benchmark data does not match with expected number. '
            'Got {} rows, expected {}'.format(len(daily_benchmark),
                 len(bizdays)))
    daily_benchmark.to_csv(join(save_daily_path,benchmark_symbol+".csv"))
    todays_benchmark = daily_benchmark.tail(1)
    
    
    try:
        minute_bar_writer.write(_minute_data_iter(csvdir, meta_data,calendar, 
                                                  syms, bizdays,"NYSE",
                                                  save_daily_path,
                                                  todays_benchmark),
                 show_progress=show_progress)
    except BcolzMinuteOverlappingData:
        pass

    daily_bar_writer.write(_pricing_iter(save_daily_path, meta_data['symbol'].tolist(),
                                         show_progress),show_progress=show_progress)

    #print("the last bizday is {}".format(bizdays[-1]))
    meta_data.loc[meta_data['symbol']==benchmark_symbol,'end_date'] = bizdays[-1].value
    meta_data.loc[meta_data['symbol']==benchmark_symbol,'auto_close_date'] = (bizdays[-1]+Timedelta(days=1)).value
    _write_meta_data(asset_db_writer,asset_db_path, meta_data)
    _write_adjustment_data(adjustment_db_path,meta_data,syms,daily_bar_path,
                           calendar.all_sessions, bizdays, meta_path)


def _write_meta_data(asset_db_writer,asset_db_path,meta_data):
    try:
        os.remove(asset_db_path)
    except:
        pass

    asset_db_writer.write(equities=meta_data)

def _write_adjustment_data(adjustment_db_path,meta_data,syms,daily_bar_path,
                           cal_sessions, bizdays, meta_path):
    try:
        os.remove(adjustment_db_path)
    except:
        pass

    adjustment_writer = SQLiteAdjustmentWriter(adjustment_db_path,
                                               BcolzDailyBarReader(daily_bar_path),
                                               cal_sessions,
                                               overwrite=True)

#    divs_splits = {'divs': pd.DataFrame(columns=['sid', 'amount',
#                                              'ex_date', 'record_date',
#                                              'declared_date', 'pay_date']),
#                   'splits': pd.DataFrame(columns=['sid', 'ratio',
#                                                'effective_date'])}
#    divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
#    divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
#    adjustment_writer.write(splits=divs_splits['splits'],
#                                dividends=divs_splits['divs'])
#    return

    first_available_day = bizdays[0]
    last_available_day = bizdays[-1]
    meta_dict = dict(zip(meta_data['symbol'].tolist(),range(len(meta_data))))

    splits = pd.read_csv(join(meta_path,"splits.csv"),parse_dates=[0])
    splits = splits[splits['symbol'].isin(syms['Ticker'])]
    splits['effective_date'] = pd.to_datetime(splits['effective_date'])
    splits['sid'] = [meta_dict.get(sym, -1) for sym in splits['symbol'].tolist()]
    splits = splits[splits['effective_date'] > first_available_day]
    splits = splits[splits['effective_date'] <= last_available_day]
    splits =splits.drop(['symbol'],axis=1)
    splits = splits[splits['sid'] != -1]

    dividends = pd.read_csv(join(meta_path,"dividends.csv"),parse_dates=[0])
    dividends = dividends[dividends['symbol'].isin(syms['Ticker'])]
    dividends['ex_date'] = pd.to_datetime(dividends['ex_date'])
    dividends['declared_date'] = pd.to_datetime(dividends['declared_date'])
    dividends['pay_date'] = pd.to_datetime(dividends['pay_date'])
    dividends['record_date'] = pd.to_datetime(dividends['record_date'])
    dividends['sid'] = [meta_dict.get(sym, -1) for sym in dividends['symbol'].tolist()]
    dividends = dividends[dividends['ex_date'] > first_available_day]
    dividends = dividends[dividends['ex_date'] <= last_available_day]
    dividends =dividends.drop(['symbol'],axis=1)
    dividends = dividends[dividends['sid'] != -1]

    adjustment_writer.write(splits=splits,
                            dividends=dividends)

def _pricing_iter(csvdir, symbols, show_progress):
    with maybe_show_progress(symbols, show_progress,
                             label='Loading custom pricing data: ') as it:
        files = os.listdir(csvdir)
        for sid, symbol in enumerate(it):
            logger.debug('%s: sid %s' % (symbol, sid))

            try:
                fname = [fname for fname in files
                         if '%s.csv' % symbol in fname][0]
            except IndexError:
                raise ValueError("%s.csv file is not in %s" % (symbol, csvdir))

            dfr = read_csv(os.path.join(csvdir, fname),
                           parse_dates=[0],
                           infer_datetime_format=True,
                           index_col=0).sort_index()

            yield sid, dfr

def _minute_data_iter(data_path,meta_data,calendar, syms, bizdays,
                      exchange,save_daily_path, todays_benchmark):
    dateparse = lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d')
    files = listdir(data_path)
    symbols = [f.split('.csv')[0] for f in files if f.endswith('.csv')]
    print("total tickers {}".format(len(symbols)))
    current_session = bizdays[-1]
    idx = calendar.minutes_for_sessions_in_range(current_session,current_session)
    names_dict = dict(zip(syms['Ticker'],syms['Name']))
    exchange_dict = dict(zip(syms['Ticker'],syms['Exchange']))
    
    # we know benchmark is always at SID = 0
    # expand the data to whole minutes in the session and yield
    todays_benchmark.index = pd.to_datetime([idx[0]])
    dfr = get_equal_sized_df(todays_benchmark,idx)
    yield 0, dfr

    try:
        meta_dict = meta_data['symbol'].to_dict()
        meta_dict = {s:i for i,s in meta_dict.iteritems()}
    except:
        meta_dict = {}

    for s in symbols:
        #print(s)
        try:
            dfr = pd.read_csv(os.path.join(data_path, s+".csv"),converters={ 'Date': dateparse })
            dfr = fixup_minute_df(dfr, calendar)
            dfr = dfr[~dfr.index.duplicated(keep='last')]
            dfr = get_equal_sized_df(dfr,idx)
            if(len(dfr)==0):
                print("{} moves out?".format(s))
                dfr = make_dummy_df(s,idx,save_daily_path)
        except pd.io.common.EmptyDataError:
            print("carrying over last day prices for {}".format(s))
            dfr = make_dummy_df(s,idx,save_daily_path)

        if len(dfr) == 0:
            print("Failed to carry over last day prices for {}".format(s))
            continue

        if s in meta_dict:
            sid = meta_dict[s]
            if meta_data.loc[sid,'start_date'] > current_session.value:
                meta_data.loc[sid,'start_date'] = current_session.value
            if meta_data.loc[sid,"end_date"] < current_session.value:
                meta_data.loc[sid,"end_date"] = current_session.value
                meta_data.loc[sid,"auto_close_date"] = (current_session + Timedelta(days=1)).value
        else:
            sid = len(meta_data)
            meta_data.loc[sid] = s, names_dict.get(s,s), current_session.value,current_session.value,(current_session + Timedelta(days=1)).value, exchange_dict.get(s,"")

        save_as_daily(join(save_daily_path,s+".csv"),dfr)

        yield sid, dfr


def ticker_cleanup(s):
    return s

def fixup_minute_df(data, calendar):
    ticker_col = np.where(data.columns.to_series().str.lower().str.contains('ticker') == True)[0][0]
    dt_col = np.where(data.columns.to_series().str.lower().str.contains('date') == True)[0][0]
    time_col = np.where(data.columns.to_series().str.lower().str.contains('time') == True)[0][0]
    vwap_col = np.where(data.columns.to_series().str.lower().str.contains('volumeweightprice') == True)[0][0]
    trade_col = np.where(data.columns.to_series().str.lower().str.contains('totaltrades') == True)[0][0]
    idx = pd.to_datetime(data.iloc[:,dt_col] + " " + data.iloc[:,time_col])
    data = data.set_index(idx)
    data.index = data.index.tz_localize(calendar.tz)
    dropcols = [dt_col,ticker_col,time_col,vwap_col,trade_col]
    data = data.drop(data.columns[dropcols],axis=1)
    data = data.rename(columns={
            'FirstTradePrice': 'open',
            'HighTradePrice': 'high',
            'LowTradePrice': 'low',
            'LastTradePrice': 'close',
            'Volume': 'volume'
            })
    data = data.sort_index().drop_duplicates()
    return data

def get_equal_sized_df(dfr, idx):
    base_dfr = pd.DataFrame(columns=['open','high','low','close','volume'],index = idx)
    valid_idx = base_dfr.index.intersection(dfr.index)
    base_dfr.loc[valid_idx] = dfr.loc[valid_idx]
    dfr = base_dfr.fillna(method = "ffill")
    dfr = dfr.fillna(method = "bfill")
    dfr = dfr.dropna()
    return dfr

def check_sym(s,syms):
    return True
    #syms = syms['Ticker'].tolist()
    #return True if s in syms else False


def save_as_daily(strpath,df):
    df = df.resample('1D').agg({'open': 'first',
               'high': 'max',
               'low': 'min',
               'close': 'last',
               'volume':'sum'})

    df = df[['open','high','low','close','volume']]
    if not isfile(strpath):
        ddf = df
    else:
        ddf = read_csv(strpath, index_col=0, parse_dates = True).sort_index()
        ddf = ddf.append(df)
        ddf = ddf[~ddf.index.duplicated(keep='last')]

    ddf.to_csv(strpath)

def make_dummy_df(sym, idx, datapath):
    fname = sym+".csv"
    try:
        dfr = pd.read_csv(os.path.join(datapath,fname),
                      parse_dates=[0],index_col=0).sort_index()
    except:
        raise ValueError('File not found to make carried-over dataframe')
    dfr['volume'].iloc[-1] = 0
    backdata = tuple(dfr.iloc[-1,:])
    base_dfr = pd.DataFrame(columns=['open','high','low','close','volume'],index = idx)
    base_dfr.iloc[0,:] = backdata
    dfr = base_dfr.fillna(method = "ffill")
    dfr = dfr.fillna(method = "bfill")
    dfr = dfr.dropna()
    return dfr


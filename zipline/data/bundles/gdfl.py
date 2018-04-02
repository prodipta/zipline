"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys
from os import listdir
from os.path import isfile, join
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import datetime

from logbook import Logger, StreamHandler
#from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta, NaT
#from contextlib2 import ExitStack

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

GDFL_DATA_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/GDFL/current/inputs"
GDFL_META_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/GDFL/meta"
GDFL_BUNDLE_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/GDFL/bundle"
GDFL_DAILY_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/GDFL/current/symbols/daily"
ASSET_DB = "assets-6.sqlite"
ADJUSTMENT_DB = "adjustments.sqlite"
META_DATA = "assets.csv"
GDFL_BIZDAYLIST = "bizdays.csv"
GDFL_SYMLIST = "symbols.csv"
GDFL_CALENDAR_NAME = "GDFL"
GDFL_CALENDAR_TZ = "Etc/UTC"
GDFL_SESSION_START = (9,15,59)
GDFL_SESSION_END = (15,29,59)
GDFL_MINUTES_PER_DAY = 375


def gdfl_minutedata(csvdir=GDFL_DATA_PATH):
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

    return CSVDIRBundleGDFL(csvdir).ingest


class CSVDIRBundleGDFL:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, csvdir=None):
        self.csvdir = csvdir
        self.bundledir = GDFL_BUNDLE_PATH
        self.metapath = GDFL_META_PATH
        self.calendar = self._create_calendar(
                GDFL_CALENDAR_NAME,
                GDFL_CALENDAR_TZ,
                GDFL_SESSION_START,
                GDFL_SESSION_END,
                self._read_bizdays(join(self.metapath,GDFL_BIZDAYLIST),self.csvdir))
        self.minute_bar_path = join(GDFL_BUNDLE_PATH,"minute")
        self.daily_bar_path = join(GDFL_BUNDLE_PATH,"daily")
        self.asset_db_path = join(GDFL_BUNDLE_PATH,ASSET_DB)
        self.adjustment_db_path = join(GDFL_BUNDLE_PATH,ADJUSTMENT_DB)
        self.meta_data = self._read_asset_db()
        self.syms = self._read_allowed_syms(join(self.metapath,GDFL_SYMLIST))
    
    def _read_allowed_syms(self, strpathmeta):
        if not isfile(strpathmeta):
            raise ValueError('Allow syms list is missing')
        else:
            syms = read_csv(strpathmeta)
        
        return syms
    
    def _read_ingest_dates(self, strpathdata):
        dts = []
        try:
            files = listdir(strpathdata)
            files = [f for f in files if f.endswith('.csv')]
            dts = [datetime.strptime(s[-12:-4],"%d%m%Y") for s in files]
            dts = pd.to_datetime(dts).tolist()
        except:
            pass
        
        return dts
    
    def _read_bizdays(self, strpathmeta, strpathdata):
        dts = []
        if not isfile(strpathmeta):
            #raise ValueError('Business days list is missing')
            dts = self._read_ingest_dates(strpathdata)
        else:
            dts = read_csv(strpathmeta)
            #dts = dts['dates'].tolist()
            dts = pd.to_datetime(dts['dates']).tolist()
            dts = dts + self._read_ingest_dates(strpathdata)
        
        bizdays = pd.DataFrame(sorted(set(dts)),columns=['dates'])
        bizdays.to_csv(strpathmeta,index=False)
        return list(set(dts))
    
    def _create_calendar(self, cal_name,tz,session_start,session_end,dts):
        cal = ExchangeCalendarFromDate(cal_name,tz,session_start,session_end,dts)
        try:
            deregister_calendar(GDFL_CALENDAR_NAME)
            get_calendar(GDFL_CALENDAR_NAME)
        except:
            register_calendar(GDFL_CALENDAR_NAME, cal)
        return get_calendar(GDFL_CALENDAR_NAME)
        
    
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
                GDFL_CALENDAR_NAME,
                GDFL_CALENDAR_TZ,
                GDFL_SESSION_START,
                GDFL_SESSION_END,
                self._read_bizdays(join(self.metapath,GDFL_BIZDAYLIST),self.csvdir))
        
        gdfl_bundle(environ,
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
                      self.csvdir,
                      self.minute_bar_path,
                      self.daily_bar_path,
                      self.asset_db_path,
                      self.adjustment_db_path,
                      self.meta_data,
                      self.syms)


@bundles.register("GDFL",create_writers=False)
def gdfl_bundle(environ,
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
                  syms = None):
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
                                             start_session,
                                             end_session,
                                             GDFL_MINUTES_PER_DAY)
    daily_bar_writer = BcolzDailyBarWriter(daily_bar_path,
                                             calendar,
                                             start_session,
                                             end_session)
    asset_db_writer = AssetDBWriter(asset_db_path)
    adjustment_writer = SQLiteAdjustmentWriter(adjustment_db_path,
                                               BcolzDailyBarReader(daily_bar_path),
                                               calendar.all_sessions,
                                               overwrite=True)
    

    divs_splits = {'divs': DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'stock_divs': DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'splits': DataFrame(columns=['sid', 'ratio',
                                                'effective_date']),
                   'mergers': DataFrame(columns=['sid', 'ratio',
                                                'effective_date'])}
    
    files = listdir(csvdir)
    files = [f for f in files if f.endswith('.csv')]
    
    for f in files:
        full_path = os.path.join(csvdir, f)
        print("handling {}".format(f))
        if 'MCX' in f:
            continue
        try:
            minute_bar_writer.write(_minute_data_iter(full_path, meta_data,calendar, syms),
                     show_progress=show_progress)
        except BcolzMinuteOverlappingData:
            pass
        os.remove(full_path)
    
    daily_bar_writer.write(_pricing_iter(GDFL_DAILY_PATH, meta_data['symbol'].tolist(),
                                         divs_splits, show_progress),
                     show_progress=show_progress)
        
    _write_meta_data(asset_db_writer,asset_db_path, meta_data)
    _write_adjustment_data(adjustment_writer,adjustment_db_path,divs_splits)


def _write_meta_data(asset_db_writer,asset_db_path,meta_data):
    try:
        os.remove(asset_db_path)
    except:
        pass

    asset_db_writer.write(equities=meta_data)

def _write_adjustment_data(adjustment_writer,adjustment_db_path,divs_splits):
    try:
        os.remove(adjustment_db_path)
    except:
        pass
    
    divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
    divs_splits['stock_divs']['sid'] = divs_splits['stock_divs']['sid'].astype(int)
    divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
    divs_splits['mergers']['sid'] = divs_splits['mergers']['sid'].astype(int)
    adjustment_writer.write(splits=divs_splits['splits'],
                            mergers=divs_splits['mergers'],
                            dividends=divs_splits['divs'],
                            stock_dividends=divs_splits['stock_divs'])

def _pricing_iter(csvdir, symbols, divs_splits, show_progress):
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

            if 'split' in dfr.columns:
                tmp = 1. / dfr[dfr['split'] != 1.0]['split']
                split = DataFrame(data=tmp.index.tolist(),
                                  columns=['effective_date'])
                split['ratio'] = tmp.tolist()
                split['sid'] = sid

                splits = divs_splits['splits']
                index = Index(range(splits.shape[0],
                                    splits.shape[0] + split.shape[0]))
                split.set_index(index, inplace=True)
                divs_splits['splits'] = splits.append(split)

            if 'dividend' in dfr.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = dfr[dfr['dividend'] != 0.0]['dividend']
                div = DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                div['record_date'] = NaT
                div['declared_date'] = NaT
                div['pay_date'] = NaT
                div['amount'] = tmp.tolist()
                div['sid'] = sid

                divs = divs_splits['divs']
                ind = Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                divs_splits['divs'] = divs.append(div)

            yield sid, dfr

def _minute_data_iter(data_path,meta_data,calendar, syms, exchange="NSE"):
    data = read_csv(data_path, parse_dates = True)
    data = fixup_minute_df(data, calendar)
    start_session = pd.Timestamp(((data.index.sort_values())[0]).date())
    end_session = pd.Timestamp(((data.index.sort_values())[-1]).date())
    idx = calendar.minutes_for_sessions_in_range(start_session,end_session)
    symbols = data['Ticker'].unique()
    names_dict = dict(zip(syms['symbol'],syms['name']))
    
    try:
        meta_dict = meta_data['symbol'].to_dict()
        meta_dict = {s:i for i,s in meta_dict.iteritems()}
    except:
        meta_dict = {}
    
    for s in symbols:
        dfr = data[data['Ticker']==s].sort_index().drop_duplicates()
        dfr = dfr[~dfr.index.duplicated(keep='last')]
        dfr = dfr.drop('Ticker',axis=1)
        dfr = get_equal_sized_df(dfr,idx)
        
        s = ticker_cleanup(s)
        if not check_sym(s,syms):
            continue

        print(s)
        if s in meta_dict:
            sid = meta_dict[s]
            if meta_data.loc[sid,'start_date'] > start_session.value:
                meta_data.loc[sid,'start_date'] = start_session.value
            if meta_data.loc[sid,"end_date"] < end_session.value:
                meta_data.loc[sid,"end_date"] = end_session.value
                meta_data.loc[sid,"auto_close_date"] = (end_session + Timedelta(days=1)).value
        else:
            sid = len(meta_data)
            meta_data.loc[sid] = s, names_dict[s], start_session.value,end_session.value,(end_session + Timedelta(days=1)).value, exchange  
        
        save_as_daily(join(GDFL_DAILY_PATH,s+".csv"),dfr)
        yield sid, dfr
    

def ticker_cleanup(s):
    s = s.replace(".NFO","")
    s = s.replace(".NSE_IDX","")
    s = s.replace(" ","")
    s = s.replace(".NSE","")
    return s

def fixup_minute_df(data, calendar):
    ticker_col = np.where(data.columns.to_series().str.lower().str.contains('ticker') == True)[0][0]
    dt_col = np.where(data.columns.to_series().str.lower().str.contains('date') == True)[0][0]
    time_col = np.where(data.columns.to_series().str.lower().str.contains('time') == True)[0][0]
    oi_col = np.where(data.columns.to_series().str.lower().str.contains('open interest') == True)[0][0]
    idx = pd.to_datetime(data.iloc[:,dt_col] + " " + data.iloc[:,time_col])
    data = data.set_index(idx)
    data.index = data.index.tz_localize(calendar.tz)
    dropcols = [dt_col,time_col,oi_col]
    data = data.drop(data.columns[dropcols],axis=1)
    data = data[data.iloc[:,ticker_col].str.contains("PE.NFO|CE.NFO")==False]
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
    syms = syms['symbol'].tolist()
    return True if s in syms else False


def save_as_daily(strpath,df):
    df = df.resample('1D').agg({'open': 'first', 
               'high': 'max', 
               'low': 'min', 
               'close': 'last',
               'volume':'sum'})
    
    if not isfile(strpath):
        ddf = df
    else:
        ddf = read_csv(strpath, index_col=0, parse_dates = True).sort_index()
        ddf = ddf.append(df)
        ddf = ddf.drop_duplicates()
    
    ddf.to_csv(strpath)
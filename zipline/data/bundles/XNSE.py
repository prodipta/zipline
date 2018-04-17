"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys
from os.path import isfile, join
import sqlalchemy as sa
import pandas as pd

from logbook import Logger, StreamHandler
from pandas import read_csv, Timedelta

from zipline.utils.calendars import deregister_calendar, get_calendar, register_calendar
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendars import ExchangeCalendarFromDate
from zipline.assets import AssetDBWriter
from zipline.data.minute_bars import BcolzMinuteBarWriter
from zipline.data.us_equity_pricing import BcolzDailyBarWriter, SQLiteAdjustmentWriter, BcolzDailyBarReader
from . import core as bundles
from zipline.assets.asset_db_schema import asset_db_table_names

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)

XNSE_DATA_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/XNSE/input"
XNSE_META_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/XNSE/meta"
XNSE_BUNDLE_PATH = "C:/Users/academy.academy-72/Desktop/dev platform/data/XNSE/bundle"
ASSET_DB = "assets-6.sqlite"
ADJUSTMENT_DB = "adjustments.sqlite"
XNSE_BIZDAYLIST = "bizdays.csv"
XNSE_SYMLIST = "symbols.csv"
XNSE_CALENDAR_NAME = "XNSE"
XNSE_CALENDAR_TZ = "Etc/UTC"
XNSE_SESSION_START = (9,15,59)
XNSE_SESSION_END = (15,29,59)
XNSE_MINUTES_PER_DAY = 375
XNSE_BENCHMARK_SYM = 'NIFTY50'

def xnse_equities(tframes=None, csvdir=XNSE_DATA_PATH):
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

    return CSVDIRBundleXNSE(tframes, csvdir).ingest


class CSVDIRBundleXNSE:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """

    def __init__(self, tframes=None, csvdir=None):
        self.csvdir = csvdir
        self.bundledir = XNSE_BUNDLE_PATH
        self.metapath = XNSE_META_PATH
        self.calendar = self._create_calendar(
                XNSE_CALENDAR_NAME,
                XNSE_CALENDAR_TZ,
                XNSE_SESSION_START,
                XNSE_SESSION_END,
                self._read_bizdays(join(self.metapath,XNSE_BIZDAYLIST)))
        self.minute_bar_path = join(XNSE_BUNDLE_PATH,"minute")
        self.daily_bar_path = join(XNSE_BUNDLE_PATH,"daily")
        self.asset_db_path = join(XNSE_BUNDLE_PATH,ASSET_DB)
        self.adjustment_db_path = join(XNSE_BUNDLE_PATH,ADJUSTMENT_DB)
        self.meta_data = self._read_asset_db()
        self.syms = self._read_allowed_syms(join(self.metapath,XNSE_SYMLIST))
    
    def _read_allowed_syms(self, strpathmeta):
        if not isfile(strpathmeta):
            raise ValueError('Allow syms list is missing')
        else:
            syms = read_csv(strpathmeta)
        
        return syms
    
    def _read_bizdays(self, strpathmeta):
        dts = []
        if not isfile(strpathmeta):
            raise ValueError('Business days list is missing')
        else:
            dts = read_csv(strpathmeta)
            #dts = dts['dates'].tolist()
            dts = pd.to_datetime(dts['dates']).tolist()
        return list(set(dts))
    
    def _create_calendar(self, cal_name,tz,session_start,session_end,dts):
        cal = ExchangeCalendarFromDate(cal_name,tz,session_start,session_end,dts)
        try:
            deregister_calendar(XNSE_CALENDAR_NAME)
            get_calendar(XNSE_CALENDAR_NAME)
        except:
            register_calendar(XNSE_CALENDAR_NAME, cal)
        return get_calendar(XNSE_CALENDAR_NAME)
    
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
                XNSE_CALENDAR_NAME,
                XNSE_CALENDAR_TZ,
                XNSE_SESSION_START,
                XNSE_SESSION_END,
                self._read_bizdays(join(self.metapath,XNSE_BIZDAYLIST)))
        
        xnse_bundle(environ,
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


@bundles.register("XNSE", create_writers=False)
def xnse_bundle(environ,
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
                  csvdir=None,
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
        raise ValueError("input data directory missing")

    if not os.path.isdir(csvdir):
        raise ValueError("%s is not a directory" % csvdir)

    minute_bar_writer = BcolzMinuteBarWriter(minute_bar_path,
                                             calendar,
                                             start_session,
                                             end_session,
                                             XNSE_MINUTES_PER_DAY,
                                             XNSE_BENCHMARK_SYM)
    daily_bar_writer = BcolzDailyBarWriter(daily_bar_path,
                                             calendar,
                                             start_session,
                                             end_session)
    asset_db_writer = AssetDBWriter(asset_db_path)
    
    
    symbols = sorted(item.split('.csv')[0] 
        for item in os.listdir(csvdir) if '.csv' in item)
    
    if not symbols:
        raise ValueError("no <symbol>.csv* files found in %s" % csvdir)

    
    daily_bar_writer.write(_pricing_iter(csvdir, symbols, meta_data, syms,
                show_progress),show_progress=show_progress)


    _write_meta_data(asset_db_writer,asset_db_path,meta_data)
    _write_adjustment_data(adjustment_writer,adjustment_db_path,meta_data,syms,daily_bar_path,calendar.all_sessions)

def _write_meta_data(asset_db_writer,asset_db_path,meta_data):
    try:
        os.remove(asset_db_path)
    except:
        pass

    asset_db_writer.write(equities=meta_data)
    
def _write_adjustment_data(adjustment_db_path,meta_data,syms,daily_bar_path,cal_sessions):
    try:
        os.remove(adjustment_db_path)
    except:
        pass
    
    adjustment_writer = SQLiteAdjustmentWriter(adjustment_db_path,
                                               BcolzDailyBarReader(daily_bar_path),
                                               cal_sessions,
                                               overwrite=True)
    
    meta_dict = dict(zip(meta_data['symbol'].tolist(),range(len(meta_data))))
    
    mergers = pd.read_csv(join(XNSE_META_PATH,"mergers.csv"),parse_dates=True)
    mergers = mergers[mergers.symbol.isin(syms.symbol)]
    mergers['effective_date'] = pd.to_datetime(mergers['effective_date'])
    mergers['sid'] = [meta_dict[sym] for sym in mergers['symbol'].tolist()]
    mergers =mergers.drop(['symbol'],axis=1)
    
    splits = pd.read_csv(join(XNSE_META_PATH,"splits.csv"),parse_dates=True)
    splits = splits[splits.symbol.isin(syms.symbol)]
    splits['effective_date'] = pd.to_datetime(splits['effective_date'])
    splits['sid'] = [meta_dict[sym] for sym in splits['symbol'].tolist()]
    splits =splits.drop(['symbol'],axis=1)
    
    dividends = pd.read_csv(join(XNSE_META_PATH,"dividends.csv"),parse_dates=True)
    dividends = dividends[dividends.symbol.isin(syms.symbol)]
    dividends['ex_date'] = pd.to_datetime(dividends['ex_date'])
    dividends['declared_date'] = pd.to_datetime(dividends['declared_date'])
    dividends['pay_date'] = pd.to_datetime(dividends['pay_date'])
    dividends['record_date'] = pd.to_datetime(dividends['record_date'])
    dividends['sid'] = [meta_dict[sym] for sym in dividends['symbol'].tolist()]
    dividends =dividends.drop(['symbol'],axis=1)
    
    adjustment_writer.write(splits=splits,
                            mergers=mergers,
                            dividends=dividends)



def _pricing_iter(csvdir, symbols, meta_data, syms, show_progress):
    with maybe_show_progress(symbols, show_progress,
                             label='Loading custom pricing data: ') as it:
        files = os.listdir(csvdir)
        
        names_dict = dict(zip(syms['symbol'],syms['name']))
        try:
            meta_dict = meta_data['symbol'].to_dict()
            meta_dict = {s:i for i,s in meta_dict.iteritems()}
        except:
            meta_dict = {}
        
        for sid, s in enumerate(it):
            logger.debug('%s: sid %s' % (s, sid))

            try:
                fname = [fname for fname in files
                         if '%s.csv' % s == fname][0]
            except IndexError:
                raise ValueError("%s.csv file is not in %s" % (s, csvdir))

            dfr = read_csv(os.path.join(csvdir, fname),
                           parse_dates=[0],
                           infer_datetime_format=True,
                           index_col=0).sort_index()

            start_date = dfr.index[0]
            end_date = dfr.index[-1]
            
            if s in meta_dict:
                sid = meta_dict[s]
                if meta_data.loc[sid,'start_date'] > start_date.value:
                    meta_data.loc[sid,'start_date'] = start_date.value
                if meta_data.loc[sid,"end_date"] < end_date.value:
                    meta_data.loc[sid,"end_date"] = end_date.value
                    meta_data.loc[sid,"auto_close_date"] = (end_date + Timedelta(days=1)).value
            else:
                sid = len(meta_data)
                meta_data.loc[sid] = s, names_dict[s], start_date,end_date,(end_date + Timedelta(days=1)), "NSE"  
            
            yield sid, dfr

def check_sym(s,syms):
    syms = syms['symbol'].tolist()
    return True if s in syms else False
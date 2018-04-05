"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys
from os.path import isfile, join
import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import datetime

from logbook import Logger, StreamHandler
from numpy import empty
from pandas import DataFrame, read_csv, Index, Timedelta, NaT

from zipline.utils.calendars import deregister_calendar, get_calendar, register_calendar
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendars import ExchangeCalendarFromDate
from zipline.assets import AssetDBWriter
from zipline.data.minute_bars import BcolzMinuteBarWriter, BcolzMinuteOverlappingData
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
        
    

    divs_splits = {'divs': DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'splits': DataFrame(columns=['sid', 'ratio',
                                                'effective_date'])}

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
    adjustment_writer = SQLiteAdjustmentWriter(adjustment_db_path,
                                               BcolzDailyBarReader(daily_bar_path),
                                               calendar.all_sessions,
                                               overwrite=True)
    
    
    symbols = sorted(item.split('.csv')[0] 
        for item in os.listdir(csvdir) if '.csv' in item)
    
    if not symbols:
        raise ValueError("no <symbol>.csv* files found in %s" % csvdir)

    
    daily_bar_writer.write(_pricing_iter(csvdir, symbols, meta_data, syms,
                divs_splits, show_progress),show_progress=show_progress)

        # Hardcode the exchange to "CSVDIR" for all assets and (elsewhere)
        # register "CSVDIR" to resolve to the NYSE calendar, because these
        # are all equities and thus can use the NYSE calendar.
    #metadata['exchange'] = "NSE"

    asset_db_writer.write(equities=meta_data)

    divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
    divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
    adjustment_writer.write(splits=divs_splits['splits'],dividends=divs_splits['divs'])


def _pricing_iter(csvdir, symbols, meta_data, syms, divs_splits, show_progress):
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
                         if '%s.csv' % s in fname][0]
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

            if 'split' in dfr.columns:
                tmp = dfr[dfr['split'] != 1.0]['split']
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

def check_sym(s,syms):
    syms = syms['symbol'].tolist()
    return True if s in syms else False
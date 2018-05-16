"""
Module for building a complete dataset from local directory with csv files.
"""
import os
import sys
from os.path import isfile, join
import pandas as pd
import json
import numpy as np

from logbook import Logger, StreamHandler
from pandas import read_csv, Timedelta

from zipline.utils.calendars import deregister_calendar, get_calendar, register_calendar
from zipline.utils.cli import maybe_show_progress
from zipline.utils.calendars import ExchangeCalendarFromDate
from zipline.assets import AssetDBWriter
from zipline.data.minute_bars import BcolzMinuteBarWriter
from zipline.data.us_equity_pricing import BcolzDailyBarWriter, SQLiteAdjustmentWriter, BcolzDailyBarReader
from . import core as bundles

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)

def xnse_equities(configpath=None):
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

    return CSVDIRBundleXNSE(configpath).ingest


class CSVDIRBundleXNSE:
    """
    Wrapper class to call csvdir_bundle with provided
    list of time frames and a path to the csvdir directory
    """
    def _read_config(self, configpath):
        with open(configpath) as configfile:
            config = json.load(configfile)
            self.meta_path=config["META_PATH"]
            self.bundle_path=config["BUNDLE_PATH"]
            self.daily_path=config["DAILY_PATH"]
            self.asset_db_name=config["ASSET_DB"]
            self.adjustment_db_name=config["ADJUSTMENT_DB"]
            self.metadata_file=config["META_DATA"]
            self.bizdays_file=config["BIZDAYLIST"]
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
        self.meta_data = self._read_asset_db()
        self.syms = self._read_allowed_syms()

    def _read_allowed_syms(self):
        return self.meta_data['symbol'].tolist()

    def _read_bizdays(self, strpathmeta):
        dts = []
        if not isfile(strpathmeta):
            raise ValueError('Business days list is missing')
        else:
            dts = read_csv(strpathmeta)
            #dts = dts['dates'].tolist()
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
        if not isfile(join(self.meta_path,self.symlist_file)):
            raise ValueError('symbols metadata list is missing')

        meta_data = pd.read_csv(join(self.meta_path,self.symlist_file))
        meta_data.loc[len(meta_data)] = self.benchmar_symbol,self.benchmar_symbol,self.bizdays[0],self.bizdays[-1]
        meta_data['start_date'] = pd.to_datetime(meta_data['start_date'])
        meta_data['end_date'] = pd.to_datetime(meta_data['end_date'])
        meta_data['auto_close_date'] = pd.to_datetime([e+pd.Timedelta(days=1) for e in meta_data['end_date'].tolist()])
        meta_data['exchange'] = 'NSE'
        
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
                      self.daily_path,
                      self.minute_bar_path,
                      self.daily_bar_path,
                      self.asset_db_path,
                      self.adjustment_db_path,
                      self.meta_data,
                      self.meta_path,
                      self.syms,
                      self.bizdays,
                      self.cal_minutes_per_day,
                      self.benchmar_symbol)


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
                  meta_path = None,
                  syms = None,
                  bizdays = None,
                  minutes_per_day = None,
                  benchmark_symbol = None):
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
                                             minutes_per_day,
                                             benchmark_symbol)
    daily_bar_writer = BcolzDailyBarWriter(daily_bar_path,
                                             calendar,
                                             start_session,
                                             end_session)
    asset_db_writer = AssetDBWriter(asset_db_path)

    daily_bar_writer.write(_pricing_iter(csvdir, syms, meta_data, bizdays,
                show_progress),show_progress=show_progress)

    meta_data = meta_data.dropna()
    meta_data = meta_data.reset_index()
    _write_meta_data(asset_db_writer,asset_db_path,meta_data)
    _write_adjustment_data(adjustment_db_path,meta_data,syms,daily_bar_path,
                           calendar.all_sessions, bizdays, meta_path)

def _write_meta_data(asset_db_writer,asset_db_path,meta_data):
    try:
        os.remove(asset_db_path)
    except:
        pass

    asset_db_writer.write(equities=meta_data)

def _write_adjustment_data(adjustment_db_path,meta_data,syms,daily_bar_path,
                           cal_sessions,bizdays, meta_path):
    try:
        os.remove(adjustment_db_path)
    except:
        pass

    adjustment_writer = SQLiteAdjustmentWriter(adjustment_db_path,
                                               BcolzDailyBarReader(daily_bar_path),
                                               cal_sessions,
                                               overwrite=True)

    meta_dict = dict(zip(meta_data['symbol'].tolist(),range(len(meta_data))))

    mergers = pd.read_csv(join(meta_path,"mergers.csv"),parse_dates=[0])
    mergers['effective_date'] = pd.to_datetime(mergers['effective_date'])
    mergers['sid'] = [meta_dict.get(sym,-1) for sym in mergers['symbol'].tolist()]
    mergers =mergers.drop(['symbol'],axis=1)
    mergers = mergers[mergers['sid'] != -1]

    splits = pd.read_csv(join(meta_path,"splits.csv"),parse_dates=[0])
    splits['effective_date'] = pd.to_datetime(splits['effective_date'])
    splits['sid'] = [meta_dict.get(sym,-1) for sym in splits['symbol'].tolist()]
    splits =splits.drop(['symbol'],axis=1)
    splits = splits[splits['sid'] != -1]

    dividends = pd.read_csv(join(meta_path,"dividends.csv"),parse_dates=[0])
    dividends['ex_date'] = pd.to_datetime(dividends['ex_date'])
    dividends['declared_date'] = pd.to_datetime(dividends['declared_date'])
    dividends['pay_date'] = pd.to_datetime(dividends['pay_date'])
    dividends['record_date'] = pd.to_datetime(dividends['record_date'])
    dividends['sid'] = [meta_dict.get(sym,-1) for sym in dividends['symbol'].tolist()]
    dividends =dividends.drop(['symbol'],axis=1)
    dividends = dividends[dividends['sid'] != -1]

    adjustment_writer.write(splits=splits,
                            mergers=mergers,
                            dividends=dividends)



def _pricing_iter(csvdir, symbols, meta_data, bizdays, show_progress):
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
            if len(dfr) == 0:
                print('removing {} as we have no data rows'.format(symbol))
                meta_data.symbol[meta_data.symbol==symbol] = np.nan
                continue
            start_date = pd.to_datetime(meta_data.loc[meta_data.symbol==symbol,'start_date'])
            end_date = pd.to_datetime(meta_data.loc[meta_data.symbol==symbol,'end_date'])
            dfr = ensure_all_days(dfr,start_date,end_date, bizdays)
            if len(dfr) == 0:
                print('removing {} as we have no data rows'.format(symbol))
                meta_data.symbol[meta_data.symbol==symbol] = np.nan
                continue

            yield sid, dfr

def ensure_all_days(dfr, start_date, end_date, bizdays):
    start_date = start_date.iloc[0]
    end_date = end_date.iloc[0]
    delta = end_date - start_date
    dts = [(start_date + Timedelta(days=x)) for x in range(0, delta.days+1)]
    dts = pd.to_datetime(dts)
    bizdays = pd.to_datetime(bizdays)
    idx = bizdays.intersection(dts)
    if len(idx) == len(dfr):
        return dfr
    dfr = get_equal_sized_df(dfr,idx)
    return dfr

def get_equal_sized_df(dfr, idx):
    base_dfr = pd.DataFrame(columns=['open','high','low','close','volume'],index = idx)
    valid_idx = base_dfr.index.intersection(dfr.index)
    base_dfr.loc[valid_idx] = dfr.loc[valid_idx]
    dfr = base_dfr.fillna(method = "ffill")
    dfr = dfr.fillna(method = "bfill")
    dfr = dfr.dropna()
    return dfr

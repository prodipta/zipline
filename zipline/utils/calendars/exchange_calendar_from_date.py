# -*- coding: utf-8 -*-

import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from pytz import timezone
from datetime import time, timedelta

from .trading_calendar import TradingCalendar
from zipline.utils.memoize import lazyval

class ExchangeCalendarFromDate(TradingCalendar):
    """
    Exchange calendar auto_generated from data
    """
    def __init__(self, name, tz, open_time, close_time, dts):
        self._name = name
        self._timezone = timezone(tz)
        self._open_time = time(*open_time)
        self._close_time = time(*close_time)
        #dts = pd.to_datetime(idx).apply(lambda x: x.date())
        #dts = dts.unique()
        dts = pd.to_datetime(dts).sort_values()
        start_date = dts[0]
        end_date = dts[-1]
        delta = end_date - start_date
        dates_list = [(start_date + timedelta(days = i))for i in range(delta.days + 1)]
        holidays = list(set(dates_list) - set(dts))
        holidays = sorted(holidays)
        self._holidays = holidays
        super(ExchangeCalendarFromDate, self).__init__()
        
    @property
    def name(self):
        return self._name

    @property
    def tz(self):
        return self._timezone

    @property
    def open_time(self):
        return (self._open_time)

    @property
    def close_time(self):
        return (self._close_time)

    @property
    def adhoc_holidays(self):
        return []
        
    
    @property
    def regular_holidays(self):
        #return nse_holidays
        return []
    
    @lazyval
    def day(self):
        return CustomBusinessDay(holidays=self._holidays)

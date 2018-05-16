# -*- coding: utf-8 -*-
"""
Created on Wed May 09 11:41:19 2018

@author: Prodipta
"""
from enum import Enum
import time
import datetime
import pytz
import pandas as pd

class ClockEvents(Enum):
    BAR = 0
    SESSION_START = 1
    SESSION_END = 2
    MINUTE_END = 3
    BEFORE_TRADING_START_BAR = 4
    HEART_BEAT = 5
    END_CLOCK = 6

class RealTimeClock:
    def __init__(self,*args, **kwargs):
        self.lifetime = kwargs.pop('lifetime', 200)
        self.heartbeat = kwargs.pop('heartbeat', 5)
        self.timezone = kwargs.pop('timezone', 'Etc/UTC')
        self.market_open_time = kwargs.pop('market_open',datetime.time(9,15,0))
        self.market_close_time = kwargs.pop('market_open',datetime.time(15,30,0))
        self.before_trading_start_minute = kwargs.pop('before_trading_start_minute',datetime.time(8,45,0))
        self.is_market_open = kwargs.pop('is_market_open', lambda x:True)
        self.minute_emission = kwargs.pop('minute_emission', True)
        
        self.timestamp = None
        self.date = None
        self.time = None
        
        self.before_trading_start = True
        self.current_date = None
        self.kill = False
        
        self.get_time_now()
        
    def get_time_now(self):
        tz = pytz.timezone(self.timezone)
        dt = datetime.datetime.now(tz)
        self.timestamp = pd.Timestamp(dt, tz= 'Etc/UTC')
        self.date = dt.date()
        self.time = dt.time()
    
    def is_active_session(self):
        return self.is_market_open(self.date)
    
    def is_in_session(self):
        if not self.is_active_session():
            return False
        if self.time >= self.market_open_time and self.time <= self.market_close_time:
            return True
        return False
    
    def is_session_start(self):
        if self.current_date is None and self.time >= self.market_open_time:
            #self.current_date = self.date
            return True
        return False
    
    def is_session_end(self):
        if self.time > self.market_close_time and self.current_date is not None:
            #self.current_date = None
            return True
        return False
    
    def is_before_trading_start(self):
        if self.before_trading_start and self.current_date is None and self.time >= self.before_trading_start_minute:
            return True
        return False
    
    def reset_clock(self):
        self.before_trading_start = True
        self.current_date = None
        self.kill = False
        
    def pause_clock(self):
        self.kill = True
        
    def close(self):
        self.reset_clock()
        raise GeneratorExit
    
    def __iter__(self):
        try:
            while not self.kill:
                self.get_time_now()
                loop_start_time = time.time()
            
                if self.is_before_trading_start():
                    self.before_trading_start = False
                    yield self.timestamp, ClockEvents.BEFORE_TRADING_START_BAR
                if self.is_session_start():
                    self.current_date = self.date
                    yield self.timestamp, ClockEvents.SESSION_START
                if self.is_session_end():
                    self.current_date = None
                    self.before_trading_start = True
                    yield self.timestamp, ClockEvents.SESSION_END
                    
                if self.is_in_session(): 
                    yield self.timestamp, ClockEvents.BAR
                    if self.minute_emission:
                        yield self.timestamp, ClockEvents.MINUTE_END
                else:
                    yield self.timestamp, ClockEvents.HEART_BEAT
                
                loop_end_time = time.time()
                time_left = round(self.heartbeat  - (loop_end_time - loop_start_time))
                if time_left > 0:
                    time.sleep(time_left)
        except GeneratorExit:
            self.reset_clock()
            return
        finally:
            self.reset_clock()
            return
            
        #yield self.timestamp, ClockEvents.END_CLOCK
        self.reset_clock()
        return

#realtime_clock = RealTimeClock(timezone='Asia/Calcutta')
#i = 0
#for t,e in realtime_clock:
#    print('{}:{}'.format(t,e))
#    i = i+1
#    if i>6:
#        print('we are done, exit the clock loop')
#        #realtime_clock.kill = True
#        break
#        #realtime_clock.close()
        
        
    


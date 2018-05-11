# -*- coding: utf-8 -*-
"""
Created on Thu May 10 14:36:16 2018

@author: Prodipta
"""

from logbook import Logger

from zipline.finance.blotter import Blotter
from zipline.utils.input_validation import expect_types
from zipline.assets import Asset


log = Logger('LiveBlotter')
warning_logger = Logger('LiveAlgoWarning')

class LiveBlotter(Blotter):
    def __init__(self, data_frequency, broker):
        self.broker = broker
        self.data_frequency = data_frequency

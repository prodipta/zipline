# -*- coding: utf-8 -*-

from enum import Enum
import pandas as pd

from zipline.assets._assets import Asset

class AssetType(Enum):
    EQUITY = 0
    FUNDS = 1
    FUTURES = 2
    OPTIONS = 3
    CRYPTO = 4
    COMMODITY = 5
    CFD = 6

class TradeOnlyAsset(Asset):
    
    def __init__(self, symbol, exchange, broker="", *args, **kwargs):
        
        sid = -1
        asset_name=kwargs.pop('asset_name', "")
        start_date=kwargs.pop('start_date', None)
        end_date=kwargs.pop('end_date', None)
        first_traded=kwargs.pop('first_traded', None)
        auto_close_date=kwargs.pop('auto_close_date', None)
        exchange_full=kwargs.pop('exchange_full', None)
        
        super(TradeOnlyAsset,self).__init__(sid, exchange, symbol, asset_name,
             start_date, end_date, first_traded, auto_close_date, exchange_full)
        
        self.broker_name=broker
        self.asset_type = kwargs.pop('asset_type', AssetType.EQUITY)
        self.option_type = kwargs.pop('option_type', None)
        self.option_strike = kwargs.pop('option_strike', 0)
        self.expiry = kwargs.pop('option_strike', pd.NaT)
        self.asset_multiplier = kwargs.pop('asset_multiplier', 1)
        self.underlying = kwargs.pop('underlying', self.symbol)
        
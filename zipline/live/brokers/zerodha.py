# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd
import time
import logging
from sqlalchemy import create_engine

from kiteconnect import KiteConnect
import kiteconnect.exceptions as KiteExceptions

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.live.brokers.brokers import Broker, AuthenticationError, OrdersError, InstrumentsError
from zipline.live.finance.order import LiveOrder
from zipline.live.finance.assets import TradeOnlyAsset, AssetType

from zipline.finance.commission import PerTrade
from zipline.finance.order import ORDER_STATUS
from zipline.finance.performance.position import Position
from zipline.finance.transaction import Transaction
from zipline.assets.assets import AssetFinder


logging.basicConfig(level=logging.DEBUG)

ZERODHA_MINIMUM_COST_PER_EQUITY_TRADE = 0.0
ZERODHA_MINIMUM_COST_PER_FUTURE_TRADE = 20.0
WORKSPACE = 'C:/Users/academy.academy-72/Desktop/dev platform/data/LIVE/WORKSPACE'


class ZerodhaBroker(Broker):
    
    def __init__(self, *args, **kwargs):
        self.api_key = kwargs.pop('api_key', "0")
        self.api_secret = kwargs.pop('api_secret', "0")
        self.request_token = kwargs.pop('request_token', "0")
        self.access_token = kwargs.pop('access_token', "0")
        self.account_id = kwargs.pop('account_id', None)
        
        self.timezone = kwargs.pop('timezone', 'Etc/UTC')
        self.commission = PerTrade(cost=ZERODHA_MINIMUM_COST_PER_FUTURE_TRADE)
        
        self.kite = KiteConnect(api_key=self.api_key)
        self.login_is_valid = False
        self.initialize(*args, **kwargs)
        
        self._orderbook = []
        self._open_orders = []
        self._positionbook = []
        self._transactions = []
        
        self.workspace = WORKSPACE
        self.bundle_path = kwargs.pop('bundle_path', None)
        asset_db_path = 'sqlite:///' + os.path.join(self.bundle_path,'assets-6.sqlite')
        engine = create_engine(asset_db_path)
        self.asset_finder = AssetFinder(engine)
        
        self.last_api_call_dt = pd.Timestamp.now()
        self.time_since_last_api_call = None
        self.call_rate_limit_in_seconds = 2
        
        self.orderbook_needs_update = True
        self.positionbook_needs_update = True
        self.account_needs_update = True
            
    @property
    def name(self):
        return 'Zerodha'
    
    @property
    def region(self):
        return 'India'
    
    @property
    def accoundID(self):
        return self.account_id
    
    @property
    def orders(self):
        if self.orderbook_needs_update:
            self.update_orderbook()
            
        return self._orderbook
    
    @property
    def open_orders(self):
        if self.orderbook_needs_update:
            self.update_orderbook()
            
        return self._open_orders
    
    @property
    def transactions(self):
        if self.orderbook_needs_update:
            self.update_transactions()
            
        return self._transactions
    
    @property
    def positions(self):
        if self.positionbook_needs_update:
            self.update_positionbook()
            
        return self._positionbook
    
    def initialize(self, *args, **kwargs):
        if not self.validate_login():
            self.authenticate(*args, **kwargs)
            
        if not self.validate_login():
            raise AuthenticationError('Cannot log in to Kite')
            
        try:
            self.save_instruments_list()
            self.update_orderbook()
            self.update_positionbook()
            self.update_transactions()
            self.update_account()
        except:
            logging.warning("Initialization was not complete")
    
    def authenticate(self, *args, **kwargs):
        request_token = kwargs.pop('request_token', self.request_token)
        access_token = kwargs.pop('access_token', self.access_token)
        api_secret = kwargs.pop('api_secret', self.api_secret)
        
        if access_token:
            self.kite.set_access_token(self.access_token)
        elif not access_token:
            if request_token and api_secret:
                try:
                    sess = self.kite.generate_session(request_token, secret=api_secret)
                    self.kite.set_access_token(sess["access_token"])
                    self.access_token = sess["access_token"]
                except:
                    raise AuthenticationError("Failed to obtain access token")
        else:
            raise AuthenticationError("access_token or credentials missing")
            
        if not self.access_token:
            raise AuthenticationError("Authentication error, no access token")
            
        return True
    
    def validate_login(self):
        login_is_valid = False
        
        if self.kite.access_token:
            try:
                self.kite.margins('NSE')
                login_is_valid = True
                self.login_is_valid = login_is_valid
            except KiteExceptions.TokenException:
                self.login_is_valid = login_is_valid
            except:
                pass
        
        return self.login_is_valid
            
    def order(self, asset, amount, style, tag):
        is_buy = amount>0
        
        tradingsymbol = self.asset_to_symbol(asset)
        exchange = self.asset_to_exchange(asset)
        product = self.kite.PRODUCT_NRML
        
        if is_buy:
            transaction_type = self.kite.TRANSACTION_TYPE_BUY
        else:
            transaction_type = self.kite.TRANSACTION_TYPE_SELL
       
        
        limit = style.get_limit_price(is_buy)
        stop = style.get_stop_price(is_buy)
        
        if limit is None and stop is None:
            order_type = self.kite.ORDER_TYPE_MARKET
        elif stop is None:
            order_type = self.kite.ORDER_TYPE_LIMIT
        elif stop is None:
            order_type = self.kite.ORDER_TYPE_STOP
        else:
            raise OrdersError("stop limit orders are not supported at present")
       
        try:
            self.api_time_out()
            order_id = self.kite.place_order(variety = self.kite.VARIETY_REGULAR,
                            exchange=exchange,
                            tradingsymbol=tradingsymbol,
                            transaction_type=transaction_type,
                            quantity=abs(amount),
                            product=product,
                            order_type=order_type,
                            price=limit, tag=tag)

            self.orderbook_needs_update = True
            self.positionbook_needs_update = True
            logging.info("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            logging.warning("Order placement failed: {}".format(e.message))
            self.handle_order_error()
            
    def cancel(self, order_id):
        try:
            self.api_time_out()
            order_id = self.kite.cancel_order(self, 
                        self.kite.VARIETY_REGULAR, 
                        order_id)
            logging.info("Order ID {} cancel request placed".format(order_id))
        except Exception as e:
            logging.warning("Order cancellation failed: {}".format(e.message))
            self.handle_order_error(order_id=order_id)
            
    def handle_order_error(self, *args, **kwargs):
        pass
    
    def update_positionbook(self):        
        self._positionbook = []
        
        self.api_time_out()
        positions = self.kite.positions()['net']
        if positions:
            for p in positions:
                self._positionbook.append(self.dict_to_position(p))
            
        # TODO: test it!
        self.api_time_out()
        holdings = self.kite.holdings()
        if holdings:
            for p in holdings:
                self._positionbook.append(self.dict_to_position(p))
            
        if not self._open_orders:
            self.positionbook_needs_update =False

    def update_orderbook(self):
        self._orderbook = []
        self._open_orders = []
        
        self.api_time_out()
        orders = self.kite.orders()
        if not orders:
            self.orderbook_needs_update = False
            return
        
        for o in orders:
            order = self.dict_to_order(o)
            self._orderbook.append(order)
            if order.status == ORDER_STATUS.OPEN:
                self._open_orders.append(o)
        
        if not self._open_orders:
            self.orderbook_needs_update = False
        
    def update_transactions(self):
        self._transactions = []
        
        if self.orderbook_needs_update:
            self.update_orderbook()
        
        if not self._orderbook:
            return
        
        for o in self._orderbook:
            self._transactions.append(self.transaction_from_order(o))
            
    def update_account(self):
        pass
    
    def save_instruments_list(self):
        try:
            cash = self.kite.instruments(exchange='NSE')
            cash = pd.DataFrame(cash)
            cash = cash.loc[cash.segment=='NSE']
            
            self.api_time_out()
            fno = self.kite.instruments(exchange='NFO')
            fno = pd.DataFrame(fno)
            nifty = fno.loc[(fno.tradingsymbol.str.contains('NIFTY')) & (fno.segment=='NFO-FUT')]
            expiries = sorted(set(pd.to_datetime(nifty.expiry.tolist()).tz_localize('Etc/UTC')))
            self.monthly_expiries = expiries
            banknifty = fno.loc[(fno.tradingsymbol.str.contains('BANKNIFTY')) & (fno.segment=='NFO-OPT')]
            expiries = sorted(set(pd.to_datetime(banknifty.expiry.tolist()).tz_localize('Etc/UTC'))-set(expiries))
            self.weekly_expiries = expiries
            self.expiry = self.monthly_expiries[0]
            cash.to_csv(os.path.join(self.workspace,'zerodha_cash.csv.gz'),compression='gzip')
            fno.to_csv(os.path.join(self.workspace,'zerodha_fno.csv.gz'),compression='gzip')
        except:
            raise InstrumentsError('Failed to save instruments lists')
    
    def symbol_to_asset(self, tradingsymbol, expiry=None):
        """
        Parameters
        ----------
        tradingsymbol : trading symbol (string)

        Returns
        -------
        asset
            An object of type zipline Asset.
        """
        if not expiry:
            expiry = self.expiry
        
        segment_ticker = tradingsymbol.split(':')
        if len(segment_ticker) == 1:
            symbol = segment_ticker[0]
        elif len(segment_ticker) == 2:
            symbol = segment_ticker[1]
        else:
            raise OrdersError('Illegal trading symbol')
        
        symbol_expiry = symbol.split(expiry.strftime('%y%b').upper())
        if len(symbol_expiry) == 1:
            symbol = symbol_expiry[0]
        elif len(symbol_expiry) == 2:
            underlying = symbol_expiry[0]
            instrument_type = symbol_expiry[1]
            if 'FUT' in instrument_type:
                symbol = underlying[0]+'-I'
            elif 'PE' in instrument_type:
                symbol = symbol
                option_type = 'PE'
                option_strike = float(instrument_type.split('PE')[0])
                asset = TradeOnlyAsset(symbol=symbol,exchange='NFO',
                                       broker='Zerodha',
                                       asset_name=underlying,
                                       option_type=option_type,
                                       option_strike=option_strike, 
                                       expiry=expiry, 
                                       asset_type=AssetType.OPTIONS,
                                       underlying='underlying')
            elif 'CE' in instrument_type:
                symbol = symbol
                option_type = 'call'
                option_strike = float(instrument_type.split('CE')[0])
                asset = TradeOnlyAsset(symbol=symbol,exchange='NFO',
                                       broker='Zerodha',
                                       asset_name=underlying,
                                       option_type=option_type,
                                       option_strike=option_strike, 
                                       expiry=expiry, 
                                       asset_type=AssetType.OPTIONS,
                                       underlying='underlying')
            else:
                raise OrdersError('Illegal trading symbol')
        else:
            raise OrdersError('Illegal trading symbol')
        
        asset = self.asset_finder.lookup_symbol(symbol,pd.Timestamp.now().tz_localize('Etc/UTC'))
        return asset
            
    def asset_to_symbol(self, asset, with_segment=False):
        """
        Parameters
        ----------
        asset : Zipline asset object

        Returns
        -------
        tradingsymbol
            A string with the instrument symbol
        """
        
        symbol = asset.symbol
        segment = 'NSE'
        
        if symbol[-2:] == '-I':
            segment = 'NFO'
            symbol = symbol[:-2]+self.expiry.strftime('%y%b').upper()+'FUT'
            
        if(with_segment):
            symbol = segment+':'+symbol
        
        return symbol
    
    def asset_to_exchange(self, asset):
        """
        Parameters
        ----------
        asset : Zipline asset object

        Returns
        -------
        exchange
            A string with the exchange or segment where the instrument is traded
        """
        
        symbol = asset.symbol
        segment = 'NSE'
        
        if symbol[-2:] == '-I':
            segment = 'NFO'
            
        return segment

    def api_time_out(self,n=0):
        if n > 0:
            time.sleep(n)
        
        if self.time_since_last_api_call:
            return
        
        dt = pd.Timestamp.now()
        self.time_since_last_api_call = pd.Timedelta(dt-self.last_api_call_dt).seconds
        if self.time_since_last_api_call > self.call_rate_limit_in_seconds:
            return
        
        sleep_time = self.call_rate_limit_in_seconds - self.time_since_last_api_call
        if sleep_time > 0:
            time.sleep(sleep_time)
            
        return
    
    def order_status_map(self, order_status):
        if order_status == 'COMPLETE':
            return ORDER_STATUS.FILLED
        elif order_status == 'OPEN':
            return ORDER_STATUS.OPEN
        elif order_status == 'REJECTED':
            return ORDER_STATUS.REJECTED
        elif order_status == 'CANCELLED':
            return ORDER_STATUS.CANCELLED
        else:
            raise OrdersError('Illegal order status')
    
    def dict_to_order(self, order_dict):
        dt = pd.Timestamp(order_dict['order_timestamp']).to_datetime()
        #reason = order_dict['status_message']
        #created = pd.Timestamp(order_dict['order_timestamp']).to_datetime()
        
        try:
            asset = self.symbol_to_asset(order_dict['tradingsymbol'])
        except OrdersError:
            asset = self.symbol_to_asset('NIFTY-I')
            #raise OrdersError('Unsupported instruments')
        
        amount = order_dict['quantity']*order_dict.get('multiplier',1)
        filled = order_dict['filled_quantity']*order_dict.get('multiplier',1)
        commission = 0
        _status = self.order_status_map(order_dict['status'])
        stop = order_dict['trigger_price']
        limit = order_dict['price']
        direction = 1 if order_dict['transaction_type']=='BUY' else -1
        broker_order_id = order_dict['order_id']
        tag = order_dict['tag']
        price = order_dict['average_price']
        validity = order_dict['validity']
        parent_id = order_dict['parent_order_id']
        
        o = LiveOrder(dt,asset,amount,stop,limit,filled,commission, 
                      broker_order_id,tag,price,validity,parent_id)
        o.direction = direction
        o.status = _status
        o.broker_order_id = o.id
        
        return o
    
    def dict_to_position(self, position_dict):
        try:
            asset = self.symbol_to_asset(position_dict['tradingsymbol'])
        except OrdersError:
            asset = self.symbol_to_asset('NIFTY-I')
            #raise OrdersError('Unsupported instruments')
            
        amount = position_dict['quantity']*position_dict['multiplier']
        cost_basis = position_dict['average_price']
        last_sale_price = position_dict['last_price']
        last_sale_date = None
        
        p = Position(asset,amount,cost_basis,last_sale_price,last_sale_date)
        
        return p
    
    def transaction_from_order(self, order):
        asset = order.asset
        amount = order.amount
        dt = order.dt
        price = order.price
        order_id = order.id
        commission = self.commission
        txn = Transaction(asset, amount, dt, price, order_id, commission)
        
        return txn
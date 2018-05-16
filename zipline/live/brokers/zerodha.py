# -*- coding: utf-8 -*-

import sys
import os
import pandas as pd
import time
import logging

from kiteconnect import KiteConnect

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.live.brokers.brokers import Broker, AuthenticationError, OrdersError
from zipline.live.finance.order import LiveOrder

from zipline.finance.order import ORDER_STATUS
from zipline.finance.performance.position import Position
from zipline.finance.transaction import Transaction
from zipline.assets.assets import AssetFinder
from sqlalchemy import create_engine



logging.basicConfig(level=logging.DEBUG)

class ZerodhaBroker(Broker):
    
    def __init__(self, args, **kwargs):
        self.api_key = kwargs.pop('api_key', None)
        self.secret_key = kwargs.pop('secret_key', None)
        self.request_token = kwargs.pop('request_token', None)
        self.access_token = kwargs.pop('access_token', None)
        self.account_id = kwargs.pop('account_id', None)
        self.bundle_path = kwargs.pop('bundle', None)
        self.timezone = kwargs.pop('timezone', 'Etc/UTC')
        self.commission = 20
        self.expiry = kwargs.pop('expiry', None)
        
        self.kite = KiteConnect(api_key=self.api_key)
        self.authenticate(args, **kwargs)
        
        self.orderbook = []
        self.positionbook = []
        self.transactions = []
        
        asset_db_path = 'sqlite:///' + os.path.join(self.bundle_path,'assets-6.sqlite')
        engine = create_engine(asset_db_path)
        self.asset_finder = AssetFinder(engine)
        
        self.last_api_call_dt = pd.Timestamp.now()
        self.time_since_last_api_call = None
        self.call_rate_limit_in_seconds = 2
            
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
        orders_list = self.kite.orders()
        if not orders_list:
            return None
        
        orders = []
        for o in orders_list:
            order = self.kite_dict_to_order(o)
            orders.append(order)
            
        return orders
        
    
    @property
    def transactions(self):
        pass
        
    def force_time_out(self):
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
    
    def kite_dict_to_order(self, order_dict):
        dt = pd.Timestamp(order_dict['order_timestamp']).to_datetime()
        #reason = order_dict['status_message']
        #created = pd.Timestamp(order_dict['order_timestamp']).to_datetime()
        
        try:
            asset = self.symbol_to_asset(order_dict['tradingsymbol'])
        except OrdersError:
            raise OrdersError('Unsupported instruments')
        
        amount = order_dict['quantity']*order_dict.get('multiplier',1)
        filled = order_dict['filled_quantity']*order_dict.get('multiplier',1)
        commission = self.commission
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
    
    def kite_dict_to_position(self, position_dict):
        asset = self.symbol_to_asset(position_dict['tradingsymbol'])
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
    
    def authenticate(self, *args, **kwargs):
        """
        Parameters
        ----------
        list of user ID/ password or access key, secret key etc.
            Also potentially information to persist logged on credentials
            if required
        
        Returns
        -------
        Boolean
            True on success, else raise exception. Side effect is to se
        """
        if self.access_token:
            return
        
        if not self.access_token:
            if self.request_token and self.secret_key:
                try:
                    sess = self.kite.generate_session("request_token_here", secret="your_secret")
                    self.kite.set_access_token(sess["access_token"])
                    self.access_token = sess["access_token"]
                except:
                    raise AuthenticationError("Failed to obtain access token")      
            
        if not self.access_token:
            raise AuthenticationError("Failed to obtain access token")
            
        return True
            
    def symbol_to_asset(self, tradingsymbol):
        segment_ticker = tradingsymbol.split(':')
        if len(segment_ticker) == 1:
            symbol = segment_ticker[0]
        elif len(segment_ticker) == 2:
            symbol = segment_ticker[1]
        else:
            raise OrdersError('Illegal trading symbol')
        
        symbol_expiry = symbol.split(self.expiry.strftime('%y%b').upper())
        if len(symbol_expiry) == 1:
            symbol = symbol_expiry[0]
        elif len(symbol_expiry) == 2:
            symbol = symbol_expiry[0]+'-I'
            instrument_type = symbol_expiry[1]
            if instrument_type != 'FUT':
                raise OrdersError('Illegal trading symbol')
        else:
            raise OrdersError('Illegal trading symbol')
        
        asset = self.asset_finder.lookup_symbol(symbol,pd.Timestamp.now().tz_localize('Etc/UTC'))
        
        return asset
            
    def asset_to_symbol(self, asset, with_segment=False):
        symbol = asset.symbol
        segment = 'NSE'
        
        if symbol[-2:] == '-I':
            segment = 'NFO'
            symbol = symbol[:-2]+self.expiry.strftime('%y%b').upper()+'FUT'
            
        if(with_segment):
            symbol = segment+':'+symbol
        
        return symbol
    
    def asset_to_exchange(self, asset):
        symbol = asset.symbol
        segment = 'NSE'
        
        if symbol[-2:] == '-I':
            segment = 'NFO'
            
        return segment
            
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
        else:
            raise OrdersError("only market and limit orders are supported")
       
        try:
            self.force_time_out()
            order_id = self.kite.place_order(variety = self.kite.VARIETY_REGULAR,
                            exchange=exchange,
                            tradingsymbol=tradingsymbol,
                            transaction_type=transaction_type,
                            quantity=abs(amount),
                            product=product,
                            order_type=order_type,
                            price=limit, tag=tag)

            logging.info("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            logging.info("Order placement failed: {}".format(e.message))
            
    def cancel(self, order_id):
        try:
            self.force_time_out()
            order_id = self.kite.cancel_order(self, 
                        self.kite.VARIETY_REGULAR, 
                        order_id)
            logging.info("Order ID {} cancel request placed".format(order_id))
        except Exception as e:
            logging.info("Order cancellation failed: {}".format(e.message))
            
    def get_positions(self):
        self.force_time_out()
        positions = self.kite.positions()['net']
        if not positions:
            return
        
        self.positionbook = []
        for p in positions:
            self.positionbook.append(self.kite_dict_to_position(p))
            
        # TODO: test it!
        self.force_time_out()
        holdings = self.kite.holdings()
        if not holdings:
            return
        for p in holdings:
            self.positionbook.append(self.kite_dict_to_position(p))
            
        
    def get_orders(self):
        self.force_time_out()
        orders = self.kite.orders()
        if not orders:
            return
        
        self.orderbook = []
        for o in orders:
            self.orderbook.append(self.kite_dict_to_order(o))
        
    def get_transactions(self):
        self.get_orders()
        
        if not self.orderbook:
            return
        
        self.transactions = []
        for o in self.orderbook:
            self.transactions.append(self.transaction_from_order(o))
    
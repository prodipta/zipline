# -*- coding: utf-8 -*-

import sys
import pandas as pd
import logging

from kiteconnect import KiteConnect

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.live.brokers import Broker, AuthenticationError, OrdersError
from zipline.api import symbol

logging.basicConfig(level=logging.DEBUG)

class ZerodhaBroker(Broker):
    
    def __init__(self, args, **kwargs):
        self.api_key = kwargs.pop('api_key', None)
        self.secret_key = kwargs.pop('secret_key', None)
        self.request_token = kwargs.pop('request_token', None)
        self.access_token = kwargs.pop('access_token', None)
        self.account_id = kwargs.pop('account_id', None)
        
        self.kite = KiteConnect(api_key=self.api_key)
        self.authenticate(args, **kwargs)
        
        self.orderbook = None
        self.positionbook = None
            
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
        pass 
    
    @property
    def transactions(self):
        pass
        
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
            True or false depending on the success
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
            
    def symbol_to_asset(self, tradingsymbol):
        segment = tradingsymbol.split(':')[0]
        if segment == 'NSE':
            asset = symbol(tradingsymbol.split(':')[1])
        else:
            
    
    def asset_to_symbol(self, asset):
        pass
    
    def asset_to_exchange(self, asset):
        pass
    
    def merge_holdings_positions(self, holdings, positions):
        pass
    
    def get_txn_from_orders_trades(self, orders,trades):
        pass
            
    def order(self, asset, amount, style):
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
            order_id = self.kite.place_order(variety = self.kite.VARIETY_REGULAR,
                            exchange=exchange,
                            tradingsymbol=tradingsymbol,
                            transaction_type=transaction_type,
                            quantity=abs(amount),
                            product=product,
                            order_type=order_type,
                            price=limit)

            logging.info("Order placed. ID is: {}".format(order_id))
        except Exception as e:
            logging.info("Order placement failed: {}".format(e.message))
            
    def cancel(self, order_id):
        try:
            order_id = self.kite.cancel_order(self, 
                        self.kite.VARIETY_REGULAR, 
                        order_id)
            logging.info("Order ID {} cancel request placed".format(order_id))
        except Exception as e:
            logging.info("Order cancellation failed: {}".format(e.message))
            
    def get_positions(self):
        positions = self.kite.positions()['net']
        for p in positions:
            
        holdings = self.kite.holdings()
        self.positions = self.merge_holdings_positions(holdings,positions)
        
    def get_orders(self):
        orders = self.kite.orders()
        self.orders = orders
        
    def get_transactions(self):
        orders = self.kite.orders()
        trades = self.kite.trades()
        self.transactions = self.get_txn_from_orders_trades(orders,trades)
    
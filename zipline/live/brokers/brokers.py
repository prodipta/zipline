# -*- coding: utf-8 -*-
"""
Created on Thu May 10 14:36:16 2018

@author: Prodipta
"""
from abc import ABCMeta, abstractmethod, abstractproperty
from six import with_metaclass

class AuthenticationError(Exception):
    """
    Raised when authentication fails
    """
    pass

class NoDataOnPositions(Exception):
    """
    Raised when positions cannot be recovered
    """
    pass

class NoDataOnOrders(Exception):
    """
    Raised when orders cannot be recovered
    """
    pass

class OrdersError(Exception):
    """
    Raised when placing, modifying or cancelling an order fails
    """
    pass

class InstrumentsError(Exception):
    """
    Raised when instruments list request fails
    """
    pass

class Broker(with_metaclass(ABCMeta, object)):
    @abstractproperty
    def name(self):
        raise NotImplementedError('name')
    
    @abstractproperty
    def region(self):
        raise NotImplementedError('region')
    
    @abstractproperty
    def accoundID(self):
        raise NotImplementedError('accountID')
    
    @abstractproperty
    def orders(self):
        """
        Parameters
        ----------
        None. Retrieves all orders sent for the current session

        Returns
        -------
        list
            A list of orders in the given account ID along with status and
            execution details, if any.
        """
        raise NotImplementedError('orders')
        
    @abstractproperty
    def open_orders(self):
        """
        Parameters
        ----------
        None. Retrieves all open orders for the current session

        Returns
        -------
        list
            A list of open orders in the given account ID along with status and
            execution details, if any.
        """
        raise NotImplementedError('open_orders')
    
    @abstractproperty
    def transactions(self):
        """
        Parameters
        ----------
        None. Retrieves all transactions executed for the current session

        Returns
        -------
        list
            A list of orders in the given account ID along with status and
            execution details, if any.
        """
        raise NotImplementedError('transactions')
    
    @abstractproperty
    def positions(self):
        """
        Parameters
        ----------
        None. Retrieves the current list of holdings and positions
            across all available segments (cash, futures, FX) etc.

        Returns
        -------
        list
            A list of positions in the given account ID
        """
        raise NotImplementedError('positions')

    @abstractmethod
    def initialize(self, *args, **kwargs):
        """
        Parameters
        ----------
        list of arguments to initialize the live trading environment
            from broker - like list of instruments, expiries etc.
        
        Returns
        -------
        Boolean
            True or false depending on the success
        """
        raise NotImplementedError('initialize')
    
    @abstractmethod
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
        raise NotImplementedError('authenticate')
    
    @abstractmethod
    def order(self, asset, amount, style):
        """
        Parameters
        ----------
        asset : asset to be ordered
        amount: amount in terms of notionals
        style: style of the order - market, limit, stop, stop-limit etc.

        Returns
        -------
        string
            A uuid of the order as returned by the broker
        """
        raise NotImplementedError('order')
    
    @abstractmethod
    def cancel(self, order_id):
        """
        Parameters
        ----------
        order_id : order to be cancelled

        Returns
        -------
        string
            A uuid of the order as returned by the broker
        """
        raise NotImplementedError('cancel')


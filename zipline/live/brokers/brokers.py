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

class Broker(with_metaclass(ABCMeta, object)):
    @abstractproperty
    def name(self):
        pass
    
    @abstractproperty
    def region(self):
        pass
    
    @abstractproperty
    def accoundID(self):
        pass
    
    @abstractproperty
    def orders(self):
        pass
    
    @abstractproperty
    def transactions(self):
        pass

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
        pass
    
    @abstractmethod
    def get_positions(self):
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
        pass
    
    @abstractmethod
    def get_transactions(self):
        """
        Parameters
        ----------
        None. Retrieves all orders sent for the current day

        Returns
        -------
        list
            A list of orders in the given account ID along with status and
            execution details, if any.
        """
        pass
    
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
        pass
    
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
        pass


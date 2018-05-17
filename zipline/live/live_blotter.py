# -*- coding: utf-8 -*-
"""
Created on Thu May 10 14:36:16 2018

@author: Prodipta
"""
from logbook import Logger
from collections import defaultdict
from logbook import Logger

from zipline.finance.blotter import Blotter
from zipline.utils.input_validation import expect_types
from zipline.assets import Asset
from zipline.finance.cancel_policy import EODCancel


log = Logger('LiveBlotter')
warning_logger = Logger('LiveAlgoWarning')

class LiveBlotter(Blotter):
    def __init__(self, data_frequency, broker):
        self.broker = broker
        self.data_frequency = data_frequency

        # these orders are aggregated by asset
        self.open_orders = defaultdict(list)

        # keep a dict of orders by their own id
        self.orders = {}

        # holding orders that have come in since the last event.
        self.new_orders = []
        self.current_dt = None

        self.max_shares = int(1e+11)
        
        self.data_frequency = data_frequency

        self.cancel_policy = EODCancel()

    def __repr__(self):
        return """
{class_name}(
    open_orders={open_orders},
    orders={orders},
    new_orders={new_orders},
    current_dt={current_dt})
""".strip().format(class_name=self.__class__.__name__,
                   open_orders=self.open_orders,
                   orders=self.orders,
                   new_orders=self.new_orders,
                   current_dt=self.current_dt)

    def set_date(self, dt):
        self.current_dt = dt

    @expect_types(asset=Asset)
    def order(self, asset, amount, style, tag):
        """Place an order.

        Parameters
        ----------
        asset : zipline.assets.Asset
            The asset that this order is for.
        amount : int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.
        style : zipline.finance.execution.ExecutionStyle
            The execution style for the order.
        order_id : str, optional
            The unique identifier for this order.

        Returns
        -------
        order_id : str or None
            The unique identifier for this order, or None if no order was
            placed.

        Notes
        -----
        amount > 0 :: Buy/Cover
        amount < 0 :: Sell/Short
        Market order:    order(asset, amount)
        Limit order:     order(asset, amount, style=LimitOrder(limit_price))
        Stop order:      order(asset, amount, style=StopOrder(stop_price))
        StopLimit order: order(asset, amount, style=StopLimitOrder(limit_price,
                               stop_price))
        """
        # something could be done with amount to further divide
        # between buy by share count OR buy shares up to a dollar amount
        # numeric == share count  AND  "$dollar.cents" == cost amount

        if amount == 0:
            # Don't bother placing orders for 0 shares.
            return None
        elif amount > self.max_shares:
            # Arbitrary limit of 100 billion (US) shares will never be
            # exceeded except by a buggy algorithm.
            raise OverflowError("Can't order more than %d shares" %
                                self.max_shares)

        order_id = self.broker.order(asset, amount, style, tag)

        return order_id

    def cancel(self, order_id, relay_status=True):
        if order_id not in self.orders:
            return

        cur_order = self.orders[order_id]

        if cur_order.open:
            order_list = self.open_orders[cur_order.asset]
            if cur_order in order_list:
                order_list.remove(cur_order)

            if cur_order in self.new_orders:
                self.new_orders.remove(cur_order)
            cur_order.cancel()
            cur_order.dt = self.current_dt

            if relay_status:
                # we want this order's new status to be relayed out
                # along with newly placed orders.
                self.new_orders.append(cur_order)

    def cancel_all_orders_for_asset(self, asset, warn=False,
                                    relay_status=True):
        """
        For a live blotter with EOD cancel policy we never have to do this
        """
        raise ValueError('Unexpected cancel_all_orders_for_asset function call from blotter')

    def execute_cancel_policy(self, event):
        """
        For a live blotter with EOD cancel policy we never have to do this
        """
        raise ValueError('Unexpected execute_cancel_policy function call from blotter')

    def reject(self, order_id, reason=''):
        """
        For a live blotter we never have to do this
        """
        raise ValueError('Unexpected reject function call from blotter')

    def hold(self, order_id, reason=''):
        """
        For a live blotter we never have to do this
        """
        raise ValueError('Unexpected hold function call from blotter')

    def process_splits(self, splits):
        """
        For a live blotter with EOD cancel policy we should never need this
        """
        raise ValueError('Unexpected process_splits function call from blotter')

    def get_transactions(self, bar_data):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: zipline._protocol.BarData

        Notes
        -----
        This method book-keeps the blotter's open_orders dictionary, so that
         it is accurate by the time we're done processing open orders.

        Returns
        -------
        transactions_list: List
            transactions_list: list of transactions resulting from the current
            open orders.  If there were no open orders, an empty list is
            returned.

        commissions_list: List
            commissions_list: list of commissions resulting from filling the
            open orders.  A commission is an object with "asset" and "cost"
            parameters.

        closed_orders: List
            closed_orders: list of all the orders that have filled.
        """

        closed_orders = []
        transactions = []
        commissions = []

        if self.open_orders:
            for asset, asset_orders in iteritems(self.open_orders):
                slippage = self.slippage_models[type(asset)]

                for order, txn in \
                        slippage.simulate(bar_data, asset, asset_orders):
                    commission = self.commission_models[type(asset)]
                    additional_commission = commission.calculate(order, txn)

                    if additional_commission > 0:
                        commissions.append({
                            "asset": order.asset,
                            "order": order,
                            "cost": additional_commission
                        })

                    order.filled += txn.amount
                    order.commission += additional_commission

                    order.dt = txn.dt

                    transactions.append(txn)

                    if not order.open:
                        closed_orders.append(order)

        return transactions, commissions, closed_orders

    def prune_orders(self, closed_orders):
        """
        Removes all given orders from the blotter's open_orders list.

        Parameters
        ----------
        closed_orders: iterable of orders that are closed.

        Returns
        -------
        None
        """
        # remove all closed orders from our open_orders dict
        for order in closed_orders:
            asset = order.asset
            asset_orders = self.open_orders[asset]
            try:
                asset_orders.remove(order)
            except ValueError:
                continue

        # now clear out the assets from our open_orders dict that have
        # zero open orders
        for asset in list(self.open_orders.keys()):
            if len(self.open_orders[asset]) == 0:
                del self.open_orders[asset]


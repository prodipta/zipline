# -*- coding: utf-8 -*-

import sys

# TODO: This is a hack, install the correct version
zp_path = "C:/Users/academy.academy-72/Documents/python/zipline/"
sys.path.insert(0, zp_path)
# TODO: End of hack part

from zipline.finance.order import Order

class LiveOrder(Order):
    __slots__ = ["tag","price", "validity", "parent_id"]
    
    def __init__(self, dt, asset, amount, stop=None, limit=None, filled=0,
                 commission=0, id=None, tag=None, price=0, validity=None,
                 parent_id=None):
        
        self.tag = tag
        self.price = price
        self.validity = validity
        self.parent_id = parent_id
        
        super(LiveOrder, self).__init__(dt, asset, amount, stop, 
             limit, filled, commission, id)
        
        
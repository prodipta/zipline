# -*- coding: utf-8 -*-
"""
Created on Fri Apr 06 18:07:12 2018

@author: Prodipta
"""

import pandas as pd
import os
import datetime
import shutil

from zipline.data.bundles import register
from zipline.data.bundles.gdfl import gdfl_minutedata

from zipline.data import bundles as bundles_module

def call_ingest(bundle_name, calendar_name, start_date, end_date):
    register(bundle_name, gdfl_minutedata(),calendar_name=calendar_name,
             start_session=start_date,end_session=end_date,
             create_writers=False)
    
    bundles_module.ingest(bundle_name,os.environ,pd.Timestamp.utcnow())
    print("ingest done")


def call_ingest_in_loop_gdfl(start_date, end_date, datapath, inputpath):
    delta = end_date - start_date
    dts = [(start_date + datetime.timedelta(days=x)).strftime('%d%m%Y') for x in range(0, delta.days+1)]
    all_files = os.listdir(datapath)

    for dt in dts:
        try:
            print("looking for {}".format(dt))
            files = [fname for fname in all_files if dt in fname]
            if(files):
                for f in files:
                    full_fname = os.path.join(datapath, f)
                    shutil.copy(full_fname, inputpath)
                print("copied {} files".format(len(files)))
                print("calling ingest function...")
                call_ingest('gdfl-bundle','GDFL',start_date,end_date)
                print("done")
        except:
            pass



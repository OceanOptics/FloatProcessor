# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 16:40:35
# @Last Modified by:   nils
# @Last Modified time: 2017-03-04 11:46:03

# DASHBOARD: update json files for web dashboard
#     float_list.json
#     float_status.json
#     google_map.json

import simplejson
import os
import numpy as np
from datetime import datetime

######################
#  DASHBOARD FIELDS  #
######################
# fields for profile
PROFILE_FIELDS = ['p','par','t','s','chla','poc','fdom','o2_c']
PROFILE_FIELDS_MANDATORY = ['p','t','s']
# fields for timeseries
TIMESERIES_FIELDS = ['profile_id', 'dt','mld','t','s','chla','poc','fdom','o2_c']
TIMESERIES_FIELDS_MANDATORY = ['profile_id', 'dt','mld','t','s']

def update_float_status(filename, float_id, wmo='undefined',
                        profile_n=-1, dt_last='undefined',
                        dt_first='undefined', status='undefined',
                        institution='undefined', project='undefined'):
    # UPDATE_FLOAT_STATUS: update json file of float status
    # EXAMPLE:
    #   update_float_status('float_status.json', 'n0572', wmo='5902462',
    #     dt_last=datetime.today())

    # load previous float status
    with open(filename) as data_file:
        fs = simplejson.load(data_file)

    # set date of update in zulu time
    dt_update = datetime.utcnow()
    fs[float_id]['dt_update'] = dt_update.strftime('%d-%b-%Y %H:%M:%S')
    # update wmo
    if wmo != 'undefined':
        fs[float_id]['wmo'] = wmo
    # update profile number
    if profile_n != -1:
        fs[float_id]['profile_n'] = profile_n
    # update institution
    if institution != 'undefined':
        fs[float_id]['institution'] = institution
    # update project
    if project != 'undefined':
        fs[float_id]['project'] = project
    # update date of last report
    if dt_last != 'undefined':
        fs[float_id]['dt_last'] = dt_last.strftime('%d-%b-%Y %H:%M:%S')
    else:
        dt_last = datetime.strptime(
            fs[float_id]['dt_last'], '%d-%b-%Y %H:%M:%S')
    # update days since last report
    delta_last = dt_update - dt_last
    fs[float_id]['days_last'] = delta_last.days
    # update date of first report
    if dt_first != 'undefined':
        fs[float_id]['dt_first'] = dt_first.strftime('%d-%b-%Y %H:%M:%S')
    else:
        dt_first = datetime.strptime(
            fs[float_id]['dt_first'], '%d-%b-%Y %H:%M:%S')
    # update days since first report
    delta_first = dt_update - dt_first
    fs[float_id]['days_last'] = delta_first.days
    # update float status
    if status != 'undefined':
        fs[float_id]['status'] = status
    elif delta_last.days > 15:
        fs[float_id]['status'] = 'lost'
    else:
        fs[float_id]['status'] = 'active'

    with open(filename, 'w') as outfile:
        simplejson.dump(fs, outfile)

def export_msg_to_json_profile(_msg, _path, _usr_id, _msg_id):
    # Profile of each cast for all variables
    #   p
    #   par
    #   temperature
    #   salinity
    #   chla
    #   poc
    #   fdom
    #   o2

    # Check input
    if 'obs' not in _msg.keys():
        print('ERROR: Missing key obs in msg.')
        return -1
    # Set filename
    filename = os.path.join(_path, _usr_id + '.' +
                            _msg_id + '.profile.json')
    # Extract data
    fs = dict()
    for f in PROFILE_FIELDS:
        if f not in _msg['obs'].keys():
            if f in PROFILE_FIELDS_MANDATORY:
                print('ERROR: Missing key ' + f + ' in msg[obs].')
                return -1
            else:
                continue
        # Convert np.array to list
        if isinstance(_msg['obs'][f], np.ndarray):
            fs[f] = _msg['obs'][f].tolist()
        else:
            fs[f] = _msg['obs'][f]

    # Write json
    with open(filename, 'w') as outfile:
        simplejson.dump(fs, outfile, ignore_nan=True,default=datetime.isoformat)
        return 0
    return -1

def export_msg_to_json_timeseries(_msg, _path, _usr_id, _reset=False):
    # Time serie (mean, std and median, 5,95percentile in MLD)
    #   profile id
    #   datetime
    #   MLD
    #   temperature
    #   salinity
    #   chla
    #   poc
    #   fdom
    #   o2

    # Check input
    if 'obs' not in _msg.keys():
        print('ERROR: Missing key obs in msg.')
        return -1
    if 'mld_index' not in _msg.keys():
        print('ERROR: Missing key mld_index in msg.')
        return -1
    # Set filename
    filename = os.path.join(_path, _usr_id + '.timeseries.json')

    # Load existing timeseries (if available)
    if os.path.isfile(filename) and not _reset:
        with open(filename) as data_file:
            fs = simplejson.load(data_file)
    else:
        fs = dict()
        for f in TIMESERIES_FIELDS:
            fs[f] = list()
            fs[f + '_std'] = list()

    # Extract data
    for f in TIMESERIES_FIELDS:
        if f in _msg.keys():
            # field with one value
            fs[f].append(_msg[f])
        elif f in _msg['obs'].keys():
            # average in MLD
            fs[f].append(np.nanmean(_msg['obs'][f]))
            fs[f+'_std'].append(np.nanstd(_msg['obs'][f]))
        elif f in TIMESERIES_FIELDS_MANDATORY:
            print('ERROR: Missing key ' + f + ' in msg|msg[obs].')
            return -1
        else:
            fs[f].append(np.NaN)

    # Remove duplicates and sort data by profile id

    # Write json
    with open(filename, 'w') as outfile:
        simplejson.dump(fs, outfile, ignore_nan=True,default=datetime.isoformat)
        return 0
    return -1

def export_msg_to_json_overview():
    # Overview figure with depth vs time vs observations
    pass

def export_msg_to_json_map():
    pass
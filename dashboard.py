# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 16:40:35
# @Last Modified by:   nils
# @Last Modified time: 2017-04-01 16:05:36

# DASHBOARD: update json files for web dashboard
#     float_list.json
#     float_status.json
#     google_map.json

import simplejson
import os
import numpy as np
from datetime import datetime
from collections import OrderedDict

######################
#  DASHBOARD FIELDS  #
######################
# fields for profile
PROFILE_FIELDS = ['p','par','t','s','chla','bbp','fdom','o2_c']
PROFILE_FIELDS_MANDATORY = ['p','t','s','chla']
# fields for timeseries
TIMESERIES_FIELDS = ['profile_id', 'dt','mld','t','s','chla','bbp','fdom','o2_c']
TIMESERIES_FIELDS_MANDATORY = ['profile_id', 'dt','mld','t','s', 'chla']

def update_float_status(_filename, _float_id, _wmo='undefined',
                        _profile_n=-1, _dt_last='undefined',
                        _dt_first='undefined', _status='undefined',
                        _institution='undefined', _project='undefined',
                        _reset=False):
    # UPDATE_FLOAT_STATUS: update json file of float status
    # EXAMPLE:
    #   update_float_status('float_status.json', 'n0572', _wmo='5902462',
    #     _dt_last=datetime.today())

    # load current float status
    if os.path.isfile(_filename) and not _reset:
        with open(_filename) as data_file:
            fs = simplejson.load(data_file, object_pairs_hook=OrderedDict)
        if _float_id not in fs.keys():
            fs[_float_id] = dict()
            fs[_float_id]['float_id'] = _float_id
    else:
        fs = OrderedDict()
        fs[_float_id] = dict()

    # set date of update in zulu time
    dt_update = datetime.utcnow()
    fs[_float_id]['dt_update'] = dt_update.strftime('%d-%b-%Y %H:%M:%S')
    # update wmo
    if _wmo != 'undefined':
        fs[_float_id]['wmo'] = _wmo
    # update profile number
    if _profile_n != -1:
        fs[_float_id]['profile_n'] = _profile_n
    # update institution
    if _institution != 'undefined':
        fs[_float_id]['institution'] = _institution
    # update project
    if _project != 'undefined':
        fs[_float_id]['project'] = _project
    # update date of last report
    if _dt_last != 'undefined':
        fs[_float_id]['dt_last'] = _dt_last.strftime('%d-%b-%Y %H:%M:%S')
        dt_last = _dt_last
    else:
        dt_last = datetime.strptime(
            fs[_float_id]['dt_last'], '%d-%b-%Y %H:%M:%S')
    # update days since last report
    delta_last = dt_update - dt_last
    fs[_float_id]['days_last'] = delta_last.days
    # update date of first report
    if _dt_first != 'undefined':
        fs[_float_id]['dt_first'] = _dt_first.strftime('%d-%b-%Y %H:%M:%S')
    elif ('dt_first' not in fs[_float_id].keys() or
          fs[_float_id]['dt_first'] == 'undefined') and _profile_n == 0:
        fs[_float_id]['dt_first'] = dt_last.strftime('%d-%b-%Y %H:%M:%S')
    # update days since first report
    if 'dt_first' in fs[_float_id].keys():
        dt_first = datetime.strptime(
            fs[_float_id]['dt_first'], '%d-%b-%Y %H:%M:%S')
        delta_first = dt_update - dt_first
        fs[_float_id]['days_first'] = delta_first.days
    # update float status
    if _status != 'undefined':
        fs[_float_id]['status'] = _status
    elif delta_last.days > 15:
        fs[_float_id]['status'] = 'inactive'
    else:
        fs[_float_id]['status'] = 'active'

    with open(_filename, 'w') as outfile:
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
        print('ERROR: Missing key mld_index in msg ' + str(_msg['profile_id']) + '.')
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
            fs[f].append(np.nanmedian(_msg['obs'][f]))
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
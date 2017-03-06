# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 14:44:34
# @Last Modified by:   nils
# @Last Modified time: 2017-03-04 11:54:52

# PROCESS: this module simplify data procesing using the toolbox module
#   1. import data
#         from raw files of Navis profilers
#         configuration and calibration files of profilers
#   2. process data to L1, and L2
#   3. export data to csv
#   4. core processing functions
#       RT for real time processing
#       Bash for reprocessing a list of float

from datetime import datetime
import os
import csv
import json
from collections import OrderedDict
import gsw
from toolbox import *
from dashboard import *


###########################
#  SENSOR SPECIFICATIONS  #
###########################
# Backscattering theta and lambda
MCOM_BETA_CENTROID_ANGLE = 150
MCOM_BETA_WAVELENGTH = 700
FLBB_BETA_CENTROID_ANGLE = 142
FLBB_BETA_WAVELENGTH = 700
FLNTU_BETA_CENTROID_ANGLE = 142
FLNTU_BETA_WAVELENGTH = 700
ECO1C_BETA_CENTROID_ANGLE = 124
ECO1C_BETA_WAVELENGTH = 700
ECO2C_BETA_CENTROID_ANGLE = 142
ECO2C_BETA_WAVELENGTH = 700
ECO3C_BETA_CENTROID_ANGLE = 124
ECO3C_BETA_WAVELENGTH = 700


#############################
#  USER CFG SPECIFICATIONS  #
#############################
# Field to ignore to retreive variables from a sensor
LIST_SENSOR_SPECIAL_FIELDS = ['model', 'sn', 'fw',
                              'wavelength', 'pathlength']


###################
#   IMPORT DATA   #
###################


def import_msg(filename):
    # Simple function to import a file from  Navis float
    #   Convert binary data from raw msg to ASCII (L0)
    valid_obs_len = [60, 72]
    valid_obs_len = [e + 1 for e in valid_obs_len]

    f = open(filename, 'r')
    d = dict()
    obs = {"p": list(), "t": list(), "s": list(), "o2_ph": list(),
           "o2_t": list(), "fchl": list(), "beta": list(), "fdom": list(),
           "par": list(), "tilt": list(), "tilt_std": list()}
    park_obs = {"dt": list(), "p": list(), "t": list(),
                "s": list(), "o2_ph": list(), "o2_t": list()}
    obs_begin = False
    obs_end = False
    crv_on = False
    for l in f:
        # Get float_id and profile_od
        if l.find('$ FloatId') != -1:
            d['float_id'] = int(float(l[-6:-2]))
            continue
        if l.find('FloatId') != -1:
            if 'float_id' not in d.keys():
                d['float_id'] = int(float(l[-5:-1]))
                continue
        if l.find('ProfileId=') != -1:
            d['profile_id'] = int(float(l[-4:-1]))
            continue

        # Get date of end of profile
        foo = l.find('terminated')
        if foo != -1:
            d['dt'] = datetime.strptime(l[foo + 12:-1], '%a %b %d %H:%M:%S %Y')
            continue

        # Get position
        if l.find('Fix:') != -1:
            s = [e for e in l.split(' ') if e]  # keep only non empty str
            d['lat'] = float(s[2])
            d['lon'] = float(s[1])
            # save date if not done yet
            if 'dt' not in d.keys():
                d['dt'] = datetime.strptime(s[3] + s[4], '%m/%d/%Y%H%M%S')
            continue

        # Check if Crover embedded
        if (l.find('cRover') != -1 or
            l.find('Crover') != -1) and not crv_on:
            crv_on = True
            obs['c_count'] = list()
            obs['c_su'] = list()

        # Get park observations
        if l.find('ParkObs:') != -1:
            s = [e for e in l.split(' ') if e]  # keep only non empty str
            park_obs['dt'].append(
                datetime.strptime(s[1] + s[2] + s[3] + s[4], '%b%d%Y%H:%M:%S'))
            park_obs['p'].append(float(s[5]))
            park_obs['t'].append(float(s[6]))
            park_obs['s'].append(float(s[7]))
            park_obs['o2_ph'].append(float(s[8]))
            park_obs['o2_t'].append(float(s[9][0:-1]))

        # Get profile observation
        if l.find('ser1') != -1:
            obs_begin = True
            continue
        if l.find('Resm') != -1:
            obs_end = True
            continue
        if obs_begin and not obs_end and len(l) in valid_obs_len:
            # Variables are 16-bit hex-encoded
            # Get pressure (dBar)
            foo = int(l[0:4], 16)
            if foo < 32768:
                obs['p'].append(float(foo) / 10.0)
            elif foo > 32768:
                obs['p'].append((float(foo) - 65536.0) / 10.0)
            else:
                obs['p'].append(float('nan'))
            # Get temperature (degC)
            foo = int(l[4:8], 16)
            if foo < 61440:
                obs['t'].append(float(foo) / 1000.0)
            elif foo > 61440:
                obs['t'].append((float(foo) - 65536.0) / 1000.0)
            else:
                obs['t'].append(float('nan'))
            # Get salinity (no units)
            foo = int(l[8:12], 16)
            if foo < 61440:
                obs['s'].append(float(foo) / 1000.0)
            elif foo > 61440:
                obs['s'].append((float(foo) - 65536.0) / 1000.0)
            else:
                obs['s'].append(float('nan'))
            # Get O2 phase
            foo = int(l[14:20], 16)
            if foo == 16777215:
                obs['o2_ph'].append(float('nan'))
            else:
                obs['o2_ph'].append(float(foo) / 100000.0 - 10.0)
            # Get O2T (volts)
            foo = int(l[20:26], 16)
            if foo == 16777215:
                obs['o2_t'].append(float('nan'))
            else:
                obs['o2_t'].append(float(foo) / 1000000.0 - 1.0)
            # Get fchl
            foo = int(l[28:34], 16)
            if foo == 16777215:
                obs['fchl'].append(float('nan'))
            else:
                obs['fchl'].append(foo - 500)
            # Get beta
            foo = int(l[34:40], 16)
            if foo == 16777215:
                obs['beta'].append(float('nan'))
            else:
                obs['beta'].append(foo - 500)
            # Get fdom
            foo = int(l[40:46], 16)
            if foo == 16777215:
                obs['fdom'].append(float('nan'))
            else:
                obs['fdom'].append(foo - 500)
            # If Crover embedded
            if crv_on:
                # Get Crover
                foo = int(l[48:52], 16)
                if foo == 65535:
                    obs['c_count'].append(float('nan'))
                else:
                    obs['c_count'].append(foo - 200)
                foo = int(l[52:58], 16)
                if foo == 16777215:
                    obs['c_su'].append(float('nan'))
                else:
                    obs['c_su'].append(float(foo) / 1000.0 - 10.0)
                # Get PAR
                foo = int(l[60:66], 16)
                if foo == 16777215:
                    obs['par'].append(float('nan'))
                else:
                    obs['par'].append(foo)
                foo = int(l[68:70], 16)
                if foo == 255:
                    obs['tilt'].append(float('nan'))
                else:
                    obs['tilt'].append(float(foo) / 10.0)
                foo = int(l[70:72], 16)
                if foo == 255:
                    obs['tilt_std'].append(float('nan'))
                else:
                    obs['tilt_std'].append(float(foo) / 100.0)
            else:
                # Get PAR (if no crover)
                foo = int(l[48:54], 16)
                if foo == 16777215:
                    obs['par'].append(float('nan'))
                else:
                    obs['par'].append(foo)
                foo = int(l[56:58], 16)
                if foo == 255:
                    obs['tilt'].append(float('nan'))
                else:
                    obs['tilt'].append(float(foo) / 10.0)
                foo = int(l[58:60], 16)
                if foo == 255:
                    obs['tilt_std'].append(float('nan'))
                else:
                    obs['tilt_std'].append(float(foo) / 100.0)

            # TODO decode rest of line of data
            # if crv_on:
            #   print(p, t, s, o2, o2t, fchl, beta, fdom,
            #         c_count, c_su, par, tilt, tilt_std)
            # else:
            #   print(p, t, s, o2, o2t, fchl, beta, fdom, par, tilt, tilt_std)
            # continue

        # Check if file is complete
        if l.find('<EOT>') != -1:
            d['EOT'] = True
            continue

    d['obs'] = obs
    d['park_obs'] = park_obs
    f.close()
    return d

def import_usr_cfg(_filename):
    # Import float configuration file
    #   filename must specify path to a json

    # Load file
    with open(_filename) as data_file:
        d = json.load(data_file, object_pairs_hook=OrderedDict)
        # TODO check required field
        return d

def import_app_cfg(_filename):
    # Import application configuration file and check missing fields
    #
    # INPUT:
    #   _filename <string> name of configuration json file to load
    #
    # OUTPUT:
    #   d <dictionnary> application configuration
    #       or
    #   -1 if error during process or missing variable

    # Load file
    with open(_filename) as data_file:
        d = json.load(data_file)
        # Check process module
        if 'process' not in d.keys():
            print(_filename + ': missing variable process')
            return -1
        else:
            if 'active' not in d['process'].keys():
                print(_filename + ': missing variable process:active')
                return -1
            else:
                if 'bash' not in d['process']['active'].keys():
                    print(_filename + ': missing variable process:active:bash')
                    return -1
                if 'rt' not in d['process']['active'].keys():
                    print(_filename + ': missing variable process:active:rt')
                    return -1
            if 'path' not in d['process'].keys():
                print(_filename + ': missing variable process:path')
                return -1
            else:
                if 'usr_cfg' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:usr_cfg')
                    return -1
                if 'msg' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:msg')
                    return -1
                if 'raw' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:raw')
                    return -1
                if 'level' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:level')
                    return -1
        # Check dashboard module
        if 'dashboard' not in d.keys():
            print(_filename + ': missing variable dashboard')
            return -1
        else:
            if 'active' not in d['dashboard'].keys():
                print(_filename + ': missing variable dashboard:active')
                return -1
            else:
                if 'bash' not in d['dashboard']['active'].keys():
                    print(_filename + ': missing variable dashboard:active:bash')
                    return -1
                if 'rt' not in d['dashboard']['active'].keys():
                    print(_filename + ': missing variable dashboard:active:rt')
                    return -1
            if 'path' not in d['dashboard'].keys():
                print(_filename + ': missing variable dashboard:path')
                return -1
            else:
                if 'dir' not in d['dashboard']['path'].keys():
                    print(_filename + ': missing variable dashboard:path:dir')
                    return -1
                if 'usr_status' not in d['dashboard']['path'].keys():
                    print(_filename + ': missing variable dashboard:path:usr_status')
                    return -1
        return d
    return -1


####################
#   PROCESS DATA   #
####################


def process_L1(_msg, _usr_cfg):
    # Process data to level 1: apply calibration and compute new products
    #   conversion from counts to scientific units
    #
    # INPUT:
    #   _msg dictionnary containing float profile at level 0
    #             usually loaded with import_msg
    #   _usr_cfg dictionnary containing float configuration
    #             usually loaded with import_cfg
    #
    # OUTPUT:
    #   l1 <msg_struct> level 1 profile
    #       or
    #   -1 if error during process

    # Set Level 1
    l1 = dict()
    l1['obs'] = dict()
    for key, val in _msg.items():
        if key is not 'obs':
            l1[key] = val

    # Calibrate observations (_msg['obs'])
    #   for each sensor (_usr_cfg['sensors'])
    for sensor_key, sensor_val in _usr_cfg['sensors'].items():
        if sensor_key == 'CTD':
            if sensor_val['model'] == 'SBE41CP':
                for var_key, var_val in sensor_val.items():
                    # Skip special fields
                    if var_key in LIST_SENSOR_SPECIAL_FIELDS:
                        continue
                    # Check that field is in profile from float
                    if var_key not in _msg['obs'].keys():
                        print('ERROR: Missing variable ' + var_key +
                              ' in msg.')
                        return -1
                    # Copy value to level 1
                    l1['obs'][var_key] = np.array(_msg['obs'][var_key],
                                                  dtype='float')
            else:
                print('ERROR: Unknow CTD Model ' + sensor_val['model'])
                return -1
        elif sensor_key == 'O2':
            if sensor_val['model'] == 'SBE63':
                # Check that field is in configuration file
                if ('o2_t' not in sensor_val.keys() or
                        'o2_ph' not in sensor_val.keys()):
                    print('ERROR: Missing variable o2_t or o2_ph'
                          ' in <usr>_cfg.json file.')
                    return -1
                # Check that field is in profile from float
                if ('o2_t' not in _msg['obs'].keys() or
                        'o2_ph' not in _msg['obs'].keys()):
                    print('ERROR: Missing variable o2_t or o2_ph in msg.')
                    return -1
                # Apply calibration
                l1['obs']['o2_t'] = o2_t_calibration(
                    np.array(_msg['obs']['o2_t'], dtype='float'),
                    sensor_val['o2_t'])
                l1['obs']['o2_c'] = o2_phase_calibration(
                    np.array(_msg['obs']['o2_ph'], dtype='float'),
                    l1['obs']['o2_t'], sensor_val['o2_ph'])
            else:
                print('ERROR: Unknow O2 Model ' + sensor_val['model'])
                return -1
        elif sensor_key == 'ECO':
            if sensor_val['model'] in ['MCOM', 'FLBBCD']:
                for var_key, var_val in sensor_val.items():
                    # Skip special fields
                    if var_key in LIST_SENSOR_SPECIAL_FIELDS:
                        continue
                    # Check that field is in profile from float
                    if var_key not in _msg['obs'].keys():
                        print('ERROR: Missing variable ' + var_key +
                              ' in msg.')
                        return -1
                    # Apply calibration
                    l1['obs'][var_key] = eco_calibration(
                        np.array(_msg['obs'][var_key], dtype='float'),
                        var_val)
            else:
                print('ERROR: Unknow ECO Model ' + sensor_val['model'])
                return -1
        elif sensor_key == 'Radiometer':
            if sensor_val['model'] == 'Satlantic PAR':
                # Check that field is in configuration file
                if ('par' not in sensor_val.keys() or
                    'tilt' not in sensor_val.keys() or
                    'tilt_std' not in sensor_val.keys()):
                    print('ERROR: Missing variable o2_t or o2_ph'
                          ' in <usr>_cfg.json file.')
                    return -1
                # Check that field is in profile from float
                if ('par' not in _msg['obs'].keys() or
                    'tilt' not in _msg['obs'].keys() or
                    'tilt_std' not in _msg['obs'].keys()):
                    print('ERROR: Missing variable o2_t or o2_ph in msg.')
                    return -1
                # Apply calibration (if necessary)
                l1['obs']['par'] = radiometer_calibration(
                    np.array(_msg['obs']['par'], dtype='float'),
                    sensor_val['par'])
                l1['obs']['tilt'] = np.array(_msg['obs']['tilt'],
                                             dtype='float')
                l1['obs']['tilt_std'] = np.array(_msg['obs']['tilt_std'],
                                                 dtype='float')
            else:
                print('ERROR: Unknow Radiometer Model ' + sensor_val['model'])
                return -1
        elif sensor_key == 'BeamC':
            if sensor_val['model'] == 'CRV2K':
                for var_key, var_val in sensor_val.items():
                    # Skip special fields
                    if var_key in LIST_SENSOR_SPECIAL_FIELDS:
                        continue
                    # Check that field is in profile from float
                    if var_key not in _msg['obs'].keys():
                        print('ERROR: Missing variable ' + var_key +
                              ' in msg.')
                        return -1
                    # Copy value to level 1
                    l1['obs'][var_key] = np.array(_msg['obs'][var_key],
                                                  dtype='float')
            else:
                print('ERROR: Unknow Radiometer Model ' + sensor_val['model'])
                return -1
        else:
            print('ERROR: Unknow sensor type ' + sensor_key)
            return -1

    return l1


def process_L2(_l1, _usr_cfg):
    # Process data to level 2: apply corrections and compute new products
    #
    # DATA ADJUSTMENTS
    #   chlorophyll a fluorescence (fchl) is corrected for
    #     non-photochemical quenching (NPQ) with Xing et al. 2012
    #     CDOM fluorescence (based on minimum value for dark)
    #   oxygen concentration (o2_c) is corrected for
    #     pressure (correction from Dan Quittman, Sea-Bird Electronics)
    #     salinity (correction from Dan Quittman, Sea-Bird Electronics)
    #
    # NEW PRODUCTS
    #   density (sigma) is estimated from
    #     temperature (t) and pratical salinity (s)
    #   particulate backscattering (bbp) is estimated from
    #     angular scatterance (beta)
    #   particulate organic carbon (POC) is estimated from
    #     particulate backscattering (bbp)
    #   phytoplankton carbon (Cphyto) is estimated from
    #     particulate backscattering (beta)
    #
    # INPUT:
    #   _l1 dictionnary containing float profile at level 1
    #             usually processed with process_L1 (obsercations are np.array)
    #   _usr_cfg dictionnary containing float configuration
    #             usually loaded with import_cfg
    #
    # OUTPUT:
    #   l2 <msg_struct> level 2 profile
    #       or
    #   -1 if error during process

    # Set Level 2
    l2 = dict()
    l2['obs'] = dict()
    for key, val in _l1.items():
        if key is not 'obs':
            l2[key] = val

    # Check that mandaroty fields are present
    if ('p' not in _l1['obs'].keys() or
        's' not in _l1['obs'].keys() or
            'par' not in _l1['obs'].keys()):
        print('ERROR: Missing observation p, s or par')
        return -1

    # Adjust observations (if necessary)
    for key, val in _l1['obs'].items():
        if key == 'fchl':
            # Apply CDOM correction
            # TODO: Implement CDOM correction from Xing for fchl
            fchl_cdomc = val
            # Apply NPQ correction
            start_npq = is_npq(_l1['obs']['p'], _l1['obs']['par'])
            if start_npq:
                fchl_npqc = npq_correction(_l1['obs']['p'],
                                           fchl_cdomc, start_npq,
                                           _method='Xing2')
            else:
                fchl_npqc = fchl_cdomc
            # Save corrected data
            l2['obs']['fl'] = fchl_cdomc   # fluorescence chlorophyll a (fl)
            l2['obs']['chla'] = fchl_npqc  # chlorophyll a (chla)
        # elif key == 'o2_c':
        #     # Compute pressure and salinity correction
        #     o2_p_corr = o2_pressure_correction(_l1['obs']['o2_t'],
        #                                        _l1['obs']['p'])
        #     o2_s_corr = o2_salinity_correction(_l1['obs']['o2_t'],
        #                                        _l1['obs']['s'])
        #     # Apply corrections
        #     l2['obs']['o2_c'] = np.array([_l1['obs']['o2_c'][i] * o2_p_corr[i] * o2_s_corr[i]
        #                for i in range(0, len(_l1['obs']['o2_c']))], dtype='float')
        elif key == 'beta':
            # Check usr_cfg fields
            if 'ECO' not in _usr_cfg['sensors']:
                print('ERROR: Missing sensor ECO in usr_cfg.')
                return -1
            if 'beta' not in _usr_cfg['sensors']['ECO']:
                print('ERROR: Missing observation beta in sensor ECO'
                      'in user_cfg')
                return -1
            # Get centroid angle and wavelength from sensor
            if _usr_cfg['sensors']['ECO']['model'] == 'MCOM':
                beta_theta = MCOM_BETA_CENTROID_ANGLE
                beta_lambda = MCOM_BETA_WAVELENGTH
            elif _usr_cfg['sensors']['ECO']['model'] == 'FLBB':
                beta_theta = FLBB_BETA_CENTROID_ANGLE
                beta_lambda = FLBB_BETA_WAVELENGTH
            else:
                print('ERROR: Unknow centroid angle and wavelength of sensor')
                return -1
            # Estimate bbp
            l2['obs']['bbp'] = estimate_bbp(_l1['obs']['beta'],
                                            _l1['obs']['t'], _l1['obs']['s'],
                                            _lambda=beta_lambda,
                                            _theta=beta_theta)
        elif key == 'c_count':
            # Ignore count from beam c as we already have scientific units (su)
            continue
        else:
            # No correction needed
            l2['obs'][key] = val

    # Compute new products
    if ('t' in l2['obs'].keys() and 's' in l2['obs'].keys()):
        # Estimate absolute salinity
        sa = gsw.SA_from_SP(l2['obs']['s'],l2['obs']['p'],l2['lon'],l2['lat'])
        # Estimate pratical temperature
        ct = gsw.CT_from_t(sa,l2['obs']['t'],l2['obs']['p'])
        # Estimate density
        l2['obs']['sigma'] = gsw.rho(sa,l2['obs']['t'],l2['obs']['p']) - 1000
    if 'sigma' in l2['obs'].keys():
        if l2['obs']['sigma'] != []:
            l2['mld_index'] = estimate_mld(l2['obs']['sigma'], _criterion=0.125)
            l2['mld'] = l2['obs']['p'][l2['mld_index']]
    if 'bbp' in l2['obs'].keys():
        # Estimate POC
        l2['obs']['poc'] = estimate_poc(l2['obs']['bbp'],
                                        _lambda=beta_lambda)['poc']
        # Estimate Cphyto
        l2['obs']['cphyto'] = estimate_cphyto(l2['obs']['bbp'],
                                              _lambda=beta_lambda)

    return l2


###################
#   EXPORT DATA   #
###################


def export_csv(_msg, _usr_cfg, _app_cfg, _proc_level,
               _filename=None, _sub_dir_user=False):
    # Save profile (_msg) in a csv file
    #
    #
    # INPUT:
    #   _msg dictionnary containing float profile
    #   _usr_cfg <dictionnary> float configuration
    #   _app_cfg <dictionnary> application configuration
    #   _proc_level <string> RAW, L0, L1 or L2 corresponding to the output
    #           directory in which the data should be exported
    #   _filename <string> name of file of csv file exported
    #       default: <usr_id>.<msg_id>.csv
    #   _sub_dir_user <bool> create a sub directory <usr_id>
    #           into the output directory to place the file
    #       default: False
    #
    # OUTPUT:
    #   csv file in
    #     _app_cfg['process']['path']['msg'][_proc_level]/<usr_id>.<msg_id>.csv
    #   0 if exporation went well
    #     or
    #   -1 if error during exportation process

    # Get ids
    # usr_id = '{0:04d}'.format(_usr_cfg['float_id'])
    usr_id = _usr_cfg['user_id']
    msg_id = '{0:03d}'.format(_msg['profile_id'])
    # Set filename
    if _filename is None:
        filename = usr_id + '.' + \
                   msg_id + '.csv'
    else:
        filename = _filename
    # Set path
    if _proc_level == 'RAW':
        subdir = _app_cfg['process']['path']['raw']
    elif _proc_level == 'L0':
        subdir = _app_cfg['process']['path']['level'][0]
    elif _proc_level == 'L1':
        subdir = _app_cfg['process']['path']['level'][1]
    elif _proc_level == 'L2':
        subdir = _app_cfg['process']['path']['level'][2]
    else:
        print('ERROR: Unknow processing level.')
        return -1
    if _sub_dir_user:
        path = os.path.join(_app_cfg['process']['path']['msg'], subdir, usr_id)
    else:
        path = os.path.join(_app_cfg['process']['path']['msg'], subdir)
    # Create path if necessary
    if not os.path.exists(path):
        os.makedirs(path)

    # Open file
    with open(os.path.join(path, filename), 'w') as csvfile:
        f = csv.writer(csvfile, delimiter=',')
        # Write header (list of field names)
        fields = ['datetime', 'lat', 'lon']
        # Add in order fields from usr_cfg
        for val in _usr_cfg['sensors'].values():
            for key in val.keys():
                # Skip special fields
                if key in LIST_SENSOR_SPECIAL_FIELDS:
                    continue
                # Add field to header if in msg
                if key in _msg['obs'].keys():
                    fields.append(key)
        # Add missing fields (new products and/or fields changing name)
        for key in _msg['obs'].keys():
            if key not in fields:
                fields.append(key)
        f.writerow(fields)

        # Write observations
        for i in range(len(_msg['obs'][fields[3]])):
            data = [str(_msg['dt']), _msg['lat'], _msg['lon']]
            for key in fields[3:]:  # Skip 3 first special fields
                data.append(_msg['obs'][key][i])
            f.writerow(data)
    return 0


####################
#   CORE PROCESS   #
####################


def rt(_msg_name, _usr_cfg_name=None, _app_cfg_name='cfg/app_cfg.json'):
       #_dark_fl_name=None):
    # Process a profile from RAW to L2
    #   processed data is exported to data directory
    #
    # INPUT
    #   _msg_name <string> name of profile to process
    #   _usr_cfg_name <string> float configuration
    #       default: <float_name>_cfg.json
    #   _app_cfg_name <string> path to application configuration
    #   _dark_fl_name <string> float fluorescence dark list
    #       required to compute minimum dark of fluorescence
    #       default: <float_name>_dark_fl.csv
    #
    # OUTPUT
    #   0 if function ran well
    #     or
    #   -1 if error during exportation process

    # Get float and profile id
    foo = _msg_name.split('.')
    usr_id = 'n' + foo[0]  # Find way to switch to f for APEX
    msg_id = foo[1]
    if __debug__:
        print('Running rt(' + _msg_name + ')...', end=' ', flush=True)

    # Check arguments
    if _usr_cfg_name is None:
        _usr_cfg_name = usr_id + '_cfg.json'
    # if _dark_fl_name is None:
    #     _dark_fl_name = usr_id + '_dark_fl.csv'

    # Load data
    app_cfg = import_app_cfg(_app_cfg_name)
    usr_cfg = import_usr_cfg(os.path.join(app_cfg['process']['path']['usr_cfg'],
                                          _usr_cfg_name))
    msg_l0 = import_msg(os.path.join(app_cfg['process']['path']['msg'],
                                     app_cfg['process']['path']['raw'],
                                     usr_id, _msg_name))


    if app_cfg['process']['active']['rt']:
        # Process data
        msg_l1 = process_L1(msg_l0, usr_cfg)  # counts to SI units
        if msg_l1 == -1:
            print('ERROR: Unable to process to level 1')
            return -1
        msg_l2 = process_L2(msg_l1, usr_cfg)  # apply corrections
        if msg_l2 == -1:
            print('ERROR: Unable to process to level 2')
            return -1

        # Save data
        if export_csv(msg_l0, usr_cfg, app_cfg, 'L0') == -1:
            print('ERROR: Unable to export Level 0 to csv')
            return -1
        if export_csv(msg_l1, usr_cfg, app_cfg, 'L1') == -1:
            print('ERROR: Unable to export Level 1 to csv')
            return -1
        if export_csv(msg_l2, usr_cfg, app_cfg, 'L2') == -1:
            print('ERROR: Unable to export Level 2 to csv')
            return -1

        # Dashboard data
        msg_db = msg_l2
    else:
        msg_db = msg_l0

    if app_cfg['dashboard']['active']['rt']:
        # Update dashboard
        update_float_status(os.path.join(app_cfg['dashboard']['path']['dir'],
                                         app_cfg['dashboard']['path']['usr_status']),
                            usr_id, wmo=usr_cfg['wmo'],
                            dt_last=msg_db['dt'],profile_n=msg_db['profile_id'])
        export_msg_to_json_profile(msg_db,
                                   app_cfg['dashboard']['path']['dir'],
                                   usr_id, msg_id)
        export_msg_to_json_timeseries(msg_db,
                                      app_cfg['dashboard']['path']['dir'],
                                      usr_id)

    if __debug__:
        print('Done')


def bash(_usr_ids, _usr_cfg_names=[], _app_cfg_name='cfg/app_cfg.json'):
    #, _dark_fl_names=None):
    # Process all the profiles from a specific float
    #   processed data is exported to data directory
    #   Note: existing data will be replaced
    #
    # INPUT
    #   _usr_ids <list> names of floats to process
    #   _usr_cfg_name <list> float configurations
    #       default: <float_name>_cfg.json
    #   _app_cfg_name <string> path to application configuration
    #   _dark_fl_name <list> float fluorescence dark list
    #       required to compute minimum dark of fluorescence
    #       default: <float_name>_dark_fl.csv
    #
    # OUTPUT
    #   0 if function ran well
    #     or
    #   -1 if error during exportation process

    # Check input
    if _usr_cfg_names == []:
        for usr_id in _usr_ids:
            _usr_cfg_names.append(usr_id + '_cfg.json')

    # Load application configuration
    app_cfg = import_app_cfg(_app_cfg_name)
    # Run each user
    for (usr_id, usr_cfg_name) in zip(_usr_ids, _usr_cfg_names):
        if __debug__:
            print('Bash Processing of ' + usr_id + '...', end=' ', flush=True)
        # Load user configuration
        usr_cfg = import_usr_cfg(os.path.join(
                                 app_cfg['process']['path']['usr_cfg'],
                                  usr_cfg_name))

        # Reset Time series on first run
        dashboard_rebuild_timeseries = True;

        # List all messages
        msg_list = [name for name in os.listdir(os.path.join(
                    app_cfg['process']['path']['msg'],
                    app_cfg['process']['path']['raw'],
                    usr_id)) if name[-4:] == '.msg']

        for msg_name in msg_list:
            # Load message
            msg_l0 = import_msg(os.path.join(app_cfg['process']['path']['msg'],
                                             app_cfg['process']['path']['raw'],
                                             usr_id, msg_name))

            if app_cfg['process']['active']['bash']:
                # Process data
                msg_l1 = process_L1(msg_l0, usr_cfg)  # counts to SI units
                if msg_l1 == -1:
                    print('ERROR: Unable to process to level 1')
                    return -1
                msg_l2 = process_L2(msg_l1, usr_cfg)  # apply corrections
                if msg_l2 == -1:
                    print('ERROR: Unable to process to level 2')
                    return -1

                # Save data
                if export_csv(msg_l0, usr_cfg, app_cfg, 'L0') == -1:
                    print('ERROR: Unable to export Level 0 to csv')
                    return -1
                if export_csv(msg_l1, usr_cfg, app_cfg, 'L1') == -1:
                    print('ERROR: Unable to export Level 1 to csv')
                    return -1
                if export_csv(msg_l2, usr_cfg, app_cfg, 'L2') == -1:
                    print('ERROR: Unable to export Level 2 to csv')
                    return -1

            # Dashboard data
                msg_db = msg_l2
            else:
                msg_db = msg_l0

            # Update dashboard
            if app_cfg['dashboard']['active']['bash']:
                msg_id = msg_name.split('.')[1]
                export_msg_to_json_profile(msg_db,
                                   app_cfg['dashboard']['path']['dir'],
                                   usr_id, msg_id)
                if 0 == export_msg_to_json_timeseries(msg_db,
                                      app_cfg['dashboard']['path']['dir'],
                                      usr_id,
                                      _reset=dashboard_rebuild_timeseries):
                    # Disable time series reset as we just did it
                    dashboard_rebuild_timeseries = False

        # Update dashboard with information from last message
        if app_cfg['dashboard']['active']['bash']:
            update_float_status(os.path.join(app_cfg['dashboard']['path']['dir'],
                                             app_cfg['dashboard']['path']['usr_status']),
                                usr_id, wmo=usr_cfg['wmo'],
                                dt_last=msg_db['dt'], profile_n=msg_db['profile_id'])

        if __debug__:
            print('Done')

    return 0

if __name__ == '__main__':
    # for i in range(109):
    #     rt('0572.%03d.msg' % i)
    # rt('0572.010.msg')
    bash(['n0572', 'n0573', 'n0574'])

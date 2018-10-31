# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 14:44:34
# @Last Modified by:   nils
# @Last Modified time: 2017-09-10 18:49:31

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
from argo_server import ArgoServer


###########################
#  SENSOR SPECIFICATIONS  #
###########################
# Backscattering theta and lambda
MCOM_BETA_CENTROID_ANGLE = 150
MCOM_BETA_WAVELENGTH = 700
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


def import_navis_msg(filename):
    # Simple function to import a file from  Navis float
    #   Convert binary data from raw msg to ASCII (L0)
    valid_obs_len = [60, 72]
    valid_obs_len = [e + 1 for e in valid_obs_len]

    f = open(filename, 'r')
    d = {'dt':None, 'lat': None, 'lon': None, 'profile_id': None, 'float_id': None}
    obs = {"p": list(), "t": list(), "s": list(), "o2_ph": list(),
           "o2_t": list(), "fchl": list(), "beta": list(), "fdom": list(),
           "par": list(), "tilt": list(), "tilt_std": list()}
    park_obs = {"dt": list(), "p": list(), "t": list(),
                "s": list(), "o2_ph": list(), "o2_t": list()}
    obs_begin = False
    obs_end = False
    crv_on = False
    for l in f:
        # Get float_id and profile_id
        if l.find('$ FloatId') != -1:
            d['float_id'] = int(float(l[-6:-2]))
        elif l.find('FloatId') != -1:
            if 'float_id' not in d.keys():
                d['float_id'] = int(float(l[-5:-1]))
        elif l.find('ProfileId=') != -1:
            d['profile_id'] = int(float(l[-4:-1]))

        # Get date of end of profile
        elif l.find('terminated') != -1:
            foo = l.find('terminated')
            d['dt'] = datetime.strptime(l[foo + 12:-1], '%a %b %d %H:%M:%S %Y')

        # Get position
        elif l.find('Fix:') != -1:
            s = [e for e in l.split(' ') if e]  # keep only non empty str
            d['lat'] = float(s[2])
            d['lon'] = float(s[1])
            # save date if not done yet
            if d['dt'] is None:
                d['dt'] = datetime.strptime(s[3] + s[4], '%m/%d/%Y%H%M%S')

        # Check if Crover embedded
        elif (l.find('cRover') != -1 or
            l.find('Crover') != -1) and not crv_on:
            crv_on = True
            obs['c_count'] = list()
            obs['c_su'] = list()

        # Get park observations
        elif l.find('ParkObs:') != -1:
            s = [e for e in l.split(' ') if e]  # keep only non empty str
            park_obs['dt'].append(
                datetime.strptime(s[1] + s[2] + s[3] + s[4], '%b%d%Y%H:%M:%S'))
            park_obs['p'].append(float(s[5]))
            park_obs['t'].append(float(s[6]))
            park_obs['s'].append(float(s[7]))
            park_obs['o2_ph'].append(float(s[8]))
            park_obs['o2_t'].append(float(s[9][0:-1]))

        # Get profile observation
        elif l.find('ser1') != -1:
            obs_begin = True
        elif l.find('Resm') != -1:
            obs_end = True
        elif l.find('00000000000000FFFFFFFFFFFF00FFFFFFFFFFFFFFFFFF00FFFFFFFFFFFFFFFFFF00FFFF') != -1:
            # skip observation
            continue
        elif obs_begin and not obs_end and len(l) in valid_obs_len:
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

        # Get engineering data (valid on ly for vanilla/BGCi floats)
        # Air Pump
        elif l.find('AirPumpAmps') != -1:
            s = [e for e in l.split('=') if e]
            d['AirPumpAmps'] = int(s[1]) * 3.3 / 4096 / 0.698
        elif l.find('AirPumpVolts') != -1:
            s = [e for e in l.split('=') if e]
            d['AirPumpVolts'] = int(s[1]) * 19.767 / 4096
        # Buoyancy Pump
        elif l.find('BuoyancyPumpAmps') != -1:
            s = [e for e in l.split('=') if e]
            d['BuoyancyPumpAmps'] = int(s[1]) * 3.3 / 4096 / 0.698
        elif l.find('BuoyancyPumpVolts') != -1:
            s = [e for e in l.split('=') if e]
            d['BuoyancyPumpVolts'] = int(s[1]) * 19.767 / 4096
        # Quiescent
        elif l.find('QuiescentAmps') != -1:
            s = [e for e in l.split('=') if e]
            d['QuiescentAmps'] = int(s[1]) * 3.3 / 4096 / 0.698
        elif l.find('QuiescentVolts') != -1:
            s = [e for e in l.split('=') if e]
            d['QuiescentVolts'] = int(s[1]) * 19.767 / 4096
        # SBE41CP
        elif l.find('Sbe41cpAmps') != -1:
            s = [e for e in l.split('=') if e]
            d['Sbe41cpAmps'] = int(s[1]) * 3.3 / 4096 / 0.698
        elif l.find('Sbe41cpVolts') != -1:
            s = [e for e in l.split('=') if e]
            d['Sbe41cpVolts'] = int(s[1]) * 19.767 / 4096
        # MCOMS
        elif l.find('McomsAmps') != -1:
            s = [e for e in l.split('=') if e]
            d['McomsAmps'] = int(s[1]) * 3.3 / 4096 / 0.698
        elif l.find('McomsVolts') != -1:
            s = [e for e in l.split('=') if e]
            d['McomsVolts'] = int(s[1]) * 19.767 / 4096
        # SBE63
        elif l.find('Sbe63Amps') != -1:
            s = [e for e in l.split('=') if e]
            d['Sbe63Amps'] = int(s[1]) * 3.3 / 4096 / 0.698
        elif l.find('Sbe63Volts') != -1:
            s = [e for e in l.split('=') if e]
            d['Sbe63Volts'] = int(s[1]) * 19.767 / 4096

        # Check if file is complete
        elif l.find('<EOT>') != -1:
            d['EOT'] = True

    d['obs'] = obs
    d['park_obs'] = park_obs
    f.close()
    return d

def import_provor_msg(_filename):
    # Import a file from NKE Provor float
    #   Read data ASCII data from multiple files:
    #       + read specified cast
    #           recommended extension _09.txt
    #       + read position in _T253.txt
    #   Output to msg ASCII (L0)
    #
    # Documentation on file naming convention:
    #   filename: id[a,b,c,d]_cycle_profile_cast.txt
    #   last letter in id correspond to the deployment number
    #   ex: a: first deployment
    #       d: 4th deployment
    #   cast: 09 upcast, 05 downcast, 06 drift
    #   T253 position

    d = dict()

    # Read metadata: file T253.txt
    with open(_filename + '_T253.txt', 'r') as f:
        f.readline()     # Skip first line of header
        l = f.readline()
        s = l.split(' ')
        # Get float_id and profile_id
        d['float_id'] = int(s[1])   # Serial number of profiler
        d['profile_id'] = int(s[3]) * 100 + int(s[4]) # Cycle# * 100 + Profile#

        # Get date of profile
        d['dt'] = datetime.strptime(s[0][1:-1], '%Y-%m-%d_%H:%M:%S')

        # Get position
        d['lat']= float(s[62]) + float(s[63])/60 + 0.000001*float(s[64])
        if int(s[65]) == 1:
          d['lat'] = -1 * d['lat']
        d['lon']= float(s[66]) + float(s[67])/60 + 0.000001*float(s[68])
        if int(s[69]) == 1:
          d['lon'] = -1 * d['lon']

    # Read up cast: file 09.txt
    list_field_id=[0, 7, 8, 9, 10, 11, 16, 17, 18, 19, 15, 12, 13, 14, 20]
    obs = {"p": list(), "t": list(), "s": list(), "o2_c1": list(), "o2_c2": list(),
           "o2_t": list(), "fchl": list(), "beta": list(), "fdom": list(), "c": list(),
           "par": list(), "ed380": list(), "ed412": list(), "ed490": list(),
           "no3": list()}
    active_obs = {"p": False, "t": False, "s": False, "o2_c1": False, "o2_c2": False,
           "o2_t": False, "fchl": False, "beta": False, "fdom": False, "c": False,
           "par": False, "ed380": False, "ed412": False, "ed490": False,
           "no3": False}
    # active_line_obs = list()
    with open(_filename + '_09.txt', 'r') as f:
        f.readline()    # Skip first line of header
        for l in f:
            s = l.split(' ')
            # active_line = False
            for field_id, field_name  in zip(list_field_id, obs.keys()):
                if s[field_id] == 'NA':
                    obs[field_name].append(float('nan'))
                else:
                    obs[field_name].append(float(s[field_id]))
                    active_obs[field_name] = True
            #         if field_name != 'p':
            #             active_line = True
            # active_line_obs.append(active_line)

    # Rm empty fields
    for k, v in active_obs.items():
        if not v:
            obs.pop(k, None)
    # Rm empty lines
    # for i in range(len(active_line_obs)):
    #     if not active_line_obs[i]:
    #         print(i)
    #         for k in obs.keys():
    #             del obs[k][i]

    # TODO Read park obs: file 06.txt
    park_obs = {"dt": list(), "p": list(), "t": list(),
            "s": list(), "o2_ph": list(), "o2_t": list()}

    d['obs'] = obs
    d['park_obs'] = park_obs
    # print(d)
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
                if 'out' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:out')
                    return -1
                if 'level' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:level')
                    return -1
                if 'log' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:log')
                    return -1
                if 'err' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:err')
                    return -1
                if 'pid' not in d['process']['path'].keys():
                    print(_filename + ': missing variable process:path:pid')
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
#   CONVERT DATA   #
####################


def convert_msg2pjm(_filename_in, _filename_out):
    # Convert msg received from Seabird Navis BGCi float
    #   to plain-jane msg (pjm) for Argo data center.
    #   The function is stripping-out non core-argo data.
    #.  Keep only pressure, temperature, and salinity.

    with open(_filename_in, 'r') as fr:

        # Create directory if necessary
        path_out = os.path.dirname(_filename_out)
        if not os.path.exists(path_out):
            os.makedirs(path_out)

        with open(_filename_out, 'w') as fw:
            park_sample_flag = False
            profile_header_flag = False
            profile_flag = False
            engineering_flag = False
            for l in fr:
                # print(l, end='')
                # Empty
                if len(l) == 1:
                    continue

                # Special lines
                elif '$                        Date        p       t      s' in l:
                    # Skip line
                    continue
                elif '$       p       t      s' in l:
                    # Trigger park sample flag
                    park_sample_flag = True
                    # Reformat line
                    fw.write('$       p       t      s' + l[-1])

                # Park Observation
                elif 'ParkObs' in l and not engineering_flag:
                    # Reformat line
                    dt = l[8:29] # date
                    p = ' %7.2f' % float(l[31:38]) # pressure
                    t = ' %7.4f' % float(l[39:46]) # temperature
                    s = ' %7.4f' % float(l[47:53]) # salinity
                    unix_epoch = ' %11d' % (datetime.strptime(dt[1:], '%b %d %Y %H:%M:%S') - datetime(1970,1,1)).total_seconds()
                    m_time = ' %7d' % 0
                    fw.write('ParkPts: ' + dt + unix_epoch + m_time + p + t + s + l[-1])

                # Park Sample
                elif park_sample_flag:
                    if l[0:9] == '# GPS fix':
                        # End Park Sample
                        park_sample_flag = False
                        # No profile header
                        # No profile
                        # Start bottom
                        engineering_flag = True
                        fw.write(l)
                    elif l[0] == '#':
                        # End Park Sample
                        park_sample_flag = False
                        # Start profile header
                        profile_header_flag = True
                        fw.write(l)
                    elif '(Park Sample)' in l:
                        fw.write(l[0:24] + ' (Park Sample)' + l[-1])
                    else:
                        fw.write(l[0:24] + l[-1])

                # Profile Header
                elif profile_header_flag:
                    if l[0:4] == 'ser1' or 'tilt: yes' in l:
                        # End profile header
                        profile_header_flag = False
                        # Start profile
                        profile_flag = True
                        # Skip line
                    else:
                        raise ValueError('Unexpected profile header line:\n'+l)

                # Profile
                elif profile_flag:
                    if l[0:14] == '00000000000000':
                        # Skip line
                        continue
                    elif l[0:4] == 'Resm':
                        # End profile
                        profile_flag = False
                        # Start bottom
                        engineering_flag = True
                        # Skip line
                    else:
                        fw.write(l[0:14] + l[-1])

                # Case of msg 000 & start Biographical & Engineering (even if already started)
                elif '# GPS fix' in l:
                    # Start bottom
                    engineering_flag = True
                    fw.write(l)

                # Biographical & Engineering lines
                else:
                    fw.write(l)


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
            if sensor_val['model'] in ['SBE41CP', 'SBE41C']:
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
            elif sensor_val['model'] == 'Oxygen Optode 4330':
                print('WARNING: Oxygen Optode 4330 not supported yet.')
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
                    print('ERROR: Missing variable par, tilt, or tilt_std'
                          ' in <usr>_cfg.json file.')
                    return -1
                # Check that field is in profile from float
                if ('par' not in _msg['obs'].keys() or
                    'tilt' not in _msg['obs'].keys() or
                    'tilt_std' not in _msg['obs'].keys()):
                    print('ERROR: Missing variable par, tilt, or tilt_std'
                          ' in msg.')
                    return -1
                # Apply calibration (if necessary)
                l1['obs']['par'] = radiometer_calibration(
                    np.array(_msg['obs']['par'], dtype='float'),
                    sensor_val['par'])
                l1['obs']['tilt'] = np.array(_msg['obs']['tilt'],
                                             dtype='float')
                l1['obs']['tilt_std'] = np.array(_msg['obs']['tilt_std'],
                                                 dtype='float')
            elif sensor_val['model'] == 'OCR504':
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
                    l1['obs'][var_key] = radiometer_calibration(
                        np.array(_msg['obs'][var_key], dtype='float'),
                        var_val)
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
    #   chlorophyll a fluorescence (chla_adj) is corrected for
    #     non-photochemical quenching (NPQ) with Xing et al. 2012
    #     CDOM fluorescence (based on minimum value for dark, not yet implemented)
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
            # Apply slope factor correction for NAAMES area floats
            fchl_slopec = slope_correction(fchl_npqc)
            # Keep manufacturer value
            l2['obs']['fchl'] = val   # fluorescence chlorophyll a (same as level 1)
            # Save corrected chla
            l2['obs']['chla_adj'] = fchl_slopec  # chlorophyll a
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
                beta_theta = ECO2C_BETA_CENTROID_ANGLE
                beta_lambda = ECO2C_BETA_WAVELENGTH
            elif _usr_cfg['sensors']['ECO']['model'] == 'FLBBCD':
                beta_theta = ECO3C_BETA_CENTROID_ANGLE
                beta_lambda = ECO3C_BETA_WAVELENGTH
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
        l2['obs']['sigma'] = gsw.sigma0(sa,ct)
    if 'sigma' in l2['obs'].keys():
        if l2['obs']['sigma'] != []:
            # Estimate "standard" Mixed Layer Depth
            #   fixed density threshold of 0.03 mg L^-1
            l2['mld'], l2['mld_index'] = estimate_mld(l2['obs']['p'],
                                                      l2['obs']['sigma'],
                                                      0.03)
            # Estimate Daily Mixed Layer Depth
            #   fixed density threshold of 0.005 mg L^-1
            l2['mld_daily'], l2['mld_daily_index'] = estimate_mld(
                                                      l2['obs']['p'],
                                                      l2['obs']['sigma'],
                                                      0.005)

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
               _filename=None, _sub_dir_user=True):
    # Save profile (_msg) in a csv file
    #
    #
    # INPUT:
    #   _msg dictionnary containing float profile
    #   _usr_cfg <dictionnary> float configuration
    #   _app_cfg <dictionnary> application configuration
    #   _proc_level <string> RAW, L0, L1 or L2 corresponding to the output
    #           directory in which the data should be exported
    #   _filename <string> name of csv file exported
    #       default: <usr_id>.<msg_id>.csv
    #   _sub_dir_user <bool> create a sub directory <usr_id>
    #           into the output directory to place the file
    #       default: True
    #
    # OUTPUT:
    #   csv file in
    #     _app_cfg['process']['path']['out'][_proc_level]/<usr_id>.<msg_id>.csv
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
        subdir = 'RAW'
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
        path = os.path.join(_app_cfg['process']['path']['out'], subdir, usr_id)
    else:
        path = os.path.join(_app_cfg['process']['path']['out'], subdir)
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


def rt(_msg_name, _usr_cfg_name=None, _app_cfg_name='cfg/float_processor_conf.json'):
       #_dark_fl_name=None):
    # Function called by real-time daemon to process profiles
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

    if __debug__:
        print('Running rt(' + _msg_name + ')...', end=' ', flush=True)

    # Load application configuration
    app_cfg = import_app_cfg(_app_cfg_name)

    # Load float data
    if _msg_name[-3:] == 'msg':
        # Navis
        foo = _msg_name.split('.')
        usr_id = 'n' + foo[0]
        msg_id = foo[1]
        # Make plan-jane MSG (PJM) -> Navis only
        convert_msg2pjm(os.path.join(app_cfg['process']['path']['msg'], usr_id, _msg_name),
                        os.path.join(app_cfg['process']['path']['out'],
                                     app_cfg['process']['path']['pjm'], usr_id, _msg_name))
        # Load float msg
        msg_l0 = import_navis_msg(os.path.join(app_cfg['process']['path']['msg'],
                                    usr_id, _msg_name))
    elif _msg_name[-3:] == 'txt':
        # PROVOR
        foo = _msg_name.split('_')
        usr_id = foo[0]
        msg_id = foo[1] + foo[2]
        msg_l0 = import_provor_msg(os.path.join(app_cfg['process']['path']['msg_provor'],
                                  usr_id, _msg_name[0:-7]))

    # Load user configuration data
    if _usr_cfg_name is None:
        _usr_cfg_name = usr_id + '_cfg.json'
    usr_cfg = import_usr_cfg(os.path.join(app_cfg['process']['path']['usr_cfg'],
                                          _usr_cfg_name))

    if app_cfg['process']['active']['rt'] and len(msg_l0['obs']['p']) > 0:
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

    # Upload data on Argo server
    if app_cfg['argo_primary']['active']['rt']:
        ArgoServer(app_cfg['argo_primary'], app_cfg['process']['path'], usr_id, _msg_name)
    if app_cfg['argo_alternate']['active']['rt']:
        ArgoServer(app_cfg['argo_alternate'], app_cfg['process']['path'], usr_id, _msg_name)

    if app_cfg['dashboard']['active']['rt']:
        if msg_db['dt'] is None:
            print('WARNING: No dt available for msg, not updating dashboard.')
        else:
            # Update dashboard (json files)
            update_float_status(os.path.join(app_cfg['dashboard']['path']['dir'],
                                             app_cfg['dashboard']['path']['usr_status']),
                                usr_id, _wmo=usr_cfg['wmo'],
                                _dt_last=msg_db['dt'],
                                _profile_n=msg_db['profile_id'])
            if len(msg_db['obs']['p']) > 0:
                # If profile not empty
                export_msg_to_json_profile(msg_db,
                                           app_cfg['dashboard']['path']['dir'],
                                           usr_id)
                export_msg_to_json_timeseries(msg_db,
                                              app_cfg['dashboard']['path']['dir'],
                                              usr_id)
                export_msg_to_json_contour_plot(msg_db,
                                       app_cfg['dashboard']['path']['dir'],
                                       usr_id)
                export_msg_to_json_map(msg_db,
                                       app_cfg['dashboard']['path']['dir'],
                                       usr_id)
            # Update database of dashboard
            update_db(msg_db, usr_cfg, app_cfg)

    if __debug__:
        print('Done')


def update(_usr_ids, _usr_cfg_names=[], _app_cfg_name='cfg/float_processor_conf.json'):
    # Add all new profiles calling rt for most recent data
    # Often used to uptade db on local computer (much faster than bash as there is no need to reprocess all the profiles
    # Check input
    if not _usr_cfg_names:
        usr_cfg_names = list()
        for usr_id in _usr_ids:
            usr_cfg_names.append(usr_id + '_cfg.json')
    else:
        usr_cfg_names = _usr_cfg_names

    # Load application configuration
    app_cfg = import_app_cfg(_app_cfg_name)

    # Run each user
    for (usr_id, usr_cfg_name) in zip(_usr_ids, usr_cfg_names):
        if __debug__:
            print('Update ' + usr_id + '...', flush=True)

        # Load user configuration
        usr_cfg = import_usr_cfg(os.path.join(
            app_cfg['process']['path']['usr_cfg'],
            usr_cfg_name))

        # List all messages
        if 'Navis' in usr_cfg['model']:
            msg_list = [name for name in os.listdir(os.path.join(
                app_cfg['process']['path']['msg'],
                usr_id)) if name[-4:] == '.msg']
        elif 'PROVOR' in usr_cfg['model']:
            msg_list = [name[0:-7] for name in os.listdir(os.path.join(
                app_cfg['process']['path']['msg_provor'],
                usr_id)) if name[-7:] == '_09.txt']
        else:
            print('ERROR: Unknow float model')
            return -1

        # Sort list as os.listdir return elements in arbitraty order
        msg_list.sort()

        # Query meta from db
        db = sqlite3.connect(app_cfg['dashboard']['path']['db'])
        cur = db.execute('SELECT profile FROM meta WHERE wmo = ?', [usr_cfg['wmo']])
        entries = cur.fetchall()
        db.close()
        if not entries[0]:
            raise ValueError('Float is not in database, run bash instead of update.')
        else:
            current_msg = entries[0][0]

        # Get float profiles
        msg_to_process = list()
        for msg_name in msg_list:
            if msg_name[-3:] == 'msg':
                # Navis
                foo = msg_name.split('.')
                msg_id = int(foo[1])
            elif msg_name[-3:] == 'txt':
                # PROVOR
                foo = msg_name.split('_')
                msg_id = int(foo[1] + foo[2])
            else:
                raise ValueError('Invalid float message name.')

            if current_msg < msg_id:
                msg_to_process.append(msg_name)

        # Add new profiles
        for msg_name in msg_to_process:
            rt(msg_name, _usr_cfg_name=usr_cfg_name, _app_cfg_name=_app_cfg_name)

        if __debug__:
            print('Update ' + usr_id + '... Done', flush=True)


def bash(_usr_ids, _usr_cfg_names=[], _app_cfg_name='cfg/float_processor_conf.json'):
    #, _dark_fl_names=None):
    # Function call to reset database
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
        usr_cfg_names = list()
        for usr_id in _usr_ids:
            usr_cfg_names.append(usr_id + '_cfg.json')
    else:
        usr_cfg_names = _usr_cfg_names

    # Load application configuration
    app_cfg = import_app_cfg(_app_cfg_name)
    # Connect to Argo server
    if app_cfg['argo_primary']['active']['bash']:
        argo_server_primary = ArgoServer(app_cfg)
    if app_cfg['argo_alternate']['active']['bash']:
        argo_server_alternate = ArgoServer(app_cfg)
    # Run each user
    for (usr_id, usr_cfg_name) in zip(_usr_ids, usr_cfg_names):
        if __debug__:
            print('Bash Processing of ' + usr_id + '...', end=' ', flush=True)
        # Load user configuration
        usr_cfg = import_usr_cfg(os.path.join(
                                 app_cfg['process']['path']['usr_cfg'],
                                  usr_cfg_name))

        # Reset Time series and map on first run
        dashboard_rebuild_timeseries = True
        dashboard_rebuild_contour_plot = True
        dashboard_rebuild_map = True
        # Init first msg date
        first_msg_dt = 'undefined';

        # List all messages
        if 'Navis' in usr_cfg['model']:
            msg_list = [name for name in os.listdir(os.path.join(
                        app_cfg['process']['path']['msg'],
                        usr_id)) if name[-4:] == '.msg']
        elif 'PROVOR' in usr_cfg['model']:
            msg_list = [name[0:-7] for name in os.listdir(os.path.join(
                        app_cfg['process']['path']['msg_provor'],
                        usr_id)) if name[-7:] == '_09.txt']
        else:
            print('ERROR: Unknow float model')
            return -1
        # Sort list as os.listdir return elements in arbitraty order
        msg_list.sort()

        for msg_name in msg_list:
            # Make plan-jane MSG (PJM) -> Navis only
            if 'Navis' in usr_cfg['model']:
                convert_msg2pjm(os.path.join(app_cfg['process']['path']['msg'], usr_id, msg_name),
                                os.path.join(app_cfg['process']['path']['out'],
                                             app_cfg['process']['path']['pjm'], usr_id, msg_name))

            # Load message
            if 'Navis' in usr_cfg['model']:
                msg_l0 = import_navis_msg(os.path.join(app_cfg['process']['path']['msg'],
                                                 usr_id, msg_name))
            elif 'PROVOR' in usr_cfg['model']:
                msg_l0 = import_provor_msg(os.path.join(app_cfg['process']['path']['msg_provor'],
                                                 usr_id, msg_name))
                msg_l0['obs'] = consolidate(msg_l0['obs'])
            else:
                print('ERROR: Unknow float model')
                return -1

            if app_cfg['process']['active']['bash'] and len(msg_l0['obs']['p']) > 0:
                # Process data
                msg_l1 = process_L1(msg_l0, usr_cfg)  # counts to SI units
                if msg_l1 == -1:
                    print('ERROR: Unable to process to level 1')
                    print('\tSkipping profile ' + '{0:03d}'.format(msg_db['profile_id']))
                    continue
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

            # Upload data on Argo server
            if app_cfg['argo_primary']['active']['bash']:
                argo_server_primary.upload_profile(app_cfg['process']['path'], usr_id, msg_name)
            if app_cfg['argo_alternate']['active']['bash']:
                argo_server_alternate.upload_profile(app_cfg['process']['path'], usr_id, msg_name)

            # Update dashboard
            if app_cfg['dashboard']['active']['bash']:
                if len(msg_db['obs']['p']) > 0:
                    # if profile not empty
                    export_msg_to_json_profile(msg_db,
                                       app_cfg['dashboard']['path']['dir'],
                                       usr_id)
                    if 0 == export_msg_to_json_timeseries(msg_db,
                                          app_cfg['dashboard']['path']['dir'],
                                          usr_id,
                                          _reset=dashboard_rebuild_timeseries):
                        # Disable time series reset as we just did it
                        dashboard_rebuild_timeseries = False
                    if 0 == export_msg_to_json_contour_plot(msg_db,
                                           app_cfg['dashboard']['path']['dir'],
                                           usr_id,
                                           _reset=dashboard_rebuild_contour_plot):
                        # Disable map reset as we just did it
                        dashboard_rebuild_contour_plot = False
                    if 0 == export_msg_to_json_map(msg_db,
                                           app_cfg['dashboard']['path']['dir'],
                                           usr_id,
                                           _reset=dashboard_rebuild_map):
                        # Disable map reset as we just did it
                        dashboard_rebuild_map = False
                # Update database with meta data and engineering data
                update_db(msg_db, usr_cfg, app_cfg)
                # if msg_db['profile_id'] == 0:
                #     first_msg_dt = msg_db['dt']

        # Update dashboard file with information from last message
        # if msg_list and app_cfg['dashboard']['active']['bash']:
        #     update_float_status(os.path.join(app_cfg['dashboard']['path']['dir'],
        #                                      app_cfg['dashboard']['path']['usr_status']),
        #                         usr_id, _wmo=usr_cfg['wmo'],
        #                         _dt_first=first_msg_dt,
        #                         _dt_last=msg_db['dt'],
        #                         _profile_n=msg_db['profile_id'])

        if __debug__:
            print('Done')

    return 0

# if __name__ == '__main__':
    # for i in range(109):
    #     rt('0572.%03d.msg' % i)
    # rt('0572.001.msg')
    # rt('lovbio032b_010_00_09.txt')
    # bash(['n0572', 'n0573', 'n0574', 'n0646', 'n0647', 'n0648'])
    # bash(['n0846', 'n0847', 'n0848', 'n0849', 'n0850', 'n0851', 'n0852'])
    # bash(['lovbio014b', 'lovbio030b', 'lovbio032b', 'metbio003d', 'metbio010d'])
    # update(['n0846'])

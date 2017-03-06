#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 16:02:19
# @Last Modified by:   nils
# @Last Modified time: 2017-01-03 22:33:53

# MAIN_RT is a script that is intended to run on individual profiles
#   the following processing is done:
#     1. load float configuration
#     2. import profile
#     3. update dashboard
#     4. process profile
#      .1 convert counts to scientific units
#      .2 apply corrections (NPQ, O2_temp, O2_sal)
#      .3 estimate other products
#     5. export new data in Seabass format

from toolbox import *
from process import *
from dashboard import *
import os  # list files in dir


# 0. Parameters
dir_data = '/Users/nils/Documents/UMaine/Lab/data/NAAMES/floats/RAW_EOT/'
dir_config = '/Users/nils/Documents/UMaine/Lab/data/NAAMES/floats/param/'
dir_www = '/Users/nils/Documents/MATLAB/Float_DB/output/'
float_status = 'NAAMES_float_status.json'

filename_profile = dir_data + 'n0572/0572.023.msg'

# 1. Load float configuration
foo = filename_profile.split('/')
user_id = foo[-2]
foo = foo[-1].split('.')
float_id = foo[0]
profile_id = foo[1]
if __debug__:
    print(user_id, float_id, profile_id)

cfg = import_usr_cfg(dir_config + user_id + '_config.json')
if __debug__:
    print("Float configuration : ")
    print(cfg)

# 2. Import profile
profile = import_msg(filename_profile)
if __debug__:
    print("Profile : ")
    print(profile)

# 3. Update dashboard
# Get number of profile
profile_n = len([name for name in os.listdir(
    dir_data + user_id + '/') if name.find('.msg') != -1])
if __debug__:
  print('Number of profiles: '+str(profile_n))
# update dashboard
update_float_status(dir_www+float_status,user_id,wmo=cfg['wmo'],dt_last=profile['dt'],profile_n=profile_n)

# 4.1 Convert counts to scientific units


# 4.2 Apply corrections

# 4.3 Estimate other products

# 5. Export data

# 6. Update figures


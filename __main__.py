# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 14:44:34
# @Last Modified by:   nils
# @Last Modified time: 2017-08-10 10:15:03

import sys
from process import bash, rt, update


print('FloatProcess v0.2.2')
if len(sys.argv) < 4:
    print('Need >3 arguments:\n' +
          '\t<string> processing mode (bash or rt)\n' +
          '\t<string> path to application configuration\n' +
          '\t<string> float_id in bash mode | msg_file_name in rt mode\n')
else:
    if sys.argv[1] == 'rt':
        if len(sys.argv) != 4:
            print('Take only one msg_file_name')
        else:
            rt(sys.argv[3], _app_cfg_name=sys.argv[2])
    elif sys.argv[1] == 'bash':
        bash(sys.argv[3:], _app_cfg_name=sys.argv[2])
    elif sys.argv[1] == 'update':
        update(sys.argv[3:], _app_cfg_name=sys.argv[2])
    else:
        print('Unable to run, unknown mode')


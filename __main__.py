# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 14:44:34
# @Last Modified by:   nils
# @Last Modified time: 2017-03-05 20:34:02

import sys
from process import bash, rt


print('FloatProcess v0.1.0')
if len(sys.argv) != 4:
    print('Need 2 arguments:\n' +
          '\t<string> processing mode (bash or rt)\n' +
          '\t<string> path to application configuration\n' +
          '\t<string> usr_id or msg_file_name depending on mode\n')
else:
    if sys.argv[1] == 'rt':
        rt(sys.argv[3], _app_cfg_name=sys.argv[2])
    elif sys.argv[1] == 'bash':
        bash([sys.argv[3]], _app_cfg_name=sys.argv[2])
    else:
        print('Unable to run, unknown mode')

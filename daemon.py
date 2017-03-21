# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2017-03-05 20:40:00
# @Last Modified by:   nils
# @Last Modified time: 2017-03-20 22:19:03

import sys
import os
import pyinotify
from process import rt, import_app_cfg


# Load application configuration
print('FloatProcess v0.1.0')
print('Starting daemon for real-time processing')
if len(sys.argv) != 2:
    print('Need 1 arguments:\n' +
          '\t<string> path to application configuration\n')
    sys.exit(-1)
else:
    CFG = import_app_cfg(sys.argv[1])
    CFG['path2cfg'] = os.path.join(sys.path[0], sys.argv[1])
    print(CFG['path2cfg'])


# Set what to do with files
class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CLOSE_WRITE(self, event):
        # event fields: path, name, pathname, & dir
        # Check if event is a directory
        if event.dir:
            return

        # Process profile from float
        foo = event.name.split('.')
        # usr_id = foo[0]
        # msg_id = foo[1]
        # ext = foo[2]
        if len(foo) == 3 and foo[2] == 'msg':
            print('Processing ' + event.name + '...', end=' ', flush=True)
            rt(event.name, _app_cfg_name=CFG['path2cfg'])
            print('Done')


# Setup watcher and notifier
wm = pyinotify.WatchManager()
notifier = pyinotify.Notifier(wm, EventHandler())
# wm.add_watch(CFG['process']['path']['msg'],
#              pyinotify.ALL_EVENTS, rec=True)
wm.add_watch(CFG['process']['path']['msg'],
             pyinotify.IN_CLOSE_WRITE, rec=True)

# Start infinit watching and notifying loop

# notifier.loop()
notifier.loop(daemonize=True,
              pid_file=CFG['process']['path']['pid'],
              stdout=CFG['process']['path']['log'],
              stderr=CFG['process']['path']['err'])
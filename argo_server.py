# Module to send msg, pjm, and logs to Argo servers

from ftplib import FTP, all_errors
import os, sys

class ArgoServer:

    def __init__(self, _cfg=None, _usr_id=None, _msg_name=None, _host=None, _username=None, _password=None, _timeout=None):
        if _cfg is not None:
            self.cfg = _cfg
            self.ftp = FTP(_cfg['argo']['host'], _cfg['argo']['username'], _cfg['argo']['password'], _cfg['argo']['timeout'])
            self.connected = True
            if _usr_id is not None and _msg_name is not None:
                self.upload_profile(_usr_id, _msg_name)
        elif _host is not None and _username is not None and _password is not None:
            self.ftp = FTP(_host, _username, _password, timeout=_timeout)
            self.connected = True
        else:
            self.ftp = FTP()
            self.connected = False

    def __del__(self):
        self.close()

    def open(self, _host='', _username='anonymous', _password=''):
        self.ftp.login(_host, _username, _password)
        self.connected = True

    def close(self):
        if self.connected:
            self.ftp.quit()
            self.connected = False

    def upload(self, _filename, _path_in, _path_out):
        # try:
            self.ftp.cwd(_path_out)
            cwd = os.getcwd()
            os.chdir(_path_in)
            f = open(_filename, 'rb')
            self.ftp.storbinary('STOR ' + _filename, f)
            f.close()
            os.chdir(cwd)
        # except all_errors:
        #     print('ERROR: Unable to upload file to FTP.')

    def upload_profile(self, _usr_id, _msg_name):
        # Upload msg
        path2msg = os.path.join(self.cfg['process']['path']['msg'], _usr_id)
        if os.path.isfile(os.path.join(path2msg, _msg_name)):
            self.upload(_msg_name, path2msg,self.cfg['argo']['path']['msg'])
        # Upload log
        log_name = _msg_name[:-4] + '.log'
        if os.path.isfile(os.path.join(path2msg, log_name)):
            self.upload(log_name, path2msg, self.cfg['argo']['path']['msg'])
        # Upload pjm
        path2pjm = os.path.join(self.cfg['process']['path']['out'],
                                self.cfg['process']['path']['pjm'], _usr_id)
        if os.path.isfile(os.path.join(path2pjm, _msg_name)):
            self.upload(_msg_name, path2pjm, self.cfg['argo']['path']['pjm'])

if __name__ == "__main__":
    from process import import_app_cfg
    app_cfg = import_app_cfg('cfg/float_processor_conf.json')

    # Test 1
    argo = ArgoServer(_host=app_cfg['argo']['host'], _username=app_cfg['argo']['username'],
                      _password=app_cfg['argo']['password'], _timeout=app_cfg['argo']['timeout'])
    argo.upload('README.md', '../FloatProcessor/', app_cfg['argo']['path']['msg'])
    # argo.upload('README.html', '/Users/nils/Data/NAAMES/floats/', app_cfg['argo']['path']['msg'])
    argo.close()

    # Test 2
    ArgoServer(app_cfg, 'n0572', '0572.007.msg')


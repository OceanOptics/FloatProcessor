# Module to send msg, pjm, and logs to Argo servers

from ftplib import FTP, all_errors
import paramiko
import os, sys


class ArgoServer:
    def __init__(self, _cfg, *args, **kargs):
        if _cfg['protocol'] == 'ftp':
            self.io = ArgoServerFTP(_cfg, *args, **kargs)
        elif _cfg['protocol'] == 'sftp':
            self.io = ArgoServerSFTP(_cfg, *args, **kargs)
        else:
            raise ValueError('Protocol not supported: ' + _cfg['protocol'])

    def open(self, _host='', _username='anonymous', _password='', *args, **kargs):
        self.io.open(_host, _username, _password, *args, **kargs)

    def close(self):
        self.io.close()

    def upload(self, _filename, _path_in, _path_out):
        self.io.upload(_filename, _path_in, _path_out)

    def upload_profile(self, _path, _usr_id, _msg_name):
        self.io.upload_profile(_path, _usr_id, _msg_name)


def upload_profile(self, _path, _usr_id, _msg_name):
    # Upload msg
    path2msg = os.path.join(_path['msg'], _usr_id)
    if os.path.isfile(os.path.join(path2msg, _msg_name)):
        self.upload(_msg_name, path2msg, self.cfg['path']['msg'])
    # Upload log
    # log_name = _msg_name[:-4] + '.log'
    # if os.path.isfile(os.path.join(path2msg, log_name)):
    #     self.upload(log_name, path2msg, self.cfg['argo']['path']['log'])
    # Upload previous log ### NOT RECOMMENDED BUT FAST TRICK THAT WILL WORK ###
    log_name = '%s.%03d.log' % (_msg_name[:-8], int(_msg_name[-7:-4])-1)
    if os.path.isfile(os.path.join(path2msg, log_name)):
        self.upload(log_name, path2msg, self.cfg['path']['log'])
    # Upload pjm
    path2pjm = os.path.join(_path['out'],
                            _path['pjm'], _usr_id)
    if os.path.isfile(os.path.join(path2pjm, _msg_name)):
        self.upload(_msg_name, path2pjm, self.cfg['path']['pjm'])


class ArgoServerFTP:
    def __init__(self, _cfg=None, _path=None, _usr_id=None, _msg_name=None, _host=None, _username=None, _password=None, _timeout=None):
        if _cfg is not None:
            self.cfg = _cfg
            self.ftp = FTP(_cfg['host'], _cfg['username'], _cfg['password'], _cfg['timeout'])
            self.connected = True
            if _path is not None and _usr_id is not None and _msg_name is not None:
                self.upload_profile(_path, _usr_id, _msg_name)
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
        if not os.path.isfile(os.path.join(_path_in, _filename)):
            raise ValueError('File does not exist: ' + _path_in + _filename +
                             '\nCurrent working directory: ' + os.getcwd())
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

    def upload_profile(self, _path, _usr_id, _msg_name):
        upload_profile(self, _path, _usr_id, _msg_name)


class ArgoServerSFTP:

    def __init__(self, _cfg=None, _path=None, _usr_id=None, _msg_name=None, _host=None, _username=None, _password=None, _port=22):
        if _cfg is not None:
            self.cfg = _cfg
            self.transport = paramiko.Transport((_cfg['host'], _cfg['port']))
            self.transport.connect(username=_cfg['username'], password=_cfg['password'])
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.connected = True
            if _path is not None and _usr_id is not None and _msg_name is not None:
                self.upload_profile(_path, _usr_id, _msg_name)
        elif _host is not None and _username is not None and _password is not None:
            self.transport = paramiko.Transport((_host, _port))
            self.transport.connect(username=_username, password=_password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.connected = True
        else:
            self.transport = None
            self.sftp = None
            self.connected = False

    def open(self, _host='', _username='anonymous', _password='', _port=22):
        self.transport = paramiko.Transport((_host, _port))
        self.transport.connect(username=_username, password=_password)
        self.connected = True

    def close(self):
        if self.connected:
            self.sftp.close()
            self.transport.close()
            self.connected = False

    def upload(self, _filename, _path_in, _path_out):
        if not os.path.isfile(os.path.join(_path_in, _filename)):
            raise ValueError('File does not exist: ' + _path_in + _filename +
                             '\nCurrent working directory: ' + os.getcwd())
        # try:
        self.sftp.put(os.path.join(_path_in, _filename), os.path.join(_path_out, _filename))
        # except all_errors:
        #     print('ERROR: Unable to upload file to FTP.')

    def upload_profile(self, _path, _usr_id, _msg_name):
        upload_profile(self, _path, _usr_id, _msg_name)


if __name__ == "__main__":
    from process import import_app_cfg
    app_cfg = import_app_cfg('cfg/float_processor_conf.json')

    ## FTP Connection
    # Test 1
    argo = ArgoServerFTP(_host=app_cfg['argo_primary']['host'], _username=app_cfg['argo_primary']['username'],
                      _password=app_cfg['argo_primary']['password'], _timeout=app_cfg['argo_primary']['timeout'])
    argo.upload('README.md', '.../FloatProcessor/', app_cfg['argo_primary']['path']['msg'])
    argo.close()
    print('TEST 1: PASSED')

    # Test 2
    ArgoServerFTP(app_cfg['argo_primary'], app_cfg['process']['path'], 'n0572', '0572.007.msg')
    print('TEST 2: PASSED')

    ## SFTP Connection
    # Test 3
    argo = ArgoServerSFTP(_host=app_cfg['argo_alternate']['host'], _username=app_cfg['argo_alternate']['username'],
                         _password=app_cfg['argo_alternate']['password'], _port=22)
    argo.upload('README.md', '../FloatProcessor/', app_cfg['argo_alternate']['path']['msg'])
    argo.close()
    print('TEST 3: PASSED')

    # Test 4
    ArgoServerSFTP(app_cfg['argo_alternate'], app_cfg['process']['path'], 'n0572', '0572.069.msg')
    print('TEST 4: PASSED')

    ## "Interface" Class
    # Test 5
    argo = ArgoServer(app_cfg['argo_alternate'], app_cfg['process']['path'], 'n0572', '0572.101.msg')
    argo.upload('README.md', '../FloatProcessor/', app_cfg['argo_primary']['path']['msg'])
    argo.upload_profile(app_cfg['process']['path'], 'n0572', '0572.091.msg')
    argo.close()
    print('TEST 5: PASSED')




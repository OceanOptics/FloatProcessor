FloatProc
=========
_Python library to process biogeochemical float profiles_

Profiles received from biogeochemical (BGC) float can be processed in real-time and delayed mode with this library. Calibrations are applied to convert engineering counts to scientific units followed by the state-of-the-art corrections ([more details](ftp://misclab.umeoce.maine.edu/floats/README.html)). A web interface can be enabled to visualize the time series and profiles from the floats.

## Installation
Packages required and tested version:

  - numpy v1.11.2 and 1.12.0
  - scipy v0.18.2 and 0.18.1
  - statsmodels v0.6.1 and 0.8.0
  - simplejson 3.10.0 (original json package does not support NaN)
  - gsw v3.0.3 (compute seawater density)
  - pyinotify v0.9.6 [optional, to run real-time monitoring files]

Setup the configuration file for the processing app:
  *coming soon*

Setup the web interface (optional):
  *coming soon*

Setup real-time run:
  *coming soon*

## First run
Batch processing

    $python3 -O __main__.py bash cfg/app_cfg.json <usr_id>

    FloatProcess v0.1.0

Real-time processing with daemon:

    $python3 -O daemon.py cfg/app_cfg.json

    FloatProcess v0.1.0
    Starting daemon for real-time processing
    <path...>/FloatProcess/cfg/app_cfg.json

Processing one profile (real-time started from other application/script)

    $python3 -O __main__.py rt cfg/app_cfg.json <msg_file_name>


## Description of library
Description of files from the packages:

 - `toolbox.py`: oceanographic toolbox containing the calibration and corrections methods
 - `process.py`: set of functions to load the configuration of the application and each individual float in order to process the profiles at different level
 - `dashboard.py`: set of functions to update the content of the web interface
 - `daemon.py`: start daemon for real-time processing monitoring a directory
 - `test*.py`: various files used for testing and development

## TODO
  - BUG FIX in O2 correction
  - ADD support other float models: PROVOR, APEX
  - ADD corrections (CDOM, Dark) to fluorescence chlorophyll *a* profiles
  - ADD drift correction to attenuation profiles
  - IMPROVE NPQ correction with Xing17 using PAR signal
  - ADD export in NetCDF format
  - IMPROVE remove warnings from gsw when NaN values
  - REFACTORING process.py in one class
  - ADD Order float by active|more recent deployment in dashboard status
  - ADD dark correction for PAR sensor
  - CHECK N0574 inactive for 14 days
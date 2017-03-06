#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 16:02:19
# @Last Modified by:   nils
# @Last Modified time: 2017-01-02 13:00:39

# Test import_data set of function
from process import *
from profiler import *
from dashboard import *

# Parameters
dir_data = '/Users/nils/Documents/UMaine/Lab/data/NAAMES/floats/RAW_EOT/'
dir_cfg = '/Users/nils/Documents/UMaine/Lab/data/NAAMES/floats/param/'
dir_www = '/Users/nils/Documents/MATLAB/Float_DB/output/'
fn_profile = dir_data + 'n0572/0572.010.msg'
fn_float_cfg = dir_cfg + 'n0572_config.json'
fn_float_status = dir_www + 'NAAMES_float_status.json'

disp = False

# Quick test
if False:
  fchl = [1, 1.5, 2, 2.5, 3.1, 3.4, 3.2, 1.5, 1, 0.5, 0, 0, 0]
  bbp = [3, 2.5, 4, 2.9, 3.1, 3.4, 3.2, 1.5, 1, 0.5, 0, 0, 0]
  par = [1200, 800, 450, 200, 90, 80, 30, 20, 10, 9, 8, 7, 6]
  p = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

  print(estimate_zeu(np.array(p, dtype="float"), np.array(par, dtype="float")))

# Test import_data
if True:
  float_profile = import_msg(fn_profile)
  if disp:
      print(float_profile)

  float_cfg = import_cfg(fn_float_cfg)
  if disp:
      print(float_cfg)


# Test proc_data
if True:
  # General calibration  equation
  su = count2su(np.array([250, 255, 254, 120, 60, 55, 50]),
                {'scale_factor': 0.012, 'dark_count': 49},
                'scale_factor * (_count - dark_count)')
  if disp:
      print(su)
  su = count2su(np.array([736386, 616459, 538043, 464632,
                          425340, 394359, 332172]),
                {"a": [332264.4, 3.04020426149e-4],
                 "im": 1.3589},
                'a[1] * (_count - a[0]) * im')
  if disp:
      print(su)

  # Specific calibration equation
  beta_L0 = eco_calibration(
      np.array(float_profile['obs']['beta']),
      float_cfg['sensors']['ECO']['beta'])
  fchl_L0 = eco_calibration(
      np.array(float_profile['obs']['fchl']),
      float_cfg['sensors']['ECO']['fchl'])
  par_L0 = radiometer_calibration(
      np.array(float_profile['obs']['par'], dtype='float'),
      float_cfg['sensors']['Radiometer']['par'])
  o2_t = o2_t_calibration(
      np.array(float_profile['obs']['o2_t']),
      float_cfg['sensors']['O2']['o2_t'])
  o2_c_L0 = o2_phase_calibration(
      np.array(float_profile['obs']['o2_ph']), o2_t,
      float_cfg['sensors']['O2']['o2_ph'])
  # O2 Corrections
  o2_p_corr = o2_pressure_correction(o2_t, np.array(float_profile['obs']['p']))
  o2_s_corr = o2_salinity_correction(o2_t,
                                     np.array(float_profile['obs']['s']))
  o2_c_L1 = [o2_c_L0[i] * o2_p_corr[i] * o2_s_corr[i]
             for i in range(0, len(o2_c_L0))]
  # FCHL corrections
  start_npq = is_npq(np.array(float_profile['obs']['p']), par_L0)
  if disp:
    print(start_npq)
  if start_npq:
    fchl_L1 = npq_correction(np.array(float_profile['obs']['p']),
                             fchl_L0, start_npq,
                             _method='Sackmann',
                             _bbp=np.array(float_profile['obs']['beta']))
    fchl_L1 = npq_correction(np.array(float_profile['obs']['p']),
                             fchl_L0, start_npq,
                             _method='Xing')
    fchl_L1 = npq_correction(np.array(float_profile['obs']['p']),
                             fchl_L0, start_npq,
                             _method='Xing2')

  # Compute extra product
  bbp = estimate_bbp(beta_L0,
                     np.array(float_profile['obs']['t']),
                     np.array(float_profile['obs']['s']),
                     _lambda=700, _theta=150)
  if disp:
    print(bbp)
  # Estimate Cphyto
  Cphyto = estimate_cphyto(bbp, _lambda=700)
  if disp:
    print(Cphyto)
  # Estimate POC
  POC = estimate_poc(bbp, _lambda=700)
  if disp:
    print(POC)

  Zeu = estimate_zeu(np.array(float_profile['obs']['p'], dtype="float"),
                     par_L0)
  print(par_L0)
  if True:
    print(Zeu)

  # Test helpers
  x = np.linspace(0, 10, 100)
  # Add random error on y
  e = np.random.normal(size=len(x))
  y = x + e
  results = regress2(x, y, _method_type_2="reduced major axis",
                     _need_intercept=False)
  if disp:
    print(results)

  # Test estimate_betasw
  _t = np.array([10, 20])
  _s = np.array([35, 36])
  _lambda = np.array([700, 440])
  res = estimate_betasw(_t, _s, _lambda=_lambda)
  if disp:
    print(res)
  if abs(res[0] - 5.9937e-5) > 1e-9:
    print("Warning:betasw")


# Test dashboard
if False:
  update_float_status(
      fn_float_status, 'n0572', wmo='5902462', dt_last=datetime.today())

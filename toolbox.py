# -*- coding: utf-8 -*-
# @Author: nils
# @Date:   2016-03-10 13:15:32
# @Last Modified by:   nils
# @Last Modified time: 2017-03-05 18:48:49
#
# This module is developped to calibrate, adjust and compute additional product
# measurements from biogeochemical floats
#
# Instruments supported are:
#   WetLabs ECO: FLBBCD, MCOM
#   Radiometry: PAR Satlantic, OCR-504
#   Oxygen: Optode 4330, SBE63
#

from scipy.interpolate import interp1d
import numpy as np
import statsmodels.api as sm
import gsw

#############################
#   CALIBRATION EQUATIONS   #
#############################


def count2su(_count, _coef, _eq):
  # COUNT2SU: Convert counts (counts) in scientific units (output) using
  # the equation (eq) with the calibration coefficients (coef)
  #
  # INPUT:
  #   _eq: string of calibration equation use count as input variable
  #   _coef: dictionnary containing the calibration coefficients
  #     names of the coefficients must match the one in the equation (eq)
  #   _count: np.array counts to convert in scientific units
  #
  # OUTPUT:
  #   return values in scientific units
  #
  # EXAMPLES:
  #   su = count2su([250, 255, 254, 120, 60, 55, 50],
  #                 {'scale_factor': 0.012, 'dark_count': 49},
  #                 'scale_factor * (_count - dark_count)')
  #   print(su)
  #   su = count2su([736386, 616459, 538043, 464632, 425340, 394359, 332172],
  #                 {"a": [332264.4,3.04020426149e-4],
  #                  "im": 1.3589},
  #                 'a[1] * (_count - a[0]) * im')
  #   print(su)

  # Load coefficients in local environment
  vars().update(_coef)
  # Run the equation
  return eval(_eq)


def eco_calibration(_counts, _coef):
  # calibration of Environmental Characterization Optics (ECO) series of sensor
  # compatible sensors are: MCOM, ECO-Triplet, ECO-Puck, FLBBCD, FLBB
  # Equation used: scientific_unit = scale_factor * (count - dark_count)
  #
  # INPUT:
  #   _counts np.array of integer of the raw data from the instrument
  #   _coef dictionnary of the calibration coefficients, must have keys:
  #     scale_factor
  #     dark_count
  #
  # OUTPUT:
  #   return data calibrated in scientific units

  return _coef['scale_factor'] * (_counts - _coef['dark_count'])


def radiometer_calibration(_counts, _coef):
  # calibration equation of radiometers
  # compatible sensors are: Satlantic PAR, OCR504 :
  # Equation used: scientific_unit = a[1] * (count - a[0]) * im
  #
  # INPUT:
  #   _counts np.array of the raw data from the instrument
  #   _coef dictionnary of the calibration coefficients, must have keys:
  #     a
  #     im
  #
  # OUTPUT:
  #   data calibrated in scientific units

 return _coef['a'][1] * (_counts - _coef['a'][0]) * _coef['im']


def o2_phase_calibration(_o2_phase_volt, _o2_t, _coef_phase):
  # convert SBE63 phase voltage to oxygen concentration
  #
  # INPUT:
  #   _o2_phase_volt np.array
  #   _o2_t np.array window temperature (degrees C)
  #   _coef_phase should be composed of
  #     a[0] is zero oxygen offset
  #     a[1] is zero oxygen slope
  #     b[0],b[1],c[0],c[1],c[2] come from the coefficient fitting process
  #     a[2] is a Ksv phase**2 term
  #
  # OUTPUT:
  #   return oxygen concentration
  #
  # translated from matlab: Dan Quittman, Sea-Bird Electronics, Copyright 2011

  ph = _o2_phase_volt / 39.4570707  # phase
  t = _o2_t  # temperature in degree C
  a = _coef_phase['a']
  b = _coef_phase['b']
  c = _coef_phase['c']
  return (((a[0] + a[1] * t + a[2] * ph ** 2) / (b[0] + b[1] * ph) - 1.0) /
          (c[0] + (c[1] * t) + c[2] * (t ** 2)))


def o2_t_calibration(_o2_t_volt, _coef_t):
  # Convert SBE63 thermistor voltage into film temperature
  # output is in degrees centigrade (degKelvin - 273.15)
  # _coef_t shoud contain a[0:3]
  #
  # translated from matlab code of Dan Quittman, Sea-Bird Electronics,
  # Copyright 2011
  a = _coef_t['a']
  Rt = 100e3 * _o2_t_volt / (3.300000 - _o2_t_volt)
  L = np.log(Rt)
  return 1 / (a[0] + a[1] * L + a[2] * L ** 2 + a[3] * L ** 3 - 273.15)

###################
#   CORRECTIONS   #
###################

def is_npq(_p, _par, _threshold=80):
  # Determine if profile is affected by non photochemical quenching (NPQ)
  #   based on PAR signal
  # OUTPUT:
  #   0 == means that the profile is NOT quenched
  #   0 != means that the profile is quenched and up to which depth
  p_npq = []
  for p, par in zip(_p, _par):
    if par > _threshold:
      p_npq.append(p)
  if p_npq:
    return max(p_npq)
  else:
    return 0


def npq_correction(_p, _fchl, _depth_start_correct, _method="Xing2",
                   _n_avg=[1, 2], _bbp=np.array([]), _threshold_fchl=0.003):
  # Correction of chlorophyll a fluorescence profile affected by
  #   non photochemical quenching (NPQ)
  #
  # INPUT:
  #    Required:
  #        _p np.array depth or pressure (m or dBar)
  #           assume that it is positive bellow the surface
  #        _fchl np.array chlorophyll a fluorescence profile (mg.m^-3)
  #        _depth_start_correct float containing depth/pressure at which
  #           correction of npq should start
  #           usually: Zeu/3 or daily MLD
  #    Optional:
  #        _bbp np.array particulate backscattering profile (m^-1)
  #        _n_avg list of 2 elements specifying number of points to average
  #           before and after depth_start_correct, for method Xing2 only
  #        _threshold_fchl float of the threshold for minimum values of fchl
  #           to use in order to build relationship in method Sackmann
  #        _method string specifying the method to correct for NPQ
  #           Xing
  #             Xing et al., (2012) assumes that, in the mixed layer
  #             chlorophyll concentration is homogeneous and it proposes to
  #             extrapolate up to surface the highest value of chlorophyll
  #             concentration encountered in the mixed layer paper.
  #           Xing2 (default)
  #             Same as method Xing but take a n point median at
  #              _depth_start_correct instead of a single value.
  #           Sackmann (require _bbp)
  #             Sackmann et al., (2008) corrects the NPQ effect by
  #             extrapolating up to the surface the fluorescence value learned
  #             below the mixed layer from the relation between fl and bbp
  #
  # OUTPUT:
  #     return profile of chlorophyll a fluorescence corrected for NPQ
  #
  # AUTHOR: Nils Haentjens, Ms, University of Maine
  # EMAIL: nils.haentjens@maine.edu
  # OTHER CODE: a matlab version of this function also exist
  #
  # REFERENCES:
  #    Xing, X., Claustre, H., & Blain, S. (2012). for in vivo chlorophyll
  #      fluorescence acquired by autonomous platforms: A case study with
  #      instrumented elephant seals in the Kerguelen region (Southern Ocean.
  #      Limnol. Oceanogr. ?, 483?495. http://doi.org/10.4319/lom.2012.10.483
  #    Sackmann, B. S., Perry, M. J., & Eriksen, C. C. (2008). Seaglider
  #      observations of variability in daytime fluorescence quenching of
  #      chlorophyll-a in Northeastern Pacific coastal waters. Biogeosciences
  #      Discussions, 5(4), 2839?2865. http://doi.org/10.5194/bgd-5-2839-2008

  # sort profile with depth
  # Without numpy
  # sorted_index = sorted(range(len(_p)), key=lambda x: _p[x])
  # p = [_p[i]for i in sorted_index]
  # fchl = [_fchl[i]for i in sorted_index]
  # if _bbp:
  #   bbp = [_bbp[i]for i in sorted_index]
  # With numpy
  sorted_index = np.argsort(_p)
  p = _p[sorted_index]
  fchl = _fchl[sorted_index]
  if np.size(_bbp) != 0:
    bbp = _bbp[sorted_index]

  # find index at which correction start
  min_delta = np.min(np.abs(p - _depth_start_correct))
  index_start_correct = indices(
      p, lambda x: abs(x - _depth_start_correct) == min_delta)[0]

  # apply approriate quenching correction
  if _method == "Xing":
    # Extend to the surface the concentration of chlorophyll a at
    # depth_start_correct
    f = interp1d(p, fchl)
    fchl[0:index_start_correct + 1] = f(_depth_start_correct)
  elif _method == "Xing2":
    # Extend to the surface the median of the concentration of chlorophyll a
    #   of the n points arround depth_start_correct
    if _n_avg[0] + _n_avg[1] + 1 < len(fchl):
      if index_start_correct - _n_avg[0] < 0:
        foo = np.nanmedian(fchl[0:index_start_correct + _n_avg[1] + 1])
      elif index_start_correct + _n_avg[1] > len(fchl):
        foo = np.nanmedian(fchl[index_start_correct - _n_avg[0]:len(fchl)])
      else:
        foo = np.nanmedian(fchl[index_start_correct - _n_avg[0]:
                                index_start_correct + _n_avg[1] + 1])
      fchl[0:index_start_correct + 1] = foo
    else:
      print("Xing2: number of point to average is bigger than" +
            "the number of points in profile")
  elif _method == "Sackmann":
    # SACKMANN Correct for NPQ applying "Sackmann et al. 2008" method
    # Learn from fchl > 0.05 && p <= index_start_correct
    sel = indices(fchl, lambda x: x > _threshold_fchl)
    sel = indices(sel, lambda x: x >= index_start_correct + 1)
    bbp_sel = [bbp[i] for i in sel]
    fchl_sel = [fchl[i] for i in sel]
    # Robust linear regression type I forced by 0
    # b = np.nanmedian([fchl[i] / bbp[i] for i in sel])
    # Robust reduced major axis regression
    res = regress2(np.array(bbp_sel), np.array(fchl_sel))
    # Apply relation just learned
    fchl[0:index_start_correct + 1] = (
        res['intercept'] + bbp[0:index_start_correct + 1] * res['slope'])
  else:
    print('Unknown method')

  # Go back to original indexing order
  # Without numpy
  # original_index = sorted(
  #     range(len(sorted_index)), key=lambda x: sorted_index[x])
  # return [fchl[i]for i in original_index]
  # With numpy
  original_index = np.argsort(sorted_index)
  return fchl[original_index]


def o2_salinity_correction(_o2_t, _sp):
  # correct SBE63 oxygen values for salinity
  # translated from matlab: Dan Quittman, Sea-Bird Electronics, Copyright 2011
  # Garcia & Gordon coefs 1992, Benson and Krause 1984 data
  b = [-0.00624523, -0.00737614, -0.010341, -0.00817083]
  c = [-4.886820e-7]
  ts = np.log((298.15 - _o2_t) / (273.15 + _o2_t))
  return np.exp((b[0] + b[1] * ts + b[2] * ts ** 2 + b[3] * ts ** 3) * _sp +
                c[0] * _sp ** 2)


def o2_pressure_correction(_o2_t, _p, _e=0.011):
  # correct SBE63 oxygen values for pressure
  # translated from matlab: Dan Quittman, Sea-Bird Electronics,
  # Copyright 2011
  p = _p * (_p > 0)  # Clamp negative values to zero
  return np.exp(_e * p / _o2_t + 273.15)

####################
#   CHANGE UNITS   #
####################


def o2_mll2umolkg(_o2_concentration):
  # Coefficients for transforming ml(NTP)/l to umol/kg
  mgml = 1.42903
  umolmg = 1.0 / (31.9988 / 1e3)
  umolml = mgml * umolmg
  sig = 1028.0
  # TODO Get density with GSW
  print('WARMING: need to get density of water')
  return _o2_concentration * umolml / sig


#########################
#   ESTIMATE PRODUCTS   #
#########################

def estimate_mld(_sigma, _criterion=0.03):
  # estimate mixed layer depth (MLD) with a fixed density threshold
  #
  # INPUT:
  #   _sigma <np.array> density anomaly (rho - 1000) (kg m^-3)
  #     assume that surface is first value of array
  #   _criterion <float> fixed threshold (kg m^-3)
  #     default: 0.03 kg m^-3
  #
  # OUTPUT:
  #   _index_mld <> index of MLD in _sigma
  #

  if _sigma != []:
    return np.argmin(abs(abs(_sigma - _sigma[0]) - _criterion))
  else:
    return -1


def estimate_zeu(_p, _par):
  # Estimate Euphotic depth (Zeu) based on PAR profile

  # Order profile according to pressure _p
  sorted_index = np.argsort(_p)
  p = _p[sorted_index]
  par = _par[sorted_index]

  # Estimate surface light
  par_surf = par[0]

  # Estimate 1 % light level from surface
  par_zeu = 0.01 * par_surf

  if np.nanmin(par) < par_zeu:
    # Get euphotic depth by linear interpolation
    f = interp1d(par, p)
    Zeu = f(par_zeu)
  else:
    Zeu = float('nan')
  return Zeu


def estimate_dark(profiles, method, pressures):
  # ESTIMATE_DARK estimate the dark for a given series of profile usually of
  # chlorophyll a fluorescence
  #
  # Methods:
  #   deep
  #   minimal
  print('Not yet implemented')


def estimate_bbp(_beta, _t, _s, _lambda=700., _theta=140., _delta=0.039,
                 _Xp=[]):
  # The backscattering coefficient of particles (bbp) is estimated
  # from measurement of scattering at a single angle
  # in the backward hemisphere (beta)
  #
  #   $$\beta_p(\theta) &= \beta(\theta) - \beta_{sw}(\theta)\\$$
  #   $$b_{bp} &= 2 \times \pi \times \chi(\theta) \times \beta_{p}(\theta)$$
  #
  # INPUT:
  #   _beta: float or np.array, total angular scatterance (m^-1 sr^-1)
  #   _t: float or np.array, temperature (degree celsius)
  #   _s: float or np.array, salinity (unitless)
  #   _lambda: float or np.array, wavelength (nm)
  #      default: 700 nm
  #   _theta: float or np.array, scattering angle (deg)
  #      WET Labs ECO-FLBB 140 deg (default)
  #      WET Labs MCOM 150 deg
  #   _delta: float, depolarization ratio
  #      default: 0.039 from Farinato and Roswell (1976)
  #   _Xp: float, chi(theta) conversion coefficient at theta
  #      default: interpolated from Sullivan et al. (2013)
  #      theta should be included in [90, 171] deg
  #
  # /!\ Make sure that all np.array are same size
  #
  # OUTPUT:
  #   bbp: particulate backscattering (m^-1)
  #
  # REQUIRE:
  #   estimate_betasw
  #
  # REFERENCE:
  #   J. M. Sullivan, M. S. Twardowski, J. R. V Zaneveld, C. C. Moore,
  #   Measuring optical backscattering in water
  #   (2013; http://www.springerlink.com/index/10.1007/3-540-37672-0).

  if np.size(_Xp) == 0:
    # Interpolate X_p with values from Sullivan et al. 2013
    theta_ref = np.arange(90, 180, 10)
    Xp_ref = np.array(
        [0.684, 0.858, 1.000, 1.097, 1.153, 1.167, 1.156, 1.131, 1.093])
    # sigma_ref = np.array(
    #   [0.034, 0.032, 0.026, 0.032, 0.044, 0.049, 0.054, 0.054, 0.057])
    f = interp1d(theta_ref, Xp_ref, kind='cubic')
    Xp = f(_theta)
  else:
    Xp = _Xp

  beta_sw = estimate_betasw(_t, _s, _lambda, _theta, _delta)
  beta_p = _beta - beta_sw
  bbp = 2 * np.pi * Xp * beta_p
  return bbp


def estimate_betasw(_t, _s, _lambda=700, _theta=140, _delta=0.039):
  # Estimate angular scatterance of sea water (beta_sw)
  #
  # INPUT:
  #   _t: float or np.array, temperature (degree celsius)
  #   _s: float or np.array, salinity (unitless)
  #   _lambda: float or np.array, wavelength (nm)
  #      default: 700 nm
  #   _theta: float or np.array, scattering angle (deg)
  #      WET Labs ECO-FLBB 140 deg (default)
  #      WET Labs MCOM 150 deg
  #   _delta: float, depolarization ratio
  #      default: 0.039 from Farinato and Roswell (1976)
  #
  # /!\ Make sure that all np.array are same size
  #
  # OUTPUT:
  #   betasw: volume scattering at angles defined by theta
  #
  # REFERENCE:
  #   Xiaodong Zhang, Lianbo Hu, and Ming-Xia He (2009), Scattering by pure
  #   seawater: Effect of salinity, Optics Express, Vol. 17, No. 7, 5698-5710
  #
  # ORIGINAL MATLAB CODE:
  #   http://www.und.edu/instruct/zhang/programs/betasw_ZHH2009.m

  # Set constant
  Na = 6.0221417930e23  # Avogadro's constant
  Kbz = 1.3806503e-23  # Boltzmann constant
  M0 = 18e-3  # Molecular weigth of water in kg/mol

  # Convert input units
  Tk = _t + 273.15  # Absolute tempearture
  rad = _theta * np.pi / 180  # Angle in radian

  # refractive index of air is from Ciddor (1996,Applied Optics)
  n_air = 1.0 + (5792105.0 / (238.0185 - 1 / (_lambda / 1e3)**2) +
                 167917.0 / (57.362 - 1 / (_lambda / 1e3)**2)) / 1e8
  # refractive index of seawater is from Quan and Fry (1994, Applied Optics)
  n0 = 1.31405
  n1 = 1.779e-4
  n2 = -1.05e-6
  n3 = 1.6e-8
  n4 = -2.02e-6
  n5 = 15.868
  n6 = 0.01155
  n7 = -0.00423
  n8 = -4382.0
  n9 = 1.1455e6
  # Absolute refractive index of pure seawater
  nsw = (n0 + (n1 + n2 * _t + n3 * _t**2) * _s + n4 * _t**2 + (n5 + n6 * _s +
         n7 * _t) / _lambda + n8 / _lambda**2 + n9 / _lambda**3)
  nsw = nsw * n_air
  # Partial derivative of seawater refractive index w.r.t. salinity
  dnswds = (n1 + n2 * _t + n3 * _t**2 + n6 / _lambda) * n_air

  # Isothermal compressibility
  # from: Lepple & Millero (1971,Deep Sea-Research), pages 10-11
  # unit: pa
  # error: ~ +/-0.004e-6 bar^-1
  # pure water secant bulk Millero (1980, Deep-sea Research)
  kw = 19652.21 + 148.4206 * _t - 2.327105 * \
      _t**2 + 1.360477e-2 * _t**3 - 5.155288e-5 * _t**4
  # Btw_cal = 1.0 / kw
  # isothermal compressibility from Kell sound measurement in pure water
  # Btw = (50.88630+0.717582*_t+0.7819867e-3*_t**2+31.62214e-6*_t**3-0.1323594e-6*_t**4+0.634575e-9*_t**5)./(1+21.65928e-3*_t)*1e-6;
  # seawater secant bulk
  a0 = 54.6746 - 0.603459 * _t + 1.09987e-2 * _t**2 - 6.167e-5 * _t**3
  b0 = 7.944e-2 + 1.6483e-2 * _t - 5.3009e-4 * _t**2
  Ks = kw + a0 * _s + b0 * _s**1.5
  # calculate seawater isothermal compressibility from the secant bulk
  IsoComp = 1.0 / Ks * 1e-5

  # Density of water and seawater
  # from: UNESCO,38,1981
  # unit: Kg/m^3,
  a0 = 8.24493e-1
  a1 = -4.0899e-3
  a2 = 7.6438e-5
  a3 = -8.2467e-7
  a4 = 5.3875e-9
  a5 = -5.72466e-3
  a6 = 1.0227e-4
  a7 = -1.6546e-6
  a8 = 4.8314e-4
  b0 = 999.842594
  b1 = 6.793952e-2
  b2 = -9.09529e-3
  b3 = 1.001685e-4
  b4 = -1.120083e-6
  b5 = 6.536332e-9
  # density for pure water
  density_w = b0 + b1 * _t + b2 * _t**2 + b3 * _t**3 + b4 * _t**4 + b5 * _t**5
  # density for pure seawater
  density_sw = density_w + ((a0 + a1 * _t + a2 * _t**2 + a3 * _t**3 + a4 * _t**4) * _s
                            + (a5 + a6 * _t + a7 * _t**2) * _s**1.5 + a8 * _s**2)

  # Water activity data of seawater is
  # from: Millero and Leung (1976,American Journal of Science,276,1035-1077).
  # Table 19 was reproduced using Eq.(14,22,23,88,107)
  # then were fitted to polynominal equation.
  # dlnawds is partial derivative of natural logarithm of water activity
  # w.r.t. salinity
  # lnaw = (-1.64555e-6-1.34779e-7*_t+1.85392e-9*_t**2-1.40702e-11*_t**3)+
  #            (-5.58651e-4+2.40452e-7*_t-3.12165e-9*_t**2+2.40808e-11*_t**3).*_s+
  #            (1.79613e-5-9.9422e-8*_t+2.08919e-9*_t**2-1.39872e-11*_t**3).*_s**1.5+
  #            (-2.31065e-6-1.37674e-9*_t-1.93316e-11*_t**2).*_s**2;
  dlnawds = ((-5.58651e-4 + 2.40452e-7 * _t - 3.12165e-9 * _t**2 + 2.40808e-11 * _t**3) +
             1.5 * (1.79613e-5 - 9.9422e-8 * _t + 2.08919e-9 * _t**2 - 1.39872e-11 * _t**3) * _s**0.5 +
             2.0 * (-2.31065e-6 - 1.37674e-9 * _t - 1.93316e-11 * _t**2) * _s)

  # Density derivative of refractive index
  # from: PMH model
  # n density derivative
  n_wat = nsw
  n_wat2 = n_wat**2
  n_density_derivative = (n_wat2 - 1.0) * (1.0 + 2.0 / 3.0 *
                                           (n_wat2 + 2.0) *
                                           (n_wat / 3.0 - 1.0 / 3.0 / n_wat)**2)
  DFRI = n_density_derivative

  # Volume scattering at 90 degree due to the density fluctuation
  beta_df = np.pi * np.pi / 2 * \
      ((_lambda * 1e-9)**(-4)) * Kbz * Tk * IsoComp * \
      DFRI**2 * (6 + 6 * _delta) / (6 - 7 * _delta)
  # Volume scattering at 90 degree due to the concentration fluctuation
  flu_con = _s * M0 * dnswds**2 / density_sw / (-dlnawds) / Na
  beta_cf = 2 * np.pi * np.pi * \
      ((_lambda * 1e-9)**(-4)) * nsw**2 * \
      (flu_con) * (6 + 6 * _delta) / (6 - 7 * _delta)
  # Total volume scattering at 90 degree
  beta90sw = beta_df + beta_cf  # volume scattering at 90 degree
  # total scattering coefficient
  # bsw = 8 * np.pi / 3 * beta90sw * (2 + _delta) / (1 + _delta)
  betasw = beta90sw * (1 + ((np.cos(rad))**2) * (1 - _delta) / (1 + _delta))

  return betasw


def estimate_poc(_bbp, _lambda=440., _method="NAB08_down"):
  # Particulate Organic Carbon (POC) is leanearly proportional to
  # particulate backscattering (bbp), various empirical relationship exist,
  # few of them are implemented in this function
  #
  # /!\ The calculations used are applicable only in the top layer
  #     with a maximum depth defined by max(MLD, Zeu).
  #
  # INPUT:
  #   _bbp np.array, particulate backscattering (m^{-1})
  #   _lambda: np.array of float, wavelength (nm)
  #       default: 440
  #   _method: str, method to use for the estimation
  #       SOCCOM: POC = 3.23e4 x bbp(700) + 2.76
  #         an emprirical relationship built for the SOCCOM floats based on
  #         the relationship between the first profile of the floats and
  #         in-situ measurements taken during deployement
  #         (cruises: PS89, P16S and IN2015v1)
  #       NAB08_down or NAB08_up: Specific to North Atlantic in Spring
  #         based on empirical relationship (n=321), with data points
  #         ranging between 0-600 m, recommend downast
  #
  # /!\ Make sure that all np.array are same size
  #
  # OUTPUT:
  #   poc np.array, particulate organic carbon concentration (mg.m^{-3})
  #   poc_lower np.array, lower poc estimation
  #   poc_upper np.array, upper poc estimation
  #
  #
  # REFERENCES:
  #     I. Cetinić et al., Particulate organic carbon and inherent optical
  #   properties during 2008 North Atlantic bloom experiment.
  #   J. Geophys. Res. Ocean. 117 (2012), doi:10.1029/2011JC007771.
  #   Emmanuel Boss, Marc Picheral, Thomas Leeuw, Alison Chase, Eric Karsenti,
  # Gabriel Gorsky, Lisa Taylor, Wayne Slade, Josephine Ras and Herve Claustre.
  # The characteristics of particulate absorption, scattering and attenuation
  # coefficients in the surface ocean; Contribution of the Tara Oceans
  # expedition. Methods in Oceanography, 7:52?62, 2013.
  # ISSN 22111220. doi: 10.1016/j.mio.2013.11.002.
  # URL http://dx.doi. org/10.1016/j.mio.2013.11.002.

  if _method == "SOCCOM":
    # switch to bbp(700)
    bbp_700 = _bbp * (700. / _lambda) ** (-0.78)
    # estimate poc from bbp(700)
    poc = 3.23 * 1e4 * bbp_700 + 2.76
    poc_lower = poc * 0.95
    poc_upper = poc * 1.05
  elif _method == "NAB08_up":
    # upcast
    bbp_700 = _bbp * (700. / _lambda) ** (-0.78)
    poc = 43317 * bbp_700 - 18.4
    poc_lower = (43317 - 2092) * bbp_700 - (18.4 + 5.8)
    poc_upper = (43317 + 2092) * bbp_700 - (18.4 - 5.8)
  elif _method == "NAB08_down":
    # downcast
    bbp_700 = _bbp * (700. / _lambda) ** (-0.78)
    poc = 35422 * bbp_700 - 14.4
    poc_lower = (35422 - 1754) * bbp_700 - (14.4 + 5.8)
    poc_upper = (35422 + 1754) * bbp_700 - (14.4 - 5.8)
  else:
    print("Unknown method")

  return {"poc": poc, "poc_lower": poc_lower, "poc_upper": poc_upper}


def estimate_cphyto(_bbp, _lambda=440., _method="Graff2015"):
  # Estimate phytoplankton carbon biomass (Cphyto) from
  # particulate backscattering (bbp). The two methods available are based on
  # relation with bbp at 440 nm, the parameter _lambda can be used to
  # shift the relation to 700 nm.
  #
  # /!\ The calculations used are applicable only in the top layer
  #     with a maximum depth defined by max(MLD, Zeu).
  #
  # INPUT:
  #   _bbp np.array, particulate backscattering (m^{-1})
  #   _lambda: np.array of float, wavelength (nm)
  #       default: 440
  #   _method: str, method to use for the estimation
  #       Graff2015: based on empirical data (default)
  #       Behrenfeld2005: based on a model
  #
  # /!\ Make sure that all np.array are same size
  #
  # OUTPUT:
  #   Cphyto np.array, phytoplankton carbon (mg.m^{-3})
  #
  #
  # REFERENCES:
  #   Michael J. Behrenfeld, Emmanuel Boss, David a. Siegel and Donald M. Shea.
  # Carbon-based ocean produc- tivity and phytoplankton physiology from space.
  # Global Biogeochemical Cycles, 19(1):1-14, 2005.
  # ISSN 08866236. doi: 10.1029/2004GB002299.
  #   Jason R. Graff, Toby K. Westberry, Allen J. Milligan,
  # Matthew B. Brown, Giorgio Dall'Olmo, Virginie van Dongen-Vogels,
  # Kristen M. Reifel, and Michael J. Behrenfeld. Analytical phytoplankton
  # carbon measurements spanning diverse ecosystems. Deep-Sea Research Part I:
  # Oceanographic Research Papers, 102:16?25, 2015. ISSN 09670637.
  # doi: 10.1016/j.dsr.2015.04.006.
  # URL http://dx.doi.org/10.1016/ j.dsr.2015.04.006.
  #   Emmanuel Boss, Marc Picheral, Thomas Leeuw, Alison Chase, Eric Karsenti,
  # Gabriel Gorsky, Lisa Taylor, Wayne Slade, Josephine Ras, and Herve Claustre.
  # The characteristics of particulate absorption, scattering and attenuation
  # coefficients in the surface ocean; Contribution of the Tara Oceans
  # expedition. Methods in Oceanography, 7:52?62, 2013.
  # ISSN 22111220. doi: 10.1016/j.mio.2013.11.002.
  # URL http://dx.doi. org/10.1016/j.mio.2013.11.002.

  if _method == "Behrenfeld2005":
    # switch to bbp(440)
    bbp_440 = _bbp * (440.0 / _lambda) ** (-0.78)
    # estimate cphyto from bbp(700)
    return 13000 * (bbp_440 - 0.00035)
  elif _method == 'Graff2015':
    # switch to bbp(470)
    bbp_470 = _bbp * (470.0 / _lambda) ** (-0.78)
    # estimate cphyto from bbp(700)
    return 12128 * bbp_470 + 0.59
  else:
    print('Unknown method ')


###############
#   HELPERS   #
###############

def indices(_a, _func):
  # Find indices of elements in array a matching criteria
  # described in function func
  # Equivalent to function find of matlab
  #
  # INPUT:
  #   _a array of elements to filter
  #   _func filtering function to apply
  #
  # OUTPUT:
  #   return the indices corresponding to the elements matching cirteria
  #
  # EXAMPLE:
  #   a = [1, 2, 3, 1, 2, 3, 1, 2, 3]
  #   inds = indices(a, lambda x: x > 2)
  #   print(inds)

  return [i for (i, val) in enumerate(_a) if _func(val)]


def regress2(_x, _y, _method_type_1="default",
             _method_type_2="reduced major axis",
             _weight_x=[], _weight_y=[], _need_intercept=True):
  # Regression Type II
  # Type II regressions are recommended if there is variability on both x and y
  # It's computing the linear regression type I for (x,y) and (y,x)
  # and then average relationship with one of the type II methods
  #
  # INPUT:
  #   _x np.array
  #   _y np.array
  #   _method_type_1 str method to use for regression type I:
  #     ordinary least square or OLS
  #     weighted least square or WLS
  #     robust linear model or RLM
  #   _method_type_2 str method to use for regression type II:
  #     major axis
  #     reduced major axis (geometric mean)
  #     arithmetic mean
  #   _need_intercept boolean
  #     True (default) add a constant to relation
  #     False force relation to go by 0
  #   _weight_x np.array containing the weigth of x
  #   _weigth_y np.array containing the weigth of y
  #
  # OUTPUT:
  #   slope
  #   intercept
  #   r
  #   std_slope
  #   std_intercept
  #   predict
  #   confidence_interval (coming soon)
  #
  # REQUIRE:
  #   numpy
  #   statsmodels

  # Check input arguments
  if _method_type_2 != "reduced major axis" and _method_type_1 != "default":
    print("The type II method choosen doesn't use type I regression")
  elif _method_type_1 == "default":
    _method_type_1 = "ordinary least square"

  # Set x, y depending on intercept requirement
  if _need_intercept:
    x_intercept = sm.add_constant(_x)
    y_intercept = sm.add_constant(_y)

  # Compute Regression Type I (if necessary)
  if (_method_type_2 == "reduced major axis" or
          _method_type_2 == "geometric mean"):
    if _method_type_1 == "OLS" or _method_type_1 == "ordinary least square":
      if _need_intercept:
        [intercept_a, slope_a] = sm.OLS(_y, x_intercept).fit().params
        [intercept_b, slope_b] = sm.OLS(_x, y_intercept).fit().params
      else:
        slope_a = sm.OLS(_y, _x).fit().params
        slope_b = sm.OLS(_x, _y).fit().params
    elif _method_type_1 == "WLS" or _method_type_1 == "weighted least square":
      if _need_intercept:
        [intercept_a, slope_a] = sm.WLS(
            _y, x_intercept, weights=1. / _weight_y).fit().params
        [intercept_b, slope_b] = sm.WLS(
            _x, y_intercept, weights=1. / _weight_x).fit().params
      else:
        slope_a = sm.WLS(_y, _x, weights=1. / _weight_y).fit().params
        slope_b = sm.WLS(_x, _y, weights=1. / _weight_x).fit().params
    elif _method_type_1 == "RLM" or _method_type_1 == "robust linear model":
      if _need_intercept:
        [intercept_a, slope_a] = sm.RLM(_y, x_intercept).fit().params
        [intercept_b, slope_b] = sm.RLM(_x, y_intercept).fit().params
        print(slope_a, intercept_a)
        print(slope_b, intercept_b)
      else:
        slope_a = sm.RLM(_y, _x).fit().params
        slope_b = sm.RLM(_x, _y).fit().params
    else:
      print('Unknon method type 1')
      return {}

  # Compute Regression Type II
  if (_method_type_2 == "reduced major axis" or
          _method_type_2 == "geometric mean"):
    # Transpose coefficients
    if _need_intercept:
      intercept_b = -intercept_b / slope_b
    slope_b = 1 / slope_b
    # Check if correlated in same direction
    if np.sign(slope_a) != np.sign(slope_b):
        print('Regression Type I are of opposite sign')
        return {}
    # Compute Reduced Major Axis Slope
    slope = np.sign(slope_a) * np.sqrt(slope_a * slope_b)
    if _need_intercept:
      # Compute Intercept (use mean for least square)
      if _method_type_1 == "OLS" or _method_type_1 == "ordinary least square":
        intercept = np.mean(_y) - slope * np.mean(_x)
      else:
        intercept = np.median(_y) - slope * np.median(_x)
    else:
      intercept = 0
    # Compute r
    r = np.sign(slope_a) * np.sqrt(slope_a / slope_b)
    # Compute predicted values
    predict = slope * _x + intercept
    # Compute standard deviation of the slope and the intercept
    n = len(_x)
    diff = _y - predict
    Sx2 = np.sum(np.multiply(_x, _x))
    den = n * Sx2 - np.sum(_x) ** 2
    s2 = np.sum(np.multiply(diff, diff)) / (n - 2)
    std_slope = np.sqrt(n * s2 / den)
    if _need_intercept:
      std_intercept = np.sqrt(Sx2 * s2 / den)
    else:
      std_intercept = 0
  elif (_method_type_2 == "Pearson's major axis" or
        _method_type_2 == "major axis"):
    if not _need_intercept:
      print("This method require an intercept")
    xm = np.mean(_x)
    ym = np.mean(_y)
    xp = _x - xm
    yp = _y - ym
    sumx2 = np.sum(np.multiply(xp, xp))
    sumy2 = np.sum(np.multiply(yp, yp))
    sumxy = np.sum(np.multiply(xp, yp))
    slope = ((sumy2 - sumx2 + np.sqrt((sumy2 - sumx2)**2 + 4 * sumxy**2)) /
             (2 * sumxy))
    intercept = ym - slope * xm
    # Compute r
    r = sumxy / np.sqrt(sumx2 * sumy2)
    # Compute standard deviation of the slope and the intercept
    n = len(_x)
    std_slope = (slope / r) * np.sqrt((1 - r ** 2) / n)
    sigx = np.sqrt(sumx2 / (n - 1))
    sigy = np.sqrt(sumy2 / (n - 1))
    std_i1 = (sigy - sigx * slope) ** 2
    std_i2 = (2 * sigx * sigy) + ((xm ** 2 * slope * (1 + r)) / r ** 2)
    std_intercept = np.sqrt((std_i1 + ((1 - r) * slope * std_i2)) / n)
    # Compute predicted values
    predict = slope * _x + intercept
  elif _method_type_2 == "arithmetic mean":
    if not _need_intercept:
      print("This method require an intercept")
    n = len(_x)
    sg = np.floor(n / 2)
    # Sort x and y in order of x
    sorted_index = sorted(range(len(_x)), key=lambda i: _x[i])
    x_w = np.array([_x[i] for i in sorted_index])
    y_w = np.array([_y[i] for i in sorted_index])
    x1 = x_w[1:sg + 1]
    x2 = x_w[sg:n]
    y1 = y_w[1:sg + 1]
    y2 = y_w[sg:n]
    x1m = np.mean(x1)
    x2m = np.mean(x2)
    y1m = np.mean(y1)
    y2m = np.mean(y2)
    xm = (x1m + x2m) / 2
    ym = (y1m + y2m) / 2
    slope = (x2m - x1m) / (y2m - y1m)
    intercept = ym - xm * slope
    # r (to verify)
    r = []
    # Compute predicted values
    predict = slope * _x + intercept
    # Compute standard deviation of the slope and the intercept
    std_slope = []
    std_intercept = []

  # Return all that
  return {"slope": slope, "intercept": intercept, "r": r,
          "std_slope": std_slope, "std_intercept": std_intercept,
          "predict": predict}

import os
import sys
import stimela.dismissable as sdm

NAME = 'Self calibration loop'


def worker(pipeline, recipe, config):
    npix = config['img_npix']
    trim = config['img_trim']
    spwid = config.get('spwid', 0)
    cleanborder = 0
    cell = config['img_cell']
    mgain = config['img_mgain']
    niter = config['img_niter']
    auto_thresh = config['img_autothreshold']
    auto_mask = config['img_automask']
    robust = config['img_robust']
    nchans = config['img_nchans']
    pol = config.get('img_pol', 'I')
    thresh_pix = config['sf_thresh_isl']
    thresh_isl = config['sf_thresh_pix']
    column = config['img_column']
    joinchannels = config['img_joinchannels']
    fit_spectral_pol = config['img_fit_spectral_pol']
    gsols = config.get('cal_Gsols', [1,1])
    taper = config.get('img_uvtaper', None)
    mfsprefix = ["",'-MFS'][int(nchans>1)]

    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataid]
    prefix = pipeline.prefix

    # Define image() extract_sources() calibrate()
    # functions for convience

    def image(num):
        key = 'image_{}'.format(num)
        mask = False
        if config[key].get('peak_based_mask_on_dirty', False):
            mask = True
            step = 'image_{}_dirty'.format(num)
            recipe.add('cab/wsclean', step,
                  {                   
                      "msname"    : mslist,
                      "column"    : config[key].get('column', column),
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config[key].get('npix', npix),
                      "trim"      : config[key].get('trim', trim),
                      "scale"     : config[key].get('cell', cell),
                      "pol"       : config[key].get('pol', pol),
                      "channelsout"   : nchans,
                      "taper-gaussian" : sdm.dismissable(config[key].get('uvtaper', taper)),
                      "prefix"    : '{0:s}_{1:d}'.format(prefix, num),
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make dirty image to create clean mask'.format(step))
            
            step = 'mask_dirty_{}'.format(num)
            recipe.add('cab/cleanmask', step,
               {
                 "image"           :  '{0:s}_{1:d}{2:s}-image.fits:output'.format(prefix, num, mfsprefix),
                 "output"          :  '{0:s}_{1:d}-mask.fits'.format(prefix, num),
                 "dilate"          :  False,
                 "peak-fraction"   :  0.5,
                 "no-negative"     :  True,
                 "boxes"           :  1,
                 "log-level"       :  'DEBUG',
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Make mask based on peak of dirty image'.format(step))
        
        elif config[key].get('mask', False):
            mask = True
            sigma = config[key].get('mask_sigma', None)
            pf = config[key].get('mask_peak_fraction', None)
            step = 'mask_{}'.format(num)
            recipe.add('cab/cleanmask', step,
               {
                 "image"           :  '{0:s}_{1:d}{2:s}-image.fits:output'.format(prefix, num-1, mfsprefix),
                 "output"          :  '{0:s}_{1:d}-mask.fits'.format(prefix, num),
                 "dilate"          :  False,
                 "peak-fraction"   :  sdm.dismissable(pf),
                 "sigma"           :  sdm.dismissable(sigma),
                 "no-negative"     :  True,
                 "boxes"           :  1,
                 "log-level"       :  'DEBUG',
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: Make mask based on peak of dirty image'.format(step))


        step = 'image_{}'.format(num)
        image_opts = {                   
                  "msname"    : mslist,
                  "column"    : config[key].get('column', column),
                  "weight"    : 'briggs {}'.format(config[key].get('robust', robust)),
                  "npix"      : config[key].get('npix', npix),
                  "trim"      : config[key].get('trim', trim),
                  "scale"     : config[key].get('cell', cell),
                  "prefix"    : '{0:s}_{1:d}'.format(prefix, num),
                  "niter"     : config[key].get('niter', niter),
                  "mgain"     : config[key].get('mgain', mgain),
                  "pol"       : config[key].get('pol', pol),
                  "taper-gaussian" : sdm.dismissable(config[key].get('uvtaper', taper)),
                  "channelsout"     : nchans,
                  "joinchannels"    : config[key].get('joinchannels', joinchannels),
                  "fit-spectral-pol": config[key].get('fit_spectral_pol', fit_spectral_pol),
                  "auto-threshold": config[key].get('autothreshold', auto_thresh),
              }
        if mask:
            image_opts.update( {"fitsmask" : '{0:s}_{1:d}-mask.fits:output'.format(prefix, num)} )
        else:
            image_opts.update( {"auto-mask" : config[key].get('automask', auto_mask)} )

        recipe.add('cab/wsclean', step,
        image_opts,
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Make image after first round of calibration'.format(step))


    def make_cube(num, imtype='model'): 
        im = '{0:s}_{1}-cube.fits:output'.format(prefix, num)
        step = 'makecube_{}'.format(num)
        images = ['{0:s}_{1}-{2:04d}-{3:s}.fits:output'.format(prefix, num, i, imtype) for i in range(nchans)]
        recipe.add('cab/fitstool', step,
            {                   
                "image"     : images,
                "output"    : im,
                "stack"     : True,
                "fits-axis" : 'FREQ',
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Make convolved model'.format(step))

        return im

    def extract_sources(num):
        key = 'extract_sources_{0:d}'.format(num)
        if config[key].get('detection_image', False):
            step = 'detection_image_{0:d}'.format(num)
            detection_image = prefix + '-detection_image_{0:d}.fits:output'.format(num)
            recipe.add('cab/fitstool', step,
                {                   
                    "image"    : [prefix+'_{0:d}{2:s}-{1:s}.fits:output'.format(num, im, mfsprefix) for im in ('image','residual')],
                    "output"   : detection_image,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))
        else:
            detection_image = None

        spi_do = config[key].get('spi', False) 
        if spi_do:
            im = make_cube(num, 'image')
        else:
            im = '{0:s}_{1:d}{2:s}-image.fits:output'.format(prefix, num, mfsprefix)

        step = 'extract_{0:d}'.format(num)
        calmodel = '{0:s}_{1:d}-pybdsm'.format(prefix, num)
        if detection_image:
            blank_limit = 1e-9
        else:
            blank_limit = None
        recipe.add('cab/pybdsm', step,
            {                   
                "image"         : im,
                "thresh_pix"    : config[key].get('thresh_pix', thresh_pix),
                "thresh_isl"    : config[key].get('thresh_isl', thresh_isl),
                "outfile"       : '{:s}.fits:output'.format(calmodel),
                "blank_limit"   : sdm.dismissable(blank_limit),
                "adaptive_rms_box" : True,
                "port2tigger"   : True,
                "multi_chan_beam": spi_do,
                "spectralindex_do": spi_do,
                "detection_image": sdm.dismissable(detection_image),
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Extract sources'.format(step))

    def predict_from_fits(num, model, index):
        if isinstance(model, str) and len(model.split('+'))==2:
            combine = True
            mm = model.split('+')
            # Combine FITS models if more than one is given
            step = 'combine_models_' + '_'.join(map(str, mm))
            calmodel = '{0:s}_{1:d}-FITS-combined.fits:output'.format(prefix, num)
            cubes = [ make_cube(n, 'model') for n in mm]
            recipe.add('cab/fitstool', step,
                {
                    "image"    : cubes,
                    "output"   : calmodel,
                    "sum"      : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add clean components'.format(step))
        else:
            calmodel = make_cube(num)
            
        step = 'predict_fromfits_{}'.format(num)
        recipe.add('cab/lwimager', 'predict', {
                "msname"        : mslist[index], 
                "simulate_fits" : calmodel,
                "column"        : 'MODEL_DATA',
                "img_nchan"     : nchans,
                "img_chanstep"  : 1,
                "nchan"         : pipeline.nchans[index][spwid],
                "cellsize"      : cell,
                "chanstep"      : 1,
        },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Predict from FITS ms={1:s}'.format(step, mslist[index]))

    def calibrate(num):
        key = 'calibrate_{0:d}'.format(num)
        model = config[key].get('model', num)
        vismodel = config[key].get('add_vis_model', False)
        if config[key].get('visonly', False):
            vismodel = True
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
            for i,msname in enumerate(mslist):
                predict_from_fits(num, model, i)
        else: 
            if isinstance(model, str) and len(model.split('+'))==2:
                combine = True
                mm = model.split('+')
                models = [ '{0:s}_{1:s}-pybdsm.lsm.html:output'.format(prefix, m) for m in mm]
                calmodel = '{0:s}_{1:d}-pybdsm-combined.lsm.html:output'.format(prefix, num)

                step = 'combine_models_' + '_'.join(map(str, mm))
                recipe.add('cab/tigger_convert', step,
                    {                   
                        "input-skymodel"    : models[0],
                        "append"    : models[1],
                        "output-skymodel"   : calmodel,
                        "rename"  : True,
                        "force"   : True,
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Combined models'.format(step))

            else:
                model = int(model)
                calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)

        for i,msname in enumerate(mslist):
            step = 'calibrate_{0:d}_{1:d}'.format(num, i)
            recipe.add('cab/calibrator', step,
               {
                 "skymodel"     :  calmodel,
                 "add-vis-model":  vismodel,
                 "msname"       :  msname,
                 "threads"      :  16,
                 "column"       :  "DATA",
                 "output-data"  : config[key].get('output_data', 'CORR_RES'),
                 "output-column": "CORRECTED_DATA",
                 "Gjones"       : True,
                 "Gjones-solution-intervals" : config[key].get('Gsols', gsols),
                 "Gjones-matrix-type" : config[key].get('gain_matrix_type', 'GainDiagPhase'),
                 "read-flags-from-ms" :	True,
                 "read-flagsets"      : "-stefcal",
                 "write-flagset"      : "stefcal",
                 "write-flagset-policy" : "replace",
                 "Gjones-ampl-clipping" :  True,
                 "Gjones-ampl-clipping-low" : config.get('cal_gain_amplitude_clip_low', 0.5),
                 "Gjones-ampl-clipping-high": config.get('cal_gain_amplitude_clip_high', 1.5),
                 "label"              : 'cal{0:d}'.format(num),
                 "make-plots"         : True,
                 "tile-size"          : 512,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: First selfcal ms={1:s}'.format(step, msname))

    # selfcal loop
    if pipeline.enable_task(config, 'image_1'):
        image(1)
    
    if pipeline.enable_task(config, 'extract_sources_1'):
        extract_sources(1)

    if pipeline.enable_task(config, 'calibrate_1'):
        calibrate(1)
        
    if pipeline.enable_task(config, 'image_2'):
        image(2)

    if pipeline.enable_task(config, 'extract_sources_2'):
        extract_sources(2)

    if pipeline.enable_task(config, 'calibrate_2'):
        calibrate(2)

    if pipeline.enable_task(config, 'image_3'):
        image(3)

    if pipeline.enable_task(config, 'extract_sources_3'):
        extract_sources(3)

    if pipeline.enable_task(config, 'calibrate_3'):
        calibrate(3)

    if pipeline.enable_task(config, 'image_4'):
        image(4)

    if pipeline.enable_task(config, 'calibrate_4'):
        calibrate(4)

    if pipeline.enable_task(config, 'image_5'):
        image(5)

    if pipeline.enable_task(config, 'restore_model'):
        num = config['restore_model']['model']

        if isinstance(num, str) and len(num.split('+'))==2:
            combine = True
            mm = num.split('+')
            models = [ '{0:s}_{1:s}-pybdsm.lsm.html:output'.format(prefix, m) for m in mm]
            final = '{0:s}_final-pybdsm.lsm.html:output'.format(prefix)

            step = 'combine_models_{0:s}_{1:s}'.format(*mm)
            recipe.add('cab/tigger_convert', step,
                {                   
                    "input-skymodel"    : models[0],
                    "append"    : models[1],
                    "output-skymodel"   : final,
                    "rename"  : True,
                    "force"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))

        else:
            num = int(model)
            final = '{0:s}_final-pybdsm.lsm.html:output'.format(prefix)

        if config['restore_model'].get('clean_model', None):
            num = int(config['restore_model'].get('clean_model', None))
       
            conv_model = prefix + '-convolved_model.fits:output'
            recipe.add('cab/fitstool', step,
                {
                    "image"    : [prefix+'_{0:d}{2:s}-{1:s}.fits:output'.format(num, im, mfsprefix) for im in ('image','residual')],
                    "output"   : conv_model,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))

            with_cc = prefix + '-with_cc.fits:output'
            recipe.add('cab/fitstool', step,
                {
                    "image"    : [prefix+'_5{0:s}-image.fits:output'.format(mfsprefix), conv_model],
                    "output"   : with_cc,
                    "sum"      : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add clean components'.format(step))

            recipe.add('cab/tigger_restore', step,
                {
                    "input-image"    : with_cc,
                    "input-skymodel" : final,
                    "output-image"   : prefix+'.fullrest.fits',
                    "force"          : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add extracted skymodel'.format(step))

        for i,msname in enumerate(mslist):
            if pipeline.enable_task(config, 'flagging_summary'):
                step = 'flagging_summary_image_selfcal_{0:d}'.format(i)
                recipe.add('cab/casa_flagdata', step,
                    {
                      "vis"         : msname,
                      "mode"        : 'summary',
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Flagging summary  ms={1:s}'.format(step, msname))

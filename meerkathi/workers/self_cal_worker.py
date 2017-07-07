import os
import sys

NAME = 'Self calibration loop'

def worker(pipeline, recipe, config):
    npix = config['img_npix']
    trim = config['img_trim']
    cleanborder = 0
    cell = config['img_cell']
    mgain = config['img_mgain']
    niter = config['img_niter']
    auto_thresh = config['img_autothreshold']
    auto_mask = config['img_automask']
    robust = config['img_robust']
    nchans = config['img_nchans']
    thresh_pix = config['sf_thresh_isl']
    thresh_isl = config['sf_thresh_pix']
    column = config['img_column']

    mslist = ['{0:s}-{1:s}.ms'.format(did, config['label']) for did in pipeline.dataids]
    prefix = pipeline.prefix

    if pipeline.enable_task(config, 'image_1'):
        if config['image_1']['peak_based_mask_on_dirty']:
            step = 'image_1_dirty'
            recipe.add('cab/wsclean', step,
                  {                   
                      "msname"    : mslist,
                      "column"    : config['image_1'].get('column', column),
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config['image_1'].get('npix', npix),
                      "trim"      : config['image_1'].get('trim', trim),
                      "scale"     : config['image_1'].get('cell', cell),
                      "channelsout"   : nchans,
                      "prefix"    : prefix+'_1',
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make dirty image to create clean mask'.format(step))

            step = 'mask_1'
            mask = prefix+'_1-mask.fits:output'
            recipe.add('cab/cleanmask', step,
                  {                   
                      "image"    : prefix+'_1-MFS-dirty.fits:output',
                      "output"   : mask,
                      "peak-fraction"    : config['image_1']['peak_based_mask_on_dirty'],
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make peak based mask from dirty image'.format(step))

            step = 'image_1'
            recipe.add('cab/wsclean', step,
                  {                   
                      "msname"    : mslist,
                      "column"    : config['image_1'].get('column', column),
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config['image_1'].get('npix', npix),
                      "trim"      : config['image_1'].get('trim', trim),
                      "scale"     : config['image_1'].get('cell', cell),
                      "prefix"    : prefix+'_1',
                      "niter"     : config['image_1'].get('niter', niter),
                      "mgain"     : config['image_1'].get('mgain', mgain),
                      "channelsout"   : nchans,
                      "auto-threshold": config['image_1'].get('autothreshold', auto_thresh),
                      "fitsmask"  : mask,
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Image with clean peak-based mask from dirty image'.format(step))
 
    if pipeline.enable_task(config, 'extract_sources_1'):
        if config['extract_sources_1']['detection_image']:
            step = 'detection_image_1'
            detection_image = prefix + '-detection_image_1.fits:output'
            recipe.add('cab/fitstool', step,
                {                   
                    "image"    : [prefix+'_1-MFS-{:s}.fits:output'.format(im) for im in ('image','residual')],
                    "output"   : detection_image,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))
        else:
            detection_image = None
           
        step = 'extract_1'
        calmodel = prefix+'_1-pybdsm'
        recipe.add('cab/pybdsm', step,
            {                   
                "image"    : prefix+'_1-MFS-image.fits:output',
                "thresh_pix"   : config['extract_sources_1'].get('thresh_pix', thresh_pix),
                "thresh_isl"   : config['extract_sources_1'].get('thresh_isl', thresh_isl),
                "outfile"  : '{:s}.fits:output'.format(calmodel),
                "blank_limit": 1e-9,
                "port2tigger": True,
                "detection_image": detection_image,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Extract sources'.format(step))

    if pipeline.enable_task(config, 'calibrate_1'):
        for i,msname in enumerate(mslist):
            step = 'calibrate_1_{:d}'.format(i)
            recipe.add('cab/calibrator', step,
               {
                 "skymodel"     :  '{:s}_1-pybdsm.lsm.html:output'.format(prefix),
                 "msname"       :  msname,
                 "threads"      :  16,
                 "column"       :  "DATA",
                 "output-data"  : config['calibrate_1']['output_data'],
                 "output-column": "CORRECTED_DATA",
                 "Gjones"       : True,
                 "Gjones-solution-intervals" : config['cal_Gsols'],
                 "Gjones-matrix-type" : config['calibrate_1']['gain_matrix_type'],
                 "read-flags-from-ms" :	True,
                 "read-flagsets"      : "-stefcal",
                 "write-flagset"      : "stefcal",
                 "write-flagset-policy" : "replace",
                 "Gjones-ampl-clipping" :  True,
                 "Gjones-ampl-clipping-low" : config['cal_gain_amplitude_clip_low'],
                 "Gjones-ampl-clipping-high": config['cal_gain_amplitude_clip_high'],
                 "label"              : "cal1",
                 "make-plots"         : True,
                 "tile-size"          : 512,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: First selfcal ms={1:s}'.format(step, msname))
        
    if pipeline.enable_task(config, 'image_2'):
        step = 'image_2'
        recipe.add('cab/wsclean', step,
              {                   
                  "msname"    : mslist,
                  "column"    : config['image_2'].get('column', column),
                  "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                  "npix"      : config['image_2'].get('npix', npix),
                  "trim"      : config['image_2'].get('trim', trim),
                  "scale"     : config['image_2'].get('cell', cell),
                  "prefix"    : prefix+'_2',
                  "niter"     : config['image_2'].get('niter', niter),
                  "mgain"     : config['image_2'].get('mgain', mgain),
                  "channelsout"   : nchans,
                  "auto-threshold": config['image_2'].get('autothreshold', auto_thresh),
                  "auto-mask"  :   config['image_2'].get('automask', auto_mask),
              },
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Make image after first round of calibration'.format(step))

    if pipeline.enable_task(config, 'extract_sources_2'):
        if config['extract_sources_2']['detection_image']:
            step = 'detection_image_2'
            detection_image = prefix + '-detection_image_2.fits:output'
            recipe.add('cab/fitstool', step,
                {                   
                    "image"    : [prefix+'_2-MFS-{:s}.fits:output'.format(im) for im in ('image','residual')],
                    "output"   : detection_image,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))
        else:
            detection_image = None
           
        step = 'extract_2'
        calmodel = prefix+'_2-pybdsm'
        recipe.add('cab/pybdsm', step,
            {                   
                "image"    : prefix+'_2-MFS-image.fits:output',
                "thresh_pix"   : config['extract_sources_2'].get('thresh_pix', thresh_pix),
                "thresh_isl"   : config['extract_sources_2'].get('thresh_isl', thresh_isl),
                "outfile"  : '{:s}.fits:output'.format(calmodel),
                "blank_limit": 1e-9,
                "port2tigger": True,
                "detection_image": detection_image,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Extract sources'.format(step))

    if pipeline.enable_task(config, 'calibrate_2'):
        for i,msname in enumerate(mslist):
            step = 'calibrate_2_{:d}'.format(i)
            recipe.add('cab/calibrator', step,
               {
                 "skymodel"     :  '{:s}_2-pybdsm.lsm.html:output'.format(prefix),
                 "msname"       :  msname,
                 "threads"      :  16,
                 "column"       :  "DATA",
                 "output-data"  : config['calibrate_2']['output_data'],
                 "output-column": "CORRECTED_DATA",
                 "Gjones"       : True,
                 "Gjones-solution-intervals" : config['cal_Gsols'],
                 "Gjones-matrix-type" : config['calibrate_2']['gain_matrix_type'],
                 "read-flags-from-ms" :	True,
                 "read-flagsets"      : "-stefcal",
                 "write-flagset"      : "stefcal",
                 "write-flagset-policy" : "replace",
                 "Gjones-ampl-clipping" :  True,
                 "Gjones-ampl-clipping-low" : config['cal_gain_amplitude_clip_low'],
                 "Gjones-ampl-clipping-high": config['cal_gain_amplitude_clip_high'],
                 "label"              : "cal2",
                 "make-plots"         : True,
                 "tile-size"          : 512,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: First selfcal ms={1:s}'.format(step, msname))


    if pipeline.enable_task(config, 'image_3'):
            step = 'image_3'
            recipe.add('cab/wsclean', step,
                  {                   
                      "msname"    : mslist,
                      "column"    : config['image_3'].get('column', column),
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config['image_3'].get('npix', npix),
                      "trim"      : config['image_3'].get('trim', trim),
                      "scale"     : config['image_3'].get('cell', cell),
                      "prefix"    : prefix+'_3',
                      "niter"     : config['image_3'].get('niter', niter),
                      "mgain"     : config['image_3'].get('mgain', mgain),
                      "channelsout"   : nchans,
                      "auto-threshold": config['image_3'].get('autothreshold', auto_thresh),
                      "auto-mask"  :   config['image_3'].get('automask', auto_mask),
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make image after first round of calibration'.format(step))

    if pipeline.enable_task(config, 'extract_sources_3'):
        if config['extract_sources_3']['detection_image']:
            step = 'detection_image_3'
            detection_image = prefix + '-detection_image_3.fits:output'
            recipe.add('cab/fitstool', step,
                {                   
                    "image"    : [prefix+'_3-MFS-{:s}.fits:output'.format(im) for im in ('image','residual')],
                    "output"   : detection_image,
                    "diff"     : True,
                    "force"    : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Make convolved model'.format(step))
        else:
            detection_image = None
           
        step = 'extract_3'
        calmodel = prefix+'_3-pybdsm'
        recipe.add('cab/pybdsm', step,
            {                   
                "image"    : prefix+'_3-MFS-image.fits:output',
                "thresh_pix"   : config['extract_sources_3'].get('thresh_pix', thresh_pix),
                "thresh_isl"   : config['extract_sources_3'].get('thresh_isl', thresh_isl),
                "outfile"  : '{:s}.fits:output'.format(calmodel),
                "blank_limit": 1e-9,
                "port2tigger": True,
                "detection_image": detection_image,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Extract sources'.format(step))
        
        if config['extract_sources_3']['append_prev']:
            step = 'append_model_3'
            combined = prefix+'_3-pybdsm-combined'
            recipe.add('cab/tigger_convert', step,
                {                   
                    "input-skymodel"    : prefix+'_2-pybdsm.lsm.html:output',
                    "append"  : prefix+'_3-pybdsm.lsm.html:output',
                    "output-skymodel"   : combined+'.lsm.html',
                    "rename"  : True,
                    "force"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))

    if pipeline.enable_task(config, 'calibrate_3'):
        for i,msname in enumerate(mslist):
            step = 'calibrate_3_{:d}'.format(i)
            recipe.add('cab/calibrator', step,
               {
                 "skymodel"     :  '{0:s}_3-pybdsm{1:s}.lsm.html:output'.format(prefix, 
                                   '-combined' if config['extract_sources_3']['append_prev'] else ''),
                 "msname"       :  msname,
                 "threads"      :  16,
                 "column"       :  "DATA",
                 "output-data"  : config['calibrate_3']['output_data'],
                 "output-column": "CORRECTED_DATA",
                 "Gjones"       : True,
                 "Gjones-solution-intervals" : config['cal_Gsols'],
                 "Gjones-matrix-type" : config['calibrate_3']['gain_matrix_type'],
                 "read-flags-from-ms" :	True,
                 "read-flagsets"      : "-stefcal",
                 "write-flagset"      : "stefcal",
                 "write-flagset-policy" : "replace",
                 "Gjones-ampl-clipping" :  True,
                 "Gjones-ampl-clipping-low" : config['cal_gain_amplitude_clip_low'],
                 "Gjones-ampl-clipping-high": config['cal_gain_amplitude_clip_high'],
                 "label"              : "cal3",
                 "make-plots"         : True,
                 "tile-size"          : 512,
               },
               input=pipeline.input,
               output=pipeline.output,
               label='{0:s}:: First selfcal ms={1:s}'.format(step, msname))

    if pipeline.enable_task(config, 'image_4'):
            step = 'image_4'
            recipe.add('cab/wsclean', step,
                  {                   
                      "msname"    : mslist,
                      "column"    : config['image_4'].get('column', column),
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config['image_4'].get('npix', npix),
                      "trim"      : config['image_4'].get('trim', trim),
                      "scale"     : config['image_4'].get('cell', cell),
                      "prefix"    : prefix+'_4',
                      "niter"     : config['image_4'].get('niter', niter),
                      "mgain"     : config['image_4'].get('mgain', mgain),
                      "channelsout"     : nchans,
                      "joinchannels"    : True,    
                      "auto-threshold": config['image_4'].get('autothreshold', auto_thresh),
                      "auto-mask"  :   config['image_4'].get('automask', auto_mask),
                  },
            input=pipeline.input,
            output=pipeline.output,
            label='{:s}:: Make image after first round of calibration'.format(step))

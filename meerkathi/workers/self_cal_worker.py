import os, shutil, glob
import sys
import yaml
import json
import os
import meerkathi
import stimela.dismissable as sdm
from meerkathi.dispatch_crew import utils
from astropy.io import fits as fits

NAME = 'Self calibration loop'

# self_cal_iter_counter is used as a global variable.


CUBICAL_OUT = {
    "CORR_DATA"    : 'sc',
    "CORR_RES"     : 'sr',
}

CUBICAL_MT = {
    "Gain2x2"      : 'complex-2x2',
    "GainDiag"      : 'complex-diag',
    "GainDiagPhase": 'phase-diag',
}


def worker(pipeline, recipe, config):
    npix = config['img_npix']
    padding = config['img_padding']
    spwid = config.get('spwid', 0)
    cell = config['img_cell']
    mgain = config['img_mgain']
    niter = config['img_niter']
    robust = config.get('img_robust',0.0)
    nchans = config.get('img_nchans',1)
    pol = config.get('img_pol', 'I')
    joinchannels = config['img_joinchannels']
    fit_spectral_pol = config.get('img_fit_spectral_pol',0)
    taper = config.get('img_uvtaper', None)
    label = config['label']
    time_chunk = config.get('cal_time_chunk', 128)
    ncpu = config.get('ncpu', 9)
    mfsprefix = ["", '-MFS'][int(nchans>1)]
    cal_niter = config.get('cal_niter', 1)
    gain_interpolation = pipeline.enable_task(config, 'gain_interpolation')
    if gain_interpolation:
        hires_label = config['gain_interpolation'].get('to_label', label)
        label = config['gain_interpolation'].get('from_label',label+'-avg')
        pipeline.set_hires_msnames(hires_label)
        hires_mslist = pipeline.hires_msnames
    pipeline.set_cal_msnames(label)
    mslist = pipeline.cal_msnames
    prefix = pipeline.prefix
    #print(hires_mslist,mslist)
    #exit()
    # Define image() extract_sources() calibrate()
    # functions for convience

    def cleanup_files(mask_name):

      if os.path.exists(pipeline.output+'/'+mask_name):
          shutil.move(pipeline.output+'/'+mask_name,pipeline.output+'/masking/'+mask_name)

      casafiles = glob.glob(pipeline.output+'/*.image')
      for i in xrange(0,len(casafiles)):
        shutil.rmtree(casafiles[i])


    def change_header(filename,headfile,copy_head):
      pblist = fits.open(filename)

      dat = pblist[0].data

      if copy_head == True:
        hdrfile = fits.open(headfile)
        head = hdrfile[0].header
      elif copy_head == False:

        head = pblist[0].header

        if 'ORIGIN' in head:
          del head['ORIGIN']
        if 'CUNIT1' in head:
          del head['CUNIT1']
        if 'CUNIT2' in head:
          del head['CUNIT2']

      fits.writeto(filename,dat,head,overwrite=True)


    def image(num):
        key = 'image'
        key_mt = 'calibrate'
        mask = False
        if num > 1:
            matrix_type = config[key_mt].get('gain_matrix_type', 'GainDiag')[num - 2 if len(config[key_mt].get('gain_matrix_type')) >= num else -1]
        else:
            matrix_type = 'null'
        # If we have a two_step selfcal and Gaindiag we want to use  CORRECTED_DATA
        if config.get('calibrate_with', 'cubical').lower() == 'meqtrees' and config[key_mt].get('two_step', False):
            if matrix_type == 'GainDiag':
                imcolumn = "CORRECTED_DATA"
            # If we do not have gaindiag but do have two step selfcal check against stupidity and that we are actually ending with ampphase cal and written to a special phase column
            elif matrix_type == 'GainDiagPhase' and config[key_mt].get('gain_matrix_type', 'GainDiag')[-1] == 'GainDiag':
                imcolumn = 'CORRECTED_DATA_PHASE'
            # If none of these apply then do our normal sefcal
            else:
                imcolumn = config[key].get('column', "CORRECTED_DATA")[num - 1 if len(config[key].get('column')) >= num else -1]
        else:
            imcolumn = config[key].get('column', "CORRECTED_DATA")[num - 1 if len(config[key].get('column')) >= num else -1]
      
        if config[key].get('peak_based_mask_on_dirty', False):
            mask = True
            step = 'image_{}_dirty'.format(num)

            recipe.add('cab/wsclean', step,
                  {
                      "msname"    : mslist,
                      "column"    : imcolumn,
                      "weight"    : 'briggs {}'.format(config.get('robust', robust)),
                      "npix"      : config[key].get('npix', npix),
                      "padding"   : config[key].get('padding', padding),
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
                  "column"    : imcolumn,
                  "weight"    : 'briggs {}'.format(config[key].get('robust', robust)),
                  "npix"      : config[key].get('npix', npix),
                  "padding"   : config[key].get('padding', padding),
                  "scale"     : config[key].get('cell', cell),
                  "prefix"    : '{0:s}_{1:d}'.format(prefix, num),
                  "niter"     : config[key].get('niter', niter),
                  "mgain"     : config[key].get('mgain', mgain),
                  "pol"       : config[key].get('pol', pol),
                  "taper-gaussian" : sdm.dismissable(config[key].get('uvtaper', taper)),
                  "channelsout"     : nchans,
                  "joinchannels"    : config[key].get('joinchannels', joinchannels),
                  "local-rms"    : config[key].get('local_rms', False),
                  "fit-spectral-pol": config[key].get('fit_spectral_pol', fit_spectral_pol),
                  "auto-threshold": config[key].get('auto_threshold',[])[num-1 if len(config[key].get('auto_threshold', [])) >= num else -1],
                  "multiscale" : config[key].get('multi_scale', False),
                  "multiscale-scales" : sdm.dismissable(config[key].get('multi_scale_scales', None)),
                  #"savesourcelist": True,
              }
        if config[key].get('mask_from_sky', False):
            fitmask = config[key].get('fits_mask', None)[num-1 if len(config[key].get('fits_mask', None)) >= num else -1]
            fitmask_address = 'masking/'+str(fitmask)
            image_opts.update( {"fitsmask" : fitmask_address+':output'})
        elif mask:
            image_opts.update( {"fitsmask" : '{0:s}_{1:d}-mask.fits:output'.format(prefix, num)} )
        else:
            image_opts.update({"auto-mask" : config[key].get('auto_mask',[])[num-1 if len(config[key].get('auto_mask', [])) >= num else -1]})

        recipe.add('cab/wsclean', step,
        image_opts,
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Make image after first round of calibration'.format(step))

    def sofia_mask(num):
        step = 'make_sofia_mask'
        key = 'sofia_mask'

        if config['img_joinchannels'] == True:
          imagename = '{0:s}_{1:d}-MFS-image.fits'.format(prefix, num)
        else:
          imagename = '{0:s}_{1:d}-image.fits'.format(prefix, num)

        if config[key].get('fornax_special',False) == True and config[key].get('use_sofia',False) == True:
          forn_kernels = [[80, 80, 0, 'b']]
          forn_thresh = config[key].get('fornax_thresh',5)[num-1 if len(config[key].get('fornax_thresh', None)) >= num else -1]
          
          image_opts_forn =  {
              "import.inFile"         : imagename,
              "steps.doFlag"          : True,
              "steps.doScaleNoise"    : False,
              "steps.doSCfind"        : True,
              "steps.doMerge"         : True,
              "steps.doReliability"   : False,
              "steps.doParameterise"  : False,
              "steps.doWriteMask"     : True,
              "steps.doMom0"          : False,
              "steps.doMom1"          : False,
              "steps.doWriteCat"      : False,
              "parameters.dilateMask" : False,
              "parameters.fitBusyFunction":False,
              "parameters.optimiseMask":False ,
              "SCfind.kernelUnit"     : 'pixel',
              "SCfind.kernels"        : forn_kernels,
              "SCfind.threshold"      : forn_thresh, 
              "SCfind.rmsMode"        : 'mad',
              "SCfind.edgeMode"       : 'constant',
              "SCfind.fluxRange"      : 'all',
              "scaleNoise.method"     : 'local',
              "scaleNoise.windowSpatial":51, 
              "scaleNoise.windowSpectral" : 1,
              "writeCat.basename"     : 'FornaxA_sofia' ,
              "merge.radiusX"         : 3, 
              "merge.radiusY"         : 3,
              "merge.radiusZ"         : 1,
              "merge.minSizeX"        : 100,
              "merge.minSizeY"        : 100, 
              "merge.minSizeZ"        : 1,
            }


        def_kernels = [[3, 3, 0, 'b'], [6, 6, 0, 'b'], [10, 10, 0, 'b']]
   
        # user_kern = config[key].get('kernels', None)
        # if user_kern:
        #   for i in xrange(0,len(user_kern))
        #     kern. 
        #     def_kernels.concatenate(config[key].get('kernels'))

        image_opts =   {
              "import.inFile"         : imagename,
              "steps.doFlag"          : True,
              "steps.doScaleNoise"    : True,
              "steps.doSCfind"        : True,
              "steps.doMerge"         : True,
              "steps.doReliability"   : False,
              "steps.doParameterise"  : True,
              "steps.doWriteMask"     : True,
              "steps.doMom0"          : False,
              "steps.doMom1"          : False,
              "steps.doWriteCat"      : True, 
              "writeCat.writeASCII"   : True,
              "writeCat.writeSQL"     : False,
              "writeCat.writeXML"     : False,
              "parameters.dilateMask" : False,
              "parameters.fitBusyFunction":False,
              "parameters.optimiseMask":False ,
              "SCfind.kernelUnit"     : 'pixel',
              "SCfind.kernels"        : def_kernels,
              "SCfind.threshold"      : config[key].get('threshold',5), 
              "SCfind.rmsMode"        : 'mad',
              "SCfind.edgeMode"       : 'constant',
              "SCfind.fluxRange"      : 'all',
              "scaleNoise.statistic"  : 'mad' ,
              "scaleNoise.method"     : 'local',
              "scaleNoise.windowSpatial"  :config[key].get('scale_noise_window',51),
              "scaleNoise.windowSpectral" : 1,
              "scaleNoise.scaleX"     : True,
              "scaleNoise.scaleY"     : True,
              "scaleNoise.scaleZ"     : False,
              "merge.radiusX"         : 3, 
              "merge.radiusY"         : 3,
              "merge.radiusZ"         : 1,
              "merge.minSizeX"        : 3,
              "merge.minSizeY"        : 3, 
              "merge.minSizeZ"        : 1,
            }
        if config[key].get('flag') :
          flags_sof = config[key].get('flagregion')
          image_opts.update({"flag.regions": flags_sof})
        
        if config[key].get('inputmask') :
          #change header of inputmask so it is the same as image
          mask_name = 'masking/'+config[key].get('inputmask')
          
          mask_name_casa = mask_name.split('.fits')[0]
          mask_name_casa = mask_name_casa+'.image'

          mask_regrid_casa = mask_name_casa+'_regrid.image'
          
          imagename_casa = '{0:s}_{1:d}{2:s}-image.image'.format(prefix, num, mfsprefix)

          recipe.add('cab/casa_importfits', step,
            {
              "fitsimage"         : imagename,
              "imagename"         : imagename_casa,
              "overwrite"         : True,
            },
            input=pipeline.output,
            output=pipeline.output,
            label='Image in casa format')

          recipe.add('cab/casa_importfits', step,
            {
              "fitsimage"         : mask_name+':output',
              "imagename"         : mask_name_casa,
              "overwrite"         : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Mask in casa format')
          
          step = '3'
          recipe.add('cab/casa_imregrid', step,
            {
              "template"      : imagename_casa+':output',
              "imagename"     : mask_name_casa+':output',
              "output"        : mask_regrid_casa,
              "overwrite"     : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Regridding mosaic to size and projection of dirty image')

          step = '4'
          recipe.add('cab/casa_exportfits', step,
            {
              "fitsimage"         : mask_name+':output',
              "imagename"         : mask_regrid_casa+':output',
              "overwrite"         : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Extracted regridded mosaic')
          
          step = '5'
          recipe.add(change_header,step,
            {
              "filename"  : pipeline.output+'/'+mask_name,
              "headfile"  : pipeline.output+'/'+imagename,
              "copy_head" : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Extracted regridded mosaic')

          image_opts.update({"import.maskFile": mask_name})
          image_opts.update({"import.inFile": imagename})
        
        if config[key].get('fornax_special',False) == True and config[key].get('use_sofia',False) == True:

          recipe.add('cab/sofia', step,
            image_opts_forn,
            input=pipeline.output,
            output=pipeline.output+'/masking/',
            label='{0:s}:: Make SoFiA mask'.format(step)) 

          fornax_namemask = 'masking/FornaxA_sofia_mask.fits'         
          image_opts.update({"import.maskFile": fornax_namemask})

        elif config[key].get('fornax_special',False) == True and config[key].get('use_sofia',False) == False:


          #this mask should be regridded to correct f.o.v.

          fornax_namemask = 'masking/Fornaxa_vla_mask_doped.fits'         
          fornax_namemask_regr = 'masking/Fornaxa_vla_mask_doped_regr.fits'         
          
          mask_name_casa = fornax_namemask.split('.fits')[0]
          mask_name_casa = fornax_namemask+'.image'

          mask_regrid_casa = fornax_namemask+'_regrid.image'
          
          imagename_casa = '{0:s}_{1:d}{2:s}-image.image'.format(prefix, num, mfsprefix)


          recipe.add('cab/casa_importfits', step,
            {
              "fitsimage"         : imagename,
              "imagename"         : imagename_casa,
              "overwrite"         : True,
            },
            input=pipeline.output,
            output=pipeline.output,
            label='Image in casa format')

          recipe.add('cab/casa_importfits', step,
            {
              "fitsimage"         : fornax_namemask+':output',
              "imagename"         : mask_name_casa,
              "overwrite"         : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Mask in casa format')
          
          step = '3'
          recipe.add('cab/casa_imregrid', step,
            {
              "template"      : imagename_casa+':output',
              "imagename"     : mask_name_casa+':output',
              "output"        : mask_regrid_casa,
              "overwrite"     : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Regridding mosaic to size and projection of dirty image')

          step = '4'
          recipe.add('cab/casa_exportfits', step,
            {
              "fitsimage"         : fornax_namemask_regr+':output',
              "imagename"         : mask_regrid_casa+':output',
              "overwrite"         : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Extracted regridded mosaic')
          
          step = '5'
          recipe.add(change_header,step,
            {
              "filename"  : pipeline.output+'/'+fornax_namemask_regr,
              "headfile"  : pipeline.output+'/'+imagename,
              "copy_head" : True,
            },
            input=pipeline.input,
            output=pipeline.output,
            label='Extracted regridded mosaic')
 
          
          image_opts.update({"import.maskFile": fornax_namemask_regr})

        recipe.add('cab/sofia', step,
          image_opts,
          input=pipeline.output,
          output=pipeline.output+'/masking/',
          label='{0:s}:: Make SoFiA mask'.format(step))

#        step = '7'
#        name_sof_out = imagename.split('.fits')[0]
#        name_sof_out = name_sof_out+'_mask.fits'

#        recipe.add(cleanup_files, step,
#          {
#           'mask_name' : name_sof_out,
#          },
#          input=pipeline.input,
#          output=pipeline.output,
#          label='{0:s}:: Cleanup SoFiA masks'.format(step))

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
        key = 'extract_sources'
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

        sourcefinder = config[key].get('sourcefinder','pybdsm')
        if (sourcefinder == 'pybdsm' or sourcefinder == 'pybdsf'):
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
				"thresh_pix"    : config[key].get('thresh_pix', [])[num-1 if len(config[key].get('thresh_pix')) >= num else -1],
				"thresh_isl"    : config[key].get('thresh_isl', [])[num-1 if len(config[key].get('thresh_isl')) >= num else -1],
				"outfile"       : '{:s}.fits:output'.format(calmodel),
				"blank_limit"   : sdm.dismissable(blank_limit),
				"adaptive_rms_box" : config[key].get('local_rms', True),
				"port2tigger"   : True,
				"multi_chan_beam": spi_do,
				"spectralindex_do": spi_do,
				"detection_image": sdm.dismissable(detection_image),
		    	},
		    	input=pipeline.input,
		    	output=pipeline.output,
		    	label='{0:s}:: Extract sources'.format(step))
        elif sourcefinder == 'sofia': 
            print 'are u crazy ?'
            print '############################################'

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
                "nchan"         : pipeline.nchans[index], #TODO: This should consider SPW IDs
                "cellsize"      : cell,
                "chanstep"      : 1,
        },
            input=pipeline.input,
            output=pipeline.output,
            label='{0:s}:: Predict from FITS ms={1:s}'.format(step, mslist[index]))


    def combine_models(models, num, enable=True):
        model_names = ['{0:s}_{1:s}-pybdsm.lsm.html:output'.format(
                       prefix, m) for m in models]
        model_names_fits = ['{0:s}/{1:s}_{2:s}-pybdsm.fits'.format(
                            pipeline.output, prefix, m) for m in models]
        calmodel = '{0:s}_{1:d}-pybdsm-combined.lsm.html:output'.format(prefix, num)

        if enable:
            step = 'combine_models_' + '_'.join(map(str, models))
            recipe.add('cab/tigger_convert', step,
                {
                    "input-skymodel"    : model_names[0],
                    "append"    : model_names[1],
                    "output-skymodel"   : calmodel,
                    "rename"  : True,
                    "force"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))

        return calmodel, model_names_fits


    def calibrate_meqtrees(num):
        key = 'calibrate'
        global reset_cal
        if num == cal_niter:
            vismodel = config[key].get('add_vis_model', False)  
        else:
            vismodel = False
        #force to calibrate with model data column if specified by user

        #If the mode is pybdsm_vis then we want to add the clean component model only at the last step,
        #which is anyway achieved by the **above** statement; no need to further specify vismodel.

        if config[key].get('model_mode', None) == 'pybdsm_vis':
            model = config[key].get('model', num)[num-1]

            modelcolumn = 'MODEL_DATA'
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num,
                                           enable=False if pipeline.enable_task(
                                           config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
                fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)
        #If the mode is pybdsm_only, don't use any clean components. So, the same as above, but with
        #vismodel =False
        elif config[key].get('model_mode', None) == 'pybdsm_only':
            vismodel = False
            model = config[key].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+')) > 1:
                mm = model.split('+')
                calmodel, fits_model = combine_models(mm, num,
                                           enable=False if pipeline.enable_task(
                                           config, 'aimfast') else True)
            else:
                model = int(model)
                calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
                fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)

            modelcolumn = ''
        #If the mode is vis_only, then there is need for an empty sky model (since meqtrees needs one).
        #In this case, vis_model is always true, the model_column is always MODEL_DATA.
        elif  config[key].get('model_mode', None) == 'vis_only':
            vismodel = True
            modelcolumn = 'MODEL_DATA'
            calmodel = '{0:s}_{1:d}-nullmodel.txt'.format(prefix, num)
            with open(os.path.join(pipeline.input, calmodel), 'w') as stdw:
                stdw.write('#format: ra_d dec_d i\n')
                stdw.write('0.0 -30.0 1e-99')
        # Let's see the matrix type we are dealing with
        if not config[key].get('two_step', False):
            matrix_type = config[key].get('gain_matrix_type', 'GainDiag')[num - 1 if len(config[key].get('gain_matrix_type')) >= num else -1]
        # If we have a two_step selfcal and Gaindiag we want to use CORRECTED_DATA_PHASE as input and write to CORRECTED_DATA

        outcolumn = "CORRECTED_DATA"
        incolumn = "DATA"
                        
        for i,msname in enumerate(mslist):
             # Let's see the matrix type we are dealing with


            gsols_ = [config[key].get('Gsols_time',[])[num-1 if num <= len(config[key].get('Gsols_time',[])) else -1],
                          config[key].get('Gsols_channel', [])[num-1 if num <= len(config[key].get('Gsols_channel',[])) else -1]]
             # If we have a two_step selfcal  we will calculate the intervals
            matrix_type = config[key].get('gain_matrix_type', 'GainDiag')[num - 1 if len(config[key].get('gain_matrix_type')) >= num else -1]
            if config[key].get('two_step', False) and config[key].get('aimfast', False) :
                if num == 1:
                    matrix_type = 'GainDiagPhase'
                    SN = 3
                else:
                    matrix_type= trace_matrix[num-2]
                    SN = trace_SN[num-2]
                fidelity_data = get_aimfast_data()
                obs_data = get_obs_data()
                int_time =  obs_data['EXPOSURE']
                tot_time=0.
                for scan_key in obs_data['SCAN']['0']:
                    tot_time += obs_data['SCAN']['0'][scan_key]
                no_ant=len(obs_data['ANT']['DISH_DIAMETER'])
                DR=fidelity_data['meerkathi_{0}-residual'.format(num)]['meerkathi_{0}-model'.format(num)]['DR']
                Noise= fidelity_data['meerkathi_{0}-residual'.format(num)]['STDDev']
                flux=DR*Noise
                solvetime = int(Noise**2*SN**2*tot_time*no_ant/(flux**2*2.)/int_time)

                if num> 1:
                    DR=fidelity_data['meerkathi_{0}-residual'.format(num-1)]['meerkathi_{0}-model'.format(num-1)]['DR']
                    flux=DR*Noise
                    prev_solvetime = int(Noise**2*SN**2*tot_time*no_ant/(flux**2*2.)/int_time)
                else:
                    prev_solvetime= solvetime+1

                if (solvetime >= prev_solvetime or reset_cal==1) and matrix_type == 'GainDiagPhase':
                    matrix_type= 'GainDiag'
                    SN = 8
                    solvetime = int(Noise**2*SN**2*tot_time*no_ant/(flux**2*2.)/int_time)
                    gsols_[0]= int(solvetime/num)
                elif solvetime >= prev_solvetime and matrix_type == 'GainDiag':
                    gsols_[0]= int(prev_solvetime/num)
                    reset_cal = 2
                else:
                    gsols_[0]= int(solvetime/num)
                if matrix_type == 'GainDiagPhase':
                    minsolvetime = int(30./int_time)
                else:
                    minsolvetime = int(30.*60./int_time)
                if minsolvetime > gsols_[0]:
                    gsols_[0] = minsolvetime
                    if matrix_type == 'GainDiag':
                        reset_cal = 2
                global trace_SN
                trace_SN.append(SN)
                global trace_matrix
                trace_matrix.append(matrix_type)
                if matrix_type == 'GainDiagPhase' and config[key].get('two_step', False):
                    outcolumn = "CORRECTED_DATA_PHASE"
                    incolumn = "DATA"
                elif config[key].get('two_step', False):
                    outcolumn = "CORRECTED_DATA"
                    incolumn = "CORRECTED_DATA_PHASE"
            elif config[key].get('two_step', False):
                if matrix_type == 'GainDiagPhase':
                    outcolumn = "CORRECTED_DATA_PHASE"
                    incolumn = "DATA"
                else:
                    outcolumn = "CORRECTED_DATA"
                    incolumn = "CORRECTED_DATA_PHASE"

            bsols_ = [config[key].get('Bsols_time',[0])[num-1 if num <= len(config[key].get('Bsols_time',[])) else -1],
                          config[key].get('Bsols_channel', [0])[num-1 if num <= len(config[key].get('Bsols_channel',[])) else -1]]
            step = 'calibrate_{0:d}_{1:d}'.format(num, i)
            recipe.add('cab/calibrator', step,
               {
                 "skymodel"             : calmodel,  
                 "add-vis-model"        : vismodel,
                 "model-column"         : modelcolumn,
                 "msname"               : msname,
                 "threads"              : ncpu,
                 "column"               : incolumn,
                 "output-data"          : config[key].get('output_data', 'CORR_DATA')[num-1 if len(config[key].get('output_data')) >= num else -1],
                 "output-column"        : outcolumn,
                 "prefix"               : '{0:s}-{1:d}_meqtrees'.format(pipeline.dataid[i], num),
                 "label"                : 'cal{0:d}'.format(num),
                 "read-flags-from-ms"   : True,
                 "read-flagsets"        : "-stefcal",
                 "write-flagset"        : "stefcal",
                 "write-flagset-policy" : "replace",
                 "Gjones"               : True,
                 "Gjones-solution-intervals" : sdm.dismissable(gsols_ or None),
                 "Gjones-matrix-type"   : matrix_type,
                 "Gjones-ampl-clipping"      : True,
                 "Gjones-ampl-clipping-low"  : config.get('cal_gain_amplitude_clip_low', 0.5),
                 "Gjones-ampl-clipping-high" : config.get('cal_gain_amplitude_clip_high', 1.5),
                 "Bjones"                    : config[key].get('Bjones', False),
                 "Bjones-solution-intervals" : sdm.dismissable(bsols_ or None),
                 "Bjones-ampl-clipping"      : config[key].get('Bjones', False),
                 "Bjones-ampl-clipping-low"  : config.get('cal_gain_amplitude_clip_low', 0.5),
                 "Bjones-ampl-clipping-high" : config.get('cal_gain_amplitude_clip_high', 1.5),
                 "make-plots"           : True,
                 "tile-size"            : time_chunk,
               },
               input=pipeline.input,
               output=pipeline.output,
               label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    def calibrate_cubical(num):
        key = 'calibrate'

        modellist = []
        model = config[key].get('model', num)[num-1]
        if isinstance(model, str) and len(model.split('+'))>1:
            mm = model.split('+')
            calmodel, fits_model = combine_models(mm, num)
        else:
            model = int(model)
            calmodel = '{0:s}_{1:d}-pybdsm.lsm.html:output'.format(prefix, model)
            fits_model = '{0:s}/{1:s}_{2:d}-pybdsm.fits'.format(pipeline.output, prefix, model)

        if config[key].get('model_mode', None) == 'pybdsm_vis':
            if (num == cal_niter):
                modellist = [calmodel, 'MODEL_DATA']
            else:
                modellist = [calmodel]
            # This is incorrect and will result in the lsm being used in the first direction and the model_data in the others. They need to be added as + however that messes up the output identifier structure
        if config[key].get('model_mode', None) == 'pybdsm_only':
            modellist = [calmodel]
        if config[key].get('model_mode', None) == 'vis_only':
            modellist = ['MODEL_DATA']
        matrix_type = config[key].get('gain_matrix_type','Gain2x2')[num-1 if len(config[key].get('gain_matrix_type')) >= num else -1]
        if matrix_type == 'Gain2x2':
            take_diag_terms = False
        else:
            take_diag_terms = True
        if config[key].get('DDjones', False) or config[key].get('two_step', False) or config[key].get('Bjones', False):
            if matrix_type == 'GainDiagPhase':
                gupdate = 'phase-diag'
                bupdate = 'phase-diag'
                dupdate = 'phase-diag'
            elif matrix_type == 'GainDiagAmp':
                gupdate = 'amp-diag'
                bupdate = 'amp-diag'
                dupdate = 'amp-diag'
            elif matrix_type == 'GainDiag':
                gupdate = 'diag'
                bupdate = 'diag'
                dupdate = 'diag'
            elif matrix_type == 'Gain2x2':
                gupdate = 'full'
                bupdate = 'full'
                dupdate = 'full'
            else:
                raise ValueError('{} is not a viable matrix_type'.format(matrix_type) )
            if config[key].get('two_step', False):
                gupdate= 'phase-diag'

        jones_chain = 'G'
        gsols_ = [config[key].get('Gsols_time', [])[num - 1 if num <= len(config[key].get('Gsols_time', [])) else -1],
                  config[key].get('Gsols_channel', [])[
                      num - 1 if num <= len(config[key].get('Gsols_channel', [])) else -1]]
        bsols_ = [config[key].get('Bsols_time', [0])[num - 1 if num <= len(config[key].get('Bsols_time', [])) else -1],
                  config[key].get('Bsols_channel', [0])[
                      num - 1 if num <= len(config[key].get('Bsols_channel', [])) else -1]]
        ddsols_ = [
            config[key].get('DDsols_time', [0])[num - 1 if num <= len(config[key].get('DDsols_time', [])) else -1],
            config[key].get('DDsols_channel', [0])[
                num - 1 if num <= len(config[key].get('DDsols_channel', [])) else -1]]

        if (config[key].get('two_step', False) and ddsols_[0] != -1) or config[key].get('ddjones', False) :
            jones_chain += ',DD'
            matrix_type = 'Gain2x2'
        elif config[key].get('DDjones', False) and config[key].get('two_step', False):
             raise ValueError('You cannot do a DD-gain calibration and a split amplitude-phase calibration all at once')
        if config[key].get('Bjones', False):
            jones_chain += ',B'
            matrix_type = 'Gain2x2'

        for i,msname in enumerate(mslist):

            step = 'calibrate_cubical_{0:d}_{1:d}'.format(num, i)
            cubical_opts= {
                  "data-ms"          : msname,
                  "data-column"      : 'DATA',
                  "sol-term-iters"   : '50',
                  "model-list"       : modellist,
                  "data-time-chunk"  : time_chunk,
                  "sel-ddid"         : sdm.dismissable(config[key].get('spwid', None)),
                  "dist-ncpu"        : ncpu,
                  "sol-jones"        : '"'+jones_chain+'"',
                  "sol-diag-diag"    : take_diag_terms,
                  "out-name"         : '{0:s}-{1:d}_cubical'.format(pipeline.dataid[i], num),
                  "out-mode"         : CUBICAL_OUT[config[key].get('output_data', 'CORR_DATA')[num-1 if len(config[key].get('output_data')) >= num else -1]],
                  "out-plots"        : True,
                  "weight-column"    : config[key].get('weight_column', 'WEIGHT'),
                  "montblanc-dtype"  : 'float',
                  "g-solvable"      : True,
                  "g-type"          : CUBICAL_MT[matrix_type],
                  "g-time-int"      : gsols_[0],
                  "g-freq-int"      : gsols_[1],
                  "g-save-to"       : "g-gains-{0:d}-{1:s}.parmdb:output".format(num,msname.split('.ms')[0]),
                  "g-clip-low"      : config.get('cal_gain_amplitude_clip_low', 0.5),
                  "g-clip-high"     : config.get('cal_gain_amplitude_clip_high', 1.5),
                  "madmax-enable"   : config[key].get('madmax_flagging',True),
                  "madmax-plot"     : True if (config[key].get('madmax_flagging')) else False,
                  "madmax-threshold" : config[key].get('madmax_flag_thresh', [0,10]),
                  "madmax-estimate" : 'corr',

                }

            if config[key].get('two_step', False) and ddsols_[0] != -1:
                cubical_opts.update({
                    "g-update-type"    : gupdate,
                    "dd-update-type"   : 'amp-diag',
                    "dd-solvable"      : True,
                    "dd-type"          : CUBICAL_MT[matrix_type],
                    "dd-time-int"      : ddsols_[0],
                    "dd-freq-int"      : ddsols_[1],
                    "dd-save-to"       : "g-amp-gains-{0:d}-{1:s}.parmdb:output".format(num,msname.split('.ms')[0]),
                    "dd-clip-low"      : config.get('cal_gain_amplitude_clip_low', 0.5),
                    "dd-clip-high"     : config.get('cal_gain_amplitude_clip_high', 1.5),
                })
            if config[key].get('Bjones', False):
               cubical_opts.update({
                                    "g-update-type"   : gupdate,
                                    "b-update-type"   : bupdate,
                                    "b-solvable": True,
                                    "b-time-int": bsols_[0],
                                    "b-freq-int": bsols_[1],
                                    "b-type" : CUBICAL_MT[matrix_type],
                                    "b-clip-low"      : config.get('cal_gain_amplitude_clip_low', 0.5),
                                    "b-save-to": "b-gains-{0:d}-{1:s}.parmdb:output".format(num,msname.split('.ms')[0]),
                                    "b-clip-high"     : config.get('cal_gain_amplitude_clip_high', 1.5)})
                                            
            if config[key].get('DDjones', False):
               cubical_opts.update({"g-update-type"   : gupdate,
                                    "dd-update-type"  : dupdate,
                                    "dd-solvable": True,
                                    "dd-time-int": ddsols_[0],
                                    "dd-freq-int": ddsols_[1],
                                    "dd-type" : CUBICAL_MT[matrix_type],
                                    "dd-clip-low"      : config.get('cal_gain_amplitude_clip_low', 0.5),
                                    "dd-clip-high"     : config.get('cal_gain_amplitude_clip_high', 1.5),
                                    "dd-dd-term":   True,
                                    "dd-save-to": "dE-gains-{0:d}-{1:s}.parmdb:output".format(num,msname.split('.ms')[0]),})
            recipe.add('cab/cubical', step, cubical_opts,  
                input=pipeline.input,
                output=pipeline.output,
                shared_memory= config[key].get('shared_memory','100Gb'),
                #shared_memory = '10Gb',
                label="{0:s}:: Calibrate step {1:d} ms={2:s}".format(step, num, msname))

    
    def apply_gains_to_fullres(apply_iter, enable=True):

        key = 'calibrate'


        calwith = config.get('calibrate_with', 'meqtrees').lower()
        if(calwith=='meqtrees'):
           meerkathi.log.info('Gains cannot be interpolated with MeqTrees, please switch to CubiCal')
           raise ValueError("Gains cannot be interpolated with MeqTrees, please switch to CubiCal")

        if config[key].get('Bjones',False):
            jones_chain = 'G,B'
        else:
            jones_chain = 'G'
        if config[key].get('DDjones', False) or (config[key].get('two_step',False) and config[key].get('DDsols_time', [0])[apply_iter - 1 if apply_iter <= len(config[key].get('DDsols_time', [])) else -1] != -1):
            jones_chain+= ',DD'
        for i,himsname in enumerate(hires_mslist):
            cubical_gain_interp_opts = {
               "data-ms"          : himsname,
               "data-column"      : 'DATA',
               "data-time-chunk"  : time_chunk,
               "sol-jones"        : jones_chain,
               "sel-ddid"         : sdm.dismissable(config[key].get('spwid', None)),
               "dist-ncpu"        : ncpu,
               "out-name"         : '{0:s}-{1:d}_cubical'.format(pipeline.dataid[i], apply_iter),
               "out-mode"         : 'ac',
               "weight-column"    : config[key].get('weight_column', 'WEIGHT'),
               "montblanc-dtype"  : 'float',
               "g-solvable"       : False,
               "g-save-to"        : None,
               "g-xfer-from"     : "g-gains-{0:d}-{1:s}.parmdb:output".format(apply_iter,(himsname.split('.ms')[0]).replace(hires_label,label))}
            if config[key].get('DDjones', False):
               cubical_gain_interp_opts.update(
                   {"dd-xfer-from": "dE-gains-{0:d}-{1:s}.parmdb:output".format(apply_iter,(himsname.split('.ms')[0]).replace(hires_label,label)),
                    "dd-solvable" : False,
                    "dd-save-to"  : None
                    })
            if config[key].get('Bjones', False):
               cubical_gain_interp_opts.update(
                   {"b-xfer-from": "b-gains-{0:d}-{1:s}.parmdb:output".format(apply_iter,(himsname.split('.ms')[0]).replace(hires_label,label)),
                    "b-solvable" : False,
                    "b-save-to"  : None
                    })
            if config[key].get('two_step',False) and config[key].get('DDsols_time', [0])[apply_iter - 1 if apply_iter <= len(config[key].get('DDsols_time', [])) else -1] != -1:
               cubical_gain_interp_opts.update(
                   {"dd-xfer-from": "g-amp-gains-{0:d}-{1:s}.parmdb:output".format(apply_iter,(himsname.split('.ms')[0]).replace(hires_label,label)),
                    "dd-solvable" : False,
                    "dd-save-to"  : None
                    })
            step = 'apply_cubical_gains_{0:d}_{1:d}'.format(apply_iter, i)
            recipe.add('cab/cubical', step, cubical_gain_interp_opts,
                input=pipeline.input,
                output=pipeline.output,
                shared_memory=config[key].get('shared_memory','100Gb'),
                label="{0:s}:: Apply cubical gains ms={1:s}".format(step, himsname))


     


    def get_aimfast_data(filename='{0:s}/fidelity_results.json'.format(pipeline.output)):
        "Extracts data from the json data file"
        with open(filename) as f:
            data = json.load(f)
        return data
    def get_obs_data(filename='{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, pipeline.prefixes[0])):
        "Extracts data from the json data file"
        if label:
            filename='{0:s}/{1:s}-{2:s}-obsinfo.json'.format(pipeline.output, pipeline.prefixes[0],label)
        with open(filename) as f:
            data = json.load(f)
        return data
    def quality_check(n, enable=True):
        "Examine the aimfast results to see if they meet specified conditions"
        # If total number of iterations is reached stop
        global reset_cal
        if enable:
            # The recipe has to be executed at this point to get the image fidelity results
            
            recipe.run()
            # Empty job que after execution
            recipe.jobs = []
            if reset_cal >= 2:
                return False
            key = 'aimfast'
            tolerance = config[key].get('tolerance', 0.02)
            fidelity_data = get_aimfast_data()
            # Ensure atleast one iteration is ran to compare previous and subsequent images
            if n>= 2:
                conv_crit = config[key].get('convergence_criteria', ["DR", "SKEW", "KURT", "STDDEV", "MEAN"])
                conv_crit= [cc.upper() for cc in conv_crit]
                # Ensure atleast one iteration is ran to compare previous and subsequent images
                residual0=fidelity_data['meerkathi_{0}-residual'.format(n - 1)]
                residual1 = fidelity_data['meerkathi_{0}-residual'.format(n)]
                # Unlike the other ratios DR should grow hence n-1/n < 1.

                if not pipeline.enable_task(config, 'extract_sources'):
                    drratio=fidelity_data['meerkathi_{0}-restored'.format(n - 1)]['DR']/fidelity_data[
                                          'meerkathi_{0}-restored'.format(n)]['DR']
                else:
                    drratio=residual0['meerkathi_{0}-model'.format(n - 1)]['DR']/residual1['meerkathi_{0}-model'.format(n)]['DR']

                # Dynamic range is important,
                if any(cc == "DR" for cc in conv_crit):
                    drweight = 0.8
                else:
                    drweight = 0.
                # The other parameters should become smaller, hence n/n-1 < 1
                skewratio=residual1['SKEW']/residual0['SKEW']
                # We care about the skewness when it is large. What is large?
                # Let's go with 0.005 at that point it's weight is 0.5
                if any(cc == "SKEW" for cc in conv_crit):
                    skewweight=residual1['SKEW']/0.01
                else:
                    skewweight = 0.
                kurtratio=residual1['KURT']/residual0['KURT']
                # Kurtosis goes to 3 so this way it counts for 0.5 when normal distribution
                if any(cc == "KURT" for cc in conv_crit):
                    kurtweight=residual1['KURT']/6.
                else:
                    kurtweight = 0.
                meanratio=residual1['MEAN']/residual0['MEAN']
                # We only care about the mean when it is large compared to the noise
                # When it deviates from zero more than 20% of the noise this is a problem
                if any(cc == "MEAN" for cc in conv_crit):
                    meanweight=residual1['MEAN']/(residual1['STDDev']*0.2)
                else:
                    meanweight = 0.
                noiseratio=residual1['STDDev']/residual0['STDDev']
                # The noise should not change if the residuals are gaussian in n-1.
                # However, they should decline in case the residuals are non-gaussian.
                # We want a weight that goes to 0 in both cases
                if any(cc == "STDDEV" for cc in conv_crit):
                    if residual0['KURT']/6. < 0.52 and residual0['SKEW'] < 0.01:
                        noiseweight=abs(1.-noiseratio)
                    else:
                        # If declining then noiseratio is small and that's good, If rising it is a real bad thing.
                        #  Hence we can just square the ratio
                        noiseweight=noiseratio
                else:
                    noiseweight = 0.
                # A huge increase in DR can increase the skew and kurtosis significantly which can mess up the calculations
                if drratio < 0.6:
                    skewweight=0.
                    kurtweight=0.

                # These weights could be integrated with the ratios however while testing I
                #  kept them separately such that the idea behind them is easy to interpret.
                # This  combines to total weigth of 1.2+0.+0.5+0.+0. so our total should be LT 1.7*(1-tolerance)
                # it needs to be slightly lower to avoid keeping fitting without improvement
                # Ok that is the wrong philosophy. Their weighted mean should be less than 1-tolerance that means improvement.
                # And the weights control how important each parameter is.
                HolisticCheck=(drratio*drweight+skewratio*skewweight+kurtratio*kurtweight+meanratio*meanweight+noiseratio*noiseweight) \
                              /(drweight+skewweight+kurtweight+meanweight+noiseweight)
                if (1 - tolerance) < HolisticCheck:
                    meerkathi.log.info('Stopping criterion: '+' '.join([cc for cc in conv_crit]))
                    meerkathi.log.info('The calculated ratios DR={:f}, Skew={:f}, Kurt={:f}, Mean={:f}, Noise={:f} '.format(
                        drratio,skewratio,kurtratio,meanratio,noiseratio))
                    meerkathi.log.info('The weights used DR={:f}, Skew={:f}, Kurt={:f}, Mean={:f}, Noise={:f} '.format(
                        drweight,skewweight,kurtweight,meanweight,noiseweight))
                    meerkathi.log.info('{:f} < {:f}'.format(1-tolerance, HolisticCheck))
                #   If we stop we want change the final output model to the previous iteration
                    global self_cal_iter_counter
                    reset_cal += 1
                    if reset_cal ==1:
                        self_cal_iter_counter -= 1
                    else:
                        self_cal_iter_counter -= 2

                    if self_cal_iter_counter < 1:
                        self_cal_iter_counter = 1


                    return True
        # If we reach the number of iterations we want to stop.
        if n == cal_niter + 1:
            meerkathi.log.info('Number of iterations reached: {:d}'.format(cal_niter))
            return False
        # If no condition is met return true to continue
        return True

    def image_quality_assessment(num):
        # Check if more than two calibration iterations to combine successive models
        # Combine models <num-1> (or combined) to <num> creat <num+1>-pybdsm-combine
        # This was based on thres_pix but change to model as when extract_sources = True is will take the last settings
        if len(config['calibrate'].get('model', [])) >= num:
            model = config['calibrate'].get('model', num)[num-1]
            if isinstance(model, str) and len(model.split('+'))==2:
                mm = model.split('+')
                combine_models(mm, num)
        # in case we are in the last round, imaging has made a model that is longer then the expected model column
        # Therefore we take this last model if model is not defined
        if num == cal_niter+1:
            try:
                model.split()
            except NameError:
                model = str(num)

        step = 'aimfast'

        aimfast_settings = {
                    "residual-image"       : '{0:s}_{1:d}{2:s}-residual.fits:output'.format(
                                                 prefix, num, mfsprefix),
                    "normality-test"       : config[step].get(
                                                 'normality_model', 'normaltest'),
                    "area-factor"          : config[step].get('area_factor', 10),
                    "label"                : "meerkathi_{}".format(num),
                }

        # if we run pybdsm we want to use the  model as well. Otherwise we want to use the image.

        if pipeline.enable_task(config, 'extract_sources'):
            aimfast_settings.update({"tigger-model"   : '{0:s}_{1:d}-pybdsm{2:s}.lsm.html:output'.format(
                prefix, num if num <= len(config['calibrate'].get('model', num))
                else len(config['calibrate'].get('model', num)),
                '-combined' if len(model.split('+')) >= 2 else '')})
        else:
            # Use the image
            if config['calibrate'].get('output_data')[num-1 if num <= len(config['calibrate'].get('output_data',[])) else -1] == "CORR_DATA":
                aimfast_settings.update({"restored-image" : '{0:s}_{1:d}{2:s}-image.fits:output'.format(
                                                                prefix, num, mfsprefix)})
            else:
                try:
                    im = config['calibrate'].get('output_data').index("CORR_RES") + 1
                except ValueError:
                    im = num
                aimfast_settings.update({"restored-image" : '{0:s}_{1:d}{2:s}-image.fits:output'.format(
                                                                prefix, im, mfsprefix)})
        recipe.add('cab/aimfast', step,
            aimfast_settings,
            input=pipeline.output,
            output=pipeline.output,
            label="{0:s}_{1:d}:: Image fidelity assessment for {2:d}".format(step, num, num))

    def aimfast_plotting():
        """Plot comparisons of catalogs and residuals"""

        out_dir = pipeline.output
        # Get residuals to compare
        res_files = sorted(glob.glob("{:s}/{:s}_?-MFS-residual.fits".format(out_dir, prefix)))
        residuals = []
        for i, r in enumerate(res_files):
            if i < len(res_files) - 1:
                residuals.append("{:s}:{:s}:output".format(r.split('/')[-1], res_files[i+1].split('/')[-1]))

        # Get models to compare
        model_files = sorted(glob.glob("{:s}/{:s}_*.lsm.html".format(out_dir, prefix)))
        models = []
        for i, m in enumerate(model_files):
            if i < len(model_files) - 1:
                models.append("{:s}:{:s}:output".format(m.split('/')[-1], model_files[i+1].split('/')[-1]))

        if len(model_files) > 1:
            step = "aimfast_comparing_models"

            recipe.add('cab/aimfast', step,
                {
                     "compare-models"     : models,
                     "area-factor"        : config['aimfast'].get('area_factor', 2)
                },
                input=pipeline.input,
                output=pipeline.output,
                label="Plotting model comparisons")

        if len(res_files) > 1:
            step = "aimfast_comparing_random_residuals"

            recipe.add('cab/aimfast', step,
                {
                     "compare-residuals"  : residuals,
                     "area-factor"        : config['aimfast'].get('area_factor', 2),
                     "data-points"        : 100
                },
                input=pipeline.input,
                output=pipeline.output,
                label="Plotting random residuals comparisons")

        if len(res_files) > 1 and len(model_files) > 1:
            step = "aimfast_comparing_source_residuals"

            recipe.add('cab/aimfast', step,
                {
                     "compare-residuals"  : residuals,
                     "area-factor"        : config['aimfast'].get('area_factor', 2),
                     "tigger-model"       : '{:s}:output'.format(model_files[-1].split('/')[-1])
                },
                input=pipeline.input,
                output=pipeline.output,
                label="Plotting source residuals comparisons")
    def create_averaged():
        for i,msname in enumerate(hires_mslist):
            print(msname,mslist[i])
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, mslist[i])):
                raise ValueError("Your low resolution file already exists. We will not overwrite.")
            else:
                step = 'average_target_{:d}'.format(i)
                recipe.add('cab/casa_split', step,
                {
                    "vis"           : msname,
                    "outputvis"     : mslist[i],
                    "timebin"       : config['create_averaged'].get('time_average', ''),
                    "width"         : config['create_averaged'].get('freq_average', 5),
                    "spw"           : config['create_averaged'].get('spw', ''),
                    "datacolumn"    : config['create_averaged'].get('column', 'data'),
                    "correlation"   : config['create_averaged'].get('correlation', ''),
                    "field"         : '0',
                    "keepflags"     : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Split and average data ms={1:s}'.format(step, msname))
                # run recipe
        recipe.run()
        # Empty job que after execution
        recipe.jobs = []

    # Optionally undo the subtraction of the MODEL_DATA column that may have been done by the image_HI worker
    if config.get('undo_subtractmodelcol', False):
        for i,msname in enumerate(mslist):
            step = 'undo_modelsub_{:d}'.format(i)
            recipe.add('cab/msutils', step,
                {
                    "command"  : 'sumcols',
                    "msname"   : msname,
                    "col1"     : 'CORRECTED_DATA',
                    "col2"     : 'MODEL_DATA',
                    "column"   : 'CORRECTED_DATA'
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Add model column to corrected column'.format(step))

    # decide which tool to use for calibration
    calwith = config.get('calibrate_with', 'meqtrees').lower()
    if calwith == 'meqtrees':
        calibrate = calibrate_meqtrees
    elif calwith == 'cubical':
        calibrate = calibrate_cubical

    # if we use the new two_step analysis aimfast has to be run
    #if config['calibrate'].get('two_step') and calwith == 'meqtrees':
    #    config['aimfast']['enable'] = True
    # if model_mode is vis only we do not want to run pybdsm
    # this is outdated: we want to run aimfast also having if using vis_only

    #if config['calibrate'].get('model_mode') == 'vis_only':
    #    config['extract_sources']['enable'] = False

    # if we do not run pybdsm we always need to output the corrected data column
    if not pipeline.enable_task(config, 'extract_sources'):
        config['calibrate']['output_data'] = [k.replace('CORR_RES','CORR_DATA') for k in config['calibrate'].get('output_data')]
    global self_cal_iter_counter
    self_cal_iter_counter = config.get('start_at_iter', 1)
    global reset_cal
    reset_cal = 0
    global trace_SN
    trace_SN = []
    global trace_matrix
    trace_matrix = []
    # if we want to run the hires modules and run selfcal on an averaged input set but forgot to create it at split or wanted to flag first we first want to copy the input ms to the correct hires set and then average the low res set
    if pipeline.enable_task(config, 'create_averaged'):
        if pipeline.enable_task(config, 'gain_interpolation'):
            create_averaged()
        else:
            raise ValueError("Why create an averaged set but then not apply the calibrations to the full data set?")



    if pipeline.enable_task(config, 'image'):
        if pipeline.enable_task(config, 'gain_interpolation'):
            meerkathi.log.info("Interpolating gains")
        image(self_cal_iter_counter)
    if pipeline.enable_task(config, 'sofia_mask'):
        sofia_mask(self_cal_iter_counter)
    if pipeline.enable_task(config, 'extract_sources'):
        extract_sources(self_cal_iter_counter)
    if pipeline.enable_task(config, 'aimfast'):
        image_quality_assessment(self_cal_iter_counter)

    while quality_check(self_cal_iter_counter,
                        enable=pipeline.enable_task(
                            config, 'aimfast')):
        if pipeline.enable_task(config, 'calibrate'):
            calibrate(self_cal_iter_counter)
        if reset_cal < 2:
            self_cal_iter_counter += 1
            if pipeline.enable_task(config, 'image'):
                image(self_cal_iter_counter)
            if pipeline.enable_task(config, 'sofia_mask'):
                sofia_mask(self_cal_iter_counter)
            if pipeline.enable_task(config, 'extract_sources'):
                extract_sources(self_cal_iter_counter)
            if pipeline.enable_task(config, 'aimfast'):
                image_quality_assessment(self_cal_iter_counter)

    if pipeline.enable_task(config, 'gain_interpolation'):
        if (self_cal_iter_counter > cal_niter):
            apply_gains_to_fullres(self_cal_iter_counter-1, enable=True)
        else:
            apply_gains_to_fullres(self_cal_iter_counter, enable=True)
    if config['aimfast'].get('plot',False):
        aimfast_plotting()

    #DO NOT ERASE THIS LOOP IT IS NEEDED FOR PIPELINE OUTSIDE DATA QUALITY CHECK!!!!!!!!!!!!!!!!!!!!!
    #else:
    #   for kk in xrange(config.get('start_at_iter', 1), config.get('cal_niter', 2)+1):
    #        if pipeline.enable_task(config, 'calibrate'):
    #            calibrate(kk)
    #        if pipeline.enable_task(config, 'image'):
    #            image(kk+1)
    #        if pipeline.enable_task(config, 'sofia_mask'):
    #            sofia_mask(kk+1)

    if config['calibrate'].get('hires_interpol')==True:
        print "Interpolating gains"
        substep = int(config.get('apply_step', cal_niter))
        apply_gains_to_fullres(substep,enable=True if (config['calibrate'].get('hires_interpol')==True) else False)

    if pipeline.enable_task(config, 'restore_model'):
        if config['restore_model']['model']:
            num = config['restore_model']['model']
            if isinstance(num, str) and len(num.split('+')) == 2:
                mm = num.split('+')
                if int(mm[-1]) > self_cal_iter_counter:
                    num = str(self_cal_iter_counter)
        else:
            extract_sources = len(config['extract_sources'].get(
                                  'thresh_isl', [self_cal_iter_counter]))
            if extract_sources > 1:
                num = '{:d}+{:d}'.format(self_cal_iter_counter-1, self_cal_iter_counter)
            else:
                num = self_cal_iter_counter

        if isinstance(num, str) and len(num.split('+')) == 2:
            mm = num.split('+')

            models = ['{0:s}_{1:s}-pybdsm.lsm.html:output'.format(
                      prefix, m) for m in mm]
            final = '{0:s}_final-pybdsm.lsm.html:output'.format(prefix)

            step = 'create_final_lsm_{0:s}_{1:s}'.format(*mm)
            recipe.add('cab/tigger_convert', step,
                {
                    "input-skymodel"    : models[0],
                    "append"            : models[1],
                    "output-skymodel"   : final,
                    "rename"            : True,
                    "force"             : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))

        elif isinstance(num, str) and num.isdigit():
            inputlsm = '{0:s}_{1:s}-pybdsm.lsm.html:output'.format(prefix, num)
            final = '{0:s}_final-pybdsm.lsm.html:output'.format(prefix)
            step = 'create_final_lsm_{0:s}'.format(num)
            recipe.add('cab/tigger_convert', step,
                {
                    "input-skymodel"    : inputlsm,
                    "output-skymodel"   : final,
                    "rename"  : True,
                    "force"   : True,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combined models'.format(step))
        else:
            raise ValueError("restore_model_model should be integer-valued string or indicate which models to be appended, eg. 2+3")

        if config['restore_model'].get('clean_model', None):
            num = int(config['restore_model'].get('clean_model', None))
            if num > self_cal_iter_counter:
                num = self_cal_iter_counter

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
                    "image"    : [prefix+'_{0:d}{1:s}-image.fits:output'.format(num, mfsprefix), conv_model],
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

    if pipeline.enable_task(config, 'highfreqres_contim'):
        # Upate pipeline attributes (useful if, e.g., channel averaging was performed by the split_data worker)
        for i, prfx in enumerate(['meerkathi-{0:s}-{1:s}'.format(did,config['label']) for did in pipeline.dataid]):
            msinfo = '{0:s}/{1:s}-obsinfo.json'.format(pipeline.output, prfx)
            with open(msinfo, 'r') as stdr: pipeline.nchans[i] = yaml.load(stdr)['SPW']['NUM_CHAN']
        step = 'highfreqres_contim'
        image_opts = {
                  "msname"                 : hires_mslist if pipeline.enable_task(config, 'gain_interpolation') else mslist,
                  "column"                 : config['highfreqres_contim'].get('column', "CORRECTED_DATA"),
                  "weight"                 : 'briggs {}'.format(config['highfreqres_contim'].get('robust', robust)),
                  "npix"                   : config['highfreqres_contim'].get('npix', npix),
                  "padding"                : config['highfreqres_contim'].get('padding', padding),
                  "scale"                  : config['highfreqres_contim'].get('cell', cell),
                  "prefix"                 : '{0:s}_{1:s}'.format(prefix, 'fine'),
                  "niter"                  : config['highfreqres_contim'].get('niter', niter),
                  "mgain"                  : config['highfreqres_contim'].get('mgain', mgain),
                  "pol"                    : config['highfreqres_contim'].get('pol', pol),
                  "taper-gaussian"         : sdm.dismissable(config['highfreqres_contim'].get('uvtaper', taper)),
                  "deconvolution-channels" : config['highfreqres_contim'].get('deconv_chans',nchans),
                  "channelsout"            : config['highfreqres_contim'].get('chans',pipeline.nchans[0][0]),
                  "joinchannels"           : config['image'].get('joinchannels', joinchannels),
                  "fit-spectral-pol"       : config['highfreqres_contim'].get('fit_spectral_pol', 1),
                  "auto-mask"              : sdm.dismissable(config['highfreqres_contim'].get('auto_mask', None)),
                  "auto-threshold"         : config['highfreqres_contim'].get('auto_threshold', 10),
                  "multiscale"             : config['highfreqres_contim'].get('multi_scale', False),
                  "multiscale-scales"      : sdm.dismissable(config['highfreqres_contim'].get('multi_scale_scales', None)),
                  "fitsmask"               : sdm.dismissable(config['highfreqres_contim'].get('fits_mask', None)),
              }

        recipe.add('cab/wsclean', step,
        image_opts,
        input=pipeline.input,
        output=pipeline.output,
        label='{:s}:: Make image and model at fine frequency resolution'.format(step))

        if not config['highfreqres_contim'].get('niter', niter): imagetype=['image','dirty']
        else:
            imagetype=['image','dirty','psf','residual','model']
            if config['highfreqres_contim'].get('mgain', mgain)<1.0: imagetype.append('first-residual')
        if config['highfreqres_contim'].get('chans', pipeline.nchans[0][0]) > 1:
            for mm in imagetype:
                step = 'finechancontcube'
                recipe.add('cab/fitstool', step,
                    {
                    "image"    : [pipeline.prefix+'_fine-{0:04d}-{1:s}.fits:output'.format(d,mm) for d in xrange(config['highfreqres_contim'].get('chans',pipeline.nchans[0][0]))],
                    "output"   : pipeline.prefix+'_fine-contcube.{0:s}.fits'.format(mm),
                    "stack"    : True,
                    "delete-files" : True,
                    "fits-axis": 'FREQ',
                    },
                    input=pipeline.input,
                    output=pipeline.output,
                    label='{0:s}:: Make {1:s} cube from wsclean {1:s} channels'.format(step,mm.replace('-','_')))


import os
import sys

NAME = 'Self calibration loop'

def worker(pipeline, recipe, config):
    npix = concfig['img_npix']
    trim = concfig['img_trim']
    cleanborder = 0
    cell = concfig['img_cell']
    mgain = concfig['img_mgain']
    niter = concfig['img_niter']
    auto_thresh = concfig['img_autothreshold']
    auto_mask = concfig['img_auto_mask']
    robust = concfig['img_robust']
    
    mslist = pipeline.msnames
    prefix = pipeline.prefix

    if config['image_1']['enable']:
        if config['image_1']['peak_based_mask_on_dirty']:
            step = 'image_1_dirty_{0:d}'.format(i)
            recipe.add('add/wsclean', step,
                  {                   
                      "msname"    : mslit,
                      "column"    : 'CORRECTED_DATA',
                      "robust"    : 'briggs {:.3f}'.format(config.get('robust', None)) or robust,
                      "npix"      : config['image_1'].get('npix', None) or npix,
                      "prefix"    : prefix+'_1',
                  },
            input=INPUT,
            output=OUTPUT,
            label='{:s}:: Make dirty image to create clean mask'.format(step))

            step = 'mask_1_{0:d}'.format(i)
            mask = prefix+'_1-mask.fits:output'
            recipe.add('add/cleanmask', step,
                  {                   
                      "image"    : prefix+'_1-dirty.fits',
                      "output"   : mask,
                      "peak-fraction"    : config['image_1']['peak_fraction'],
                  },
            input=INPUT,
            output=OUTPUT,
            label='{:s}:: Make peak based mask from dirty image'.format(step))

            step = 'image_1_{0:d}'.format(i)
            recipe.add('add/wsclean', step,
                  {                   
                      "msname"    : mslit,
                      "column"    : 'CORRECTED_DATA',
                      "robust"    : 'briggs {:.3f}'.format(config.get('robust', None)) or robust,
                      "npix"      : config['image_1'].get('npix', None) or npix,
                      "trim"      : config['image_1'].get('trim', None) or trim,
                      "prefix"    : prefix+'_1',
                      "niter"     : config['image_1'].get('niter', None) or niter,
                      "mgain"     : config['image_1'].get('mgain', None) or mgain,
                      "channelsout"   : config['image_1'].get('nchans', None) or nchans,
                      "auto-threshold": config['image_1'].get('auto_thresh', None) or auto_thresh,
                      "fitsmask"  : mask,
                  },
            input=INPUT,
            output=OUTPUT,
            label='{:s}:: Image with clean peak-based mask from dirty image'.format(step))
 
        if config['extract_sources_1']['enable']:
            if config['extract_sources_1']['detection_image']:
                step = 'detection_image_1_{0:d}'.format(i)
                detection_image = prefix + '-detection_image_1.fits:output'
                recipe.add('add/fitstool', step,
                     {                   
                         "image"    : prefix+'_1-MFS-image.fits',
                         "output"   : detection_image,
                         "diff"     : True,
                         "force"    : True,
                     },
                     input=INPUT,
                     output=OUTPUT,
                     label='{0:s}:: Make convolved model'.format(step))
            else:
                detection_image = None

            recipe.add('add/pybdsm', step,
                 {                   
                     "image"    : prefix+'_1-MFS-image.fits',
                     "thresh_pix"   : config['extract_sources_1'].get('thresh_pix', None) or thresh_pix,
                     "thresh_isl"   : config['extract_sources_1'].get('thresh_isl', None) or thresh_isl,
                     "outfile"  : '{:s}.fits:output'.format(calmodel),
                     "port2tigger": True,
                     "detection_image": detection_image,
                 },
                 input=INPUT,
                 output=OUTPUT,
                 label='{0:s}:: Extract sources'.format(step))
               
            step = 'extract_1_{0:d}'.format(i)
            calmodel = prefix+'_1-pybdsm'
            recipe.add('add/pybdsm', step,
                  {                   
                      "image"    : prefix+'_1-MFS-image.fits',
                      "thresh_pix"   : config['extract_sources_1'].get('thresh_pix', None) or thresh_pix,
                      "thresh_isl"   : config['extract_sources_1'].get('thresh_isl', None) or thresh_isl,
                      "outfile"  : '{:s}.fits:output'.format(calmodel),
                      "port2tigger": True,
                  },
                  input=INPUT,
                  output=OUTPUT,
                  label='{0:s}:: Extract sources'.format(step))

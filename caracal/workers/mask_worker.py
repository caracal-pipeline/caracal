import os
import numpy as np
from astropy import units as u
import astropy.coordinates as coord
import shutil
import glob
from .utils.masking_utils import (
    query_catalog_nvss,
    query_catalog_sumss,
    pbcorr,
    set_mosaic_files,
    make_mask,
    make_mask_nvss,
    merge_masks,
    build_beam, 
    change_header,
)

NAME = 'Make Masks'
LABEL = 'mask'


def worker(pipeline, recipe, config):

    ################################################################################
    # Worker's MODULES
    ################################################################################

    def cleanup_mosaic_files(mask_dir):

        montage_tmpdir = pipeline.output + '/mask_mosaic'
        if os.path.exists(montage_tmpdir):
            shutil.rmtree(montage_tmpdir)

        casafiles = glob.glob(mask_dir + '/*.image')
        for i in range(0, len(casafiles)):
            shutil.rmtree(casafiles[i])

    def move_files(catalog_name, mask_dir):
        montage_mosaic = pipeline.output + '/mosaic.fits'
        montage_mosaic_area = pipeline.output + '/mosaic_area.fits'
        cat_mosaic = mask_dir + catalog_name + '_mosaic.fits'
        cat_mosaic_area = mask_dir + catalog_name + '_mosaic_area.fits'
        if os.path.exists(montage_mosaic):
            shutil.move(montage_mosaic, cat_mosaic)
        if os.path.exists(montage_mosaic_area):
            shutil.move(montage_mosaic_area, cat_mosaic_area)


################################################################################
# MAIN
################################################################################

    mask_dir = pipeline.masking + '/'

    centre = config['centre_coord']

    flabel = config['label_in']
    target_ms_dict = pipeline.get_target_mss(flabel)
    
    msinfo = None
    
    for target in target_ms_dict:
        if msinfo is None:
            msinfo = pipeline.get_msinfo(target_ms_dict[target])
            
        if centre[0] == 'HH:MM:SS' and centre[1] == 'DD:MM:SS':
            targetpos = msinfo['REFERENCE_DIR']
            while len(targetpos) == 1:
                targetpos = targetpos[0]
            coords = [targetpos[0] / np.pi * 180., targetpos[1] / np.pi * 180.]
            centreCoord = coord.SkyCoord(
                coords[0], coords[1], frame='icrs', unit=(u.deg, u.deg))
            centre[0] = centreCoord.ra.hms
            centre[1] = centreCoord.dec.dms

        mask_cell = config['cell_size']
        mask_imsize = config['mask_size']

        final_mask = mask_dir + str(config['label_out']) + '_' + str(target) + '.fits'
        catalog_name = config['catalog_query']['catalog']
        
        catalog_tab = mask_dir + catalog_name + '_' + pipeline.prefix + '_catalog.txt'

        if catalog_name == 'SUMSS':

            if pipeline.enable_task(config, 'catalog_query'):
                key = 'catalog_query'

                recipe.add(query_catalog_sumss, 'query_source_catalog',
                           {
                               'centre': centre,
                               'width_im': config[key]['image_width'],
                               'cat_name': catalog_name,
                               'catalog_table': catalog_tab,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Catalog pulled')

            # read catalog table
            fields_dir = pipeline.input + '/fields/'

            step = 'prepare'  # set directories
            recipe.add(set_mosaic_files, step,
                       {
                           'catalog_table': catalog_tab,
                           'mask_dir': mask_dir,
                           'fields_dir': fields_dir,

                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Preparing folders')

            step = 'mosaic'
            recipe.add('cab/montage', step,
                       {
                           'input_dir': 'masking/formosaic' + ':output',
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Mosaicing catalog')

            step = 'cleanup'  # cleanup
            recipe.add(move_files, step,
                       {
                           'catalog_name': catalog_name,
                           'mask_dir': mask_dir,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Cleanup folders')

        elif catalog_name == 'NVSS':

            if pipeline.enable_task(config, 'catalog_query'):
                key = 'catalog_query'

                catalog_tab = mask_dir + catalog_name + '_' + pipeline.prefix + '_catalog.txt'

                recipe.add(query_catalog_nvss, 'query-nvss',
                           {
                               'centre': centre,
                               'width_im': config[key]['image_width'],
                               'cat_name': catalog_name,
                               'thresh': config[key]['nvss_thr'],
                               'cell': mask_cell,
                               'imsize': mask_imsize,
                               'obs_freq': config['pbcorr']['frequency'],
                               'catalog_table': catalog_tab,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Catalog pulled')

            if pipeline.enable_task(config, 'make_mask'):

                if pipeline.enable_task(config, 'merge_with_extended') == False:
                    cat_mask = final_mask
                else:
                    cat_mask = mask_dir + '/' + config['label_out'] + '_' + str(target) + '.fits'
                catalog_tab = mask_dir + catalog_name + '_' + pipeline.prefix + '_catalog.txt'

                recipe.add(make_mask_nvss, 'make_mask-nvss',
                           {
                               "catalog_table": catalog_tab,
                               "centre": centre,
                               'cell': mask_cell,
                               'imsize': mask_imsize,
                               "mask": cat_mask,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Mask from catalog')

        if pipeline.enable_task(config, 'make_mask') and catalog_name == 'SUMSS':
            if pipeline.enable_task(config, 'pbcorr'):

                recipe.add(build_beam, 'make_pb',
                           {
                               'obs_freq': config['pbcorr']['frequency'],
                               'centre': centre,
                               'cell': mask_cell,
                               'imsize': mask_imsize,
                               'out_beam': mask_dir + '/gauss_pbeam.fits',
                           },
                           input=pipeline.input,
                           output=pipeline.output)

                mosaic = 'masking/' + catalog_name + '_mosaic.fits'
                mosaic_casa = 'masking/mosaic_casa.image'

                beam = 'masking/gauss_pbeam.fits'
                beam_casa = 'masking/gauss_pbeam.image'

                mosaic_regrid_casa = 'masking/mosaic_regrid.image'
                mosaic_regrid = 'masking/' + catalog_name + '_mosaic_regrid.fits'
                mosaic_pbcorr = 'masking/' + catalog_name + '_mosaic_pbcorr.fits'

                step = 'import-mosaic'
                recipe.add('cab/casa_importfits', step,
                           {
                               "fitsimage": mosaic + ':output',
                               "imagename": mosaic_casa + ":output",
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Mosaic in casa format')

                step = 'import-beam'
                recipe.add('cab/casa_importfits', step,
                           {
                               "fitsimage": beam + ':output',
                               "imagename": beam_casa,
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Beam in casa format')

                step = 'regrid'
                recipe.add('cab/casa_imregrid', step,
                           {
                               "template": beam_casa + ':output',
                               "imagename": mosaic_casa + ':output',
                               "output": mosaic_regrid_casa,
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Regridding mosaic to size and projection of dirty image')

                step = 'export'
                recipe.add('cab/casa_exportfits', step,
                           {
                               "fitsimage": mosaic_regrid + ':output',
                               "imagename": mosaic_regrid_casa + ':output',
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Extracted regridded mosaic')

                recipe.add(pbcorr, 'correct_pb',
                           {
                               "mosaic_regrid": pipeline.output + '/' + mosaic_regrid,
                               "mosaic_pbcorr": pipeline.output + '/' + mosaic_pbcorr,
                               "beam": pipeline.output + '/' + beam,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Correcting mosaic for primary beam')

            if config['make_mask']['input_image'] == 'pbcorr':
                in_image = 'masking/' + catalog_name + '_mosaic_pbcorr.fits'
            else:
                in_image = 'masking/' + config['make_mask']['input_image']

            if config['make_mask']['mask_method'] == 'thresh':

                if pipeline.enable_task(config, 'merge_with_extended') == False:
                    cat_mask = final_mask
                else:
                    cat_mask = mask_dir + '/' + catalog_name + '_mask.fits'

                recipe.add(make_mask, 'make_mask-mosaic',
                           {
                               "mosaic_pbcorr": pipeline.output + '/' + in_image,
                               "mask": cat_mask,
                               "contour": config['make_mask']['thr_lev'],
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Mask done')

            elif config['make_mask']['mask_method'] == 'sofia':

                imagename = in_image
                def_kernels = [[3, 3, 0, 'b'], [6, 6, 0, 'b'], [15, 15, 0, 'b']]
                image_opts = {
                    "import.inFile": imagename,
                    "steps.doFlag": True,
                    "steps.doScaleNoise": True,
                    "steps.doSCfind": True,
                    "steps.doMerge": False,
                    "steps.doReliability": False,
                    "steps.doParameterise": False,
                    "steps.doWriteMask": True,
                    "steps.doMom0": False,
                    "steps.doMom1": False,
                    "steps.doWriteCat": False,
                    "SCfind.kernelUnit": 'pixel',
                    "SCfind.kernels": def_kernels,
                    "SCfind.threshold": config['make_mask']['thr_lev'],
                    "SCfind.rmsMode": 'mad',
                    "SCfind.edgeMode": 'constant',
                    "SCfind.fluxRange": 'all',
                    "scaleNoise.statistic": 'mad',
                    "scaleNoise.windowSpatial": config['make_mask']['scale_noise_window'],
                    "scaleNoise.windowSpectral": 1,
                    "scaleNoise.method": 'local',
                    "scaleNoise.fluxRange": 'all',
                    "scaleNoise.scaleX": True,
                    "scaleNoise.scaleY": True,
                    "scaleNoise.scaleZ": False,
                    "writeCat.basename": str(config['label_out']),
                }

                step = "make_mask-sofia"
                recipe.add('cab/sofia', 'make_mask-sofia',
                           image_opts,
                           input=pipeline.output,
                           output=pipeline.output + '/masking/',
                           label='{0:s}:: Make SoFiA mask'.format(step))
                recipe.add(change_header, 'extract-mosaic',
                           {
                               "filename": final_mask,
                               "headfile": pipeline.output + '/' + imagename,
                               "copy_head": True,
                           },
                           input=pipeline.output,
                           output=pipeline.output,
                           label='Extracted regridded mosaic')

        if pipeline.enable_task(config, 'merge_with_extended'):

            key = 'merge_with_extended'

            ext_name = config[key]['extended_source_map']
            extended = 'fields/' + ext_name
            extended_casa = 'masking/extended.image'

            extended_regrid_casa = 'masking/Fornaxa_vla_regrid.image'
            extended_regrid = 'masking/Fornaxa_vla_regrid.fits'

            beam = 'masking/gauss_pbeam.fits'

            if os.path.exists(pipeline.output + '/' + beam) == False:
                recipe.add(build_beam, 'build_pb',
                           {
                               'obs_freq': config['pbcorr']['frequency'],
                               'centre': centre,
                               'cell': mask_cell,
                               'imsize': mask_imsize,
                               'out_beam': mask_dir + '/gauss_pbeam.fits',
                           },
                           input=pipeline.input,
                           output=pipeline.output)

            beam_casa = 'masking/gauss_pbeam.image'

            if os.path.exists(pipeline.output + '/' + beam_casa) == False:
                recipe.add('cab/casa_importfits', 'import-pb',
                           {
                               "fitsimage": beam + ':output',
                               "imagename": beam_casa,
                               "overwrite": True,
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Importing beam for extended mask')

            ext_name_root = ext_name.split('.')[0]
            extended_pbcorr = 'masking/' + ext_name_root + '_pbcorr.fits'
            extended_mask = '/masking/' + ext_name_root + '_mask.fits'

            recipe.add('cab/casa_importfits', 'import-mosaic',
                       {
                           "fitsimage": extended + ":input",
                           "imagename": extended_casa + ":output",
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Importing extended mask')

            recipe.add('cab/casa_imregrid', 'regrid',
                       {
                           "imagename": extended_casa + ":output",
                           "template": beam_casa + ":output",
                           "output": extended_regrid_casa,
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Regridding extended mask')

            recipe.add('cab/casa_exportfits', 'export',
                       {
                           "fitsimage": extended_regrid + ":output",
                           "imagename": extended_regrid_casa + ":output",
                           "overwrite": True,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Exporting extended mask')

            recipe.add(pbcorr, 'correct_pb',
                       {
                           "mosaic_regrid": pipeline.output + '/' + extended_regrid,
                           "mosaic_pbcorr": pipeline.output + '/' + extended_pbcorr,
                           "beam": pipeline.output + '/' + beam,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Correcting mask for primary beam')

            if config['merge_with_extended']['mask_method'] == 'thresh':

                recipe.add(make_mask, 'make_mask-extend',
                           {
                               "mosaic_pbcorr": pipeline.output + '/' + extended_pbcorr,
                               "mask": pipeline.output + extended_mask,
                               "contour": config['merge_with_extended']['thr_lev'],
                           },
                           input=pipeline.input,
                           output=pipeline.output,
                           label='Mask done')

            cat_mask = config['mask_prefix']['mask_method']
            recipe.add(merge_masks, 'make_mask-merge',  # 'Merging VLA Fornax into catalog mask',
                       {
                           "extended_mask": pipeline.output + extended_mask,
                           "catalog_mask": cat_mask,
                           "end_mask": final_mask,
                       },
                       input=pipeline.input,
                       output=pipeline.output,
                       label='Total mask done')

        recipe.add(cleanup_mosaic_files, 'cleanup',
                   {
                       'mask_dir': mask_dir,
                   },
                   input=pipeline.input,
                   output=pipeline.output,
                   label='Cleanup folders')

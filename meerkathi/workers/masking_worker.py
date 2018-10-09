import os, sys, math, shutil
import yaml
import numpy as np
from astroquery.vizier import Vizier
from astropy import units as u
import astropy.coordinates as coord
from astropy import wcs
from astropy.io import fits, ascii


NAME = 'Make masks'

def worker(pipeline, recipe, config):


################################################################################
#Worker's MODULES 
################################################################################

	pipeline.output = pipeline.output+'/masking/'
	if os.path.exists(pipeline.output) != True:
		os.mkdir(pipeline.output)


	def query_catalog(centre,width_im,cat_name):

		catalog_table = pipeline.output+cat_name+'_'+pipeline.prefix+'_catalog.txt'			
		Vizier.ROW_LIMIT = -1

		p = Vizier.query_region(coord.SkyCoord(centre[0], centre[1], unit=(u.hourangle, u.deg), frame='icrs'), 
			width=width_im, catalog=cat_name)
		tab = p[0]
		ascii.write(tab,catalog_table,overwrite=True)


	def build_beam(obs_freq,copy_head,headfile,centre,cell,imsize,out_beam):

		if copy_head == True:
			hdrfile = fits.open(headfile)
			hdr = hdrfile[0].header
		elif copy_head == False:

			w = wcs.WCS(naxis=2)

			centre = coord.SkyCoord(centre[0], centre[1], unit=(u.hourangle, u.deg), frame='icrs')
			cell /= 3600.

			w.wcs.crpix = [imsize/2, imsize/2]
			w.wcs.cdelt = np.array([-cell, cell])
			w.wcs.crval = [centre.ra.deg, centre.dec.deg]
			w.wcs.ctype = ["RA---SIN", "DEC--SIN"]

			hdr = w.to_header()
			hdr['SIMPLE']  = 'T'
			hdr['BITPIX']  = -32
			hdr['NAXIS']   = 2
			hdr.set('NAXIS1',  imsize, after='NAXIS')
			hdr.set('NAXIS2',  imsize, after='NAXIS1')
		
		if 'CUNIT1' in hdr:
			del hdr['CUNIT1']
		if 'CUNIT2' in hdr:
			del hdr['CUNIT2']
		
		pb_fwhm = 1.02*(2.99792458E8)/obs_freq/13.5/np.pi*180.
		pb_fwhm_pix = pb_fwhm/hdr['CDELT2']
		x, y = np.meshgrid(np.linspace(-hdr['NAXIS2']/2.,hdr['NAXIS2']/2.,hdr['NAXIS2']), 
						   np.linspace(-hdr['NAXIS1']/2.,hdr['NAXIS1']/2.,hdr['NAXIS1']))
		d = np.sqrt(x*x+y*y)
		sigma, mu = pb_fwhm_pix/2.35482, 0.0
		gauss = np.exp(-( (d-mu)**2 / ( 2.0 * sigma**2 ) ) )

		fits.writeto(out_beam,gauss,hdr,overwrite=True)

	def make_mosaic(catalog_table,fields_dir,mask_dir):
			
		tab = ascii.read(catalog_table)
		unique, counts = np.unique(tab['Mosaic'], return_counts=True)
		mosaic_tmpdir = mask_dir+'/formosaic/'
		mosaic_outdir = mask_dir+'/mosaic/'
		if os.path.exists(mosaic_tmpdir) == False:
				os.mkdir(mosaic_tmpdir)

		for i in xrange(0,len(unique)):
				
				summsfield = fields_dir+str(unique[i])+'.FITS'
				outfield = mosaic_tmpdir+str(unique[i])+'.FITS'         
				shutil.copy(summsfield,outfield)
		
		if os.path.exists(mosaic_outdir) == True:
				shutil.rmtree(mosaic_outdir)

		montage.mosaic(mosaic_tmpdir, mosaic_outdir)
		shutil.rmtree(mosaic_tmpdir)

#   This method does not conserve the number of pixels in the image
#   def regrid_mosaic(mosaic,beam,mosaic_regrid,headername):
#       
#       montage.mGetHdr(beam,headername)
#       montage.mProject(mosaic,mosaic_regrid,headername)

	def pbcorr(beam,mosaic_regrid,mosaic_pbcorr):
			
			#montage.mProject(mosaic,mosaic_pbgrid,header_beam)
			
		pblist = fits.open(beam)
		pbdata = pblist[0].data

		mlist = fits.open(mosaic_regrid)
		mdata = mlist[0].data
		mhead = mlist[0].header
		mhead['EPOCH']=2000
		
		if 'LONPOLE' in mhead:
			del mhead['LONPOLE']
		if 'LATPOLE' in mhead:
			del mhead['LATPOLE']
		if 'RADESYS' in mhead:
			del mhead['RADESYS']
		
		pbcorr = np.multiply(mdata,pbdata)
		fits.writeto(mosaic_pbcorr,pbcorr,mhead,overwrite=True)
		mlist.close()
		pblist.close()

	def change_header(filename, copy_head, headfile):

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

	def make_mask(mosaic_pbcorr,mask,contour):

		moslist = fits.open(mosaic_pbcorr)
		mosdata = moslist[0].data
		moshead = moslist[0].header

		mosdata[np.isnan(mosdata)] = 0.0

		index_in = np.where(mosdata >= contour)
		index_out = np.where(mosdata <= contour)
		mosdata[index_in] = 1.0
		mosdata[index_out] = 0.0

		fits.writeto(mask,mosdata,moshead,overwrite=True)

################################################################################
#MAIN 
################################################################################

	mask_dir  = pipeline.output+'/masking'
	if os.path.exists(mask_dir) != True:
		os.mkdir(mask_dir)


	centre = config.get('centre_coord', [0,0])
	catalog_name = config.get('catalog', 'SUMSS')
	
	#input files
	catalog_mask = mask_dir+str(config.get('catalog_mask', 'tmp_catalog.fits'))
	#extended_finalmask = pipeline.output+'/masking/'+str(config.get('fornaxa_finalmask', 'tmp_fornaxa.fits'))
	final_mask = mask_dir+str(config.get('final_mask', 'tmp_final_mask.fits'))   


	if pipeline.enable_task(config, 'query_catalog'):
		key = 'query_catalog'

		recipe.add(query_catalog, 'query_source_catalog', 
			{
				'centre'  : centre,
				'width_im': config[key].get('width_image', '2d'), 
				'cat_name': catalog_name,
			}, 
			input=pipeline.input, 
			output=pipeline.output,
			label = 'Catalog pulled')



	#if catalog_name == 'SUMSS':




# 	#	if pipeline.enable_task(config, 'make_mosaic'):
# 			key = 'make_mosaic'

# 			recipe.add(make_mosaic, 'make mosaic using montage', 
# 					{ 'catalog_table' : pipeline.output+'/masking/SUMMS_'+pipeline.prefix+'_catalog.txt',
# 					  'fields_dir'    : pipeline.input+'/fields/',
# 					  'mask_dir'      : pipeline.output+'/masking/',
# 					}, 
# 					input=pipeline.input, 
# 					output=pipeline.output,
# 					label='Mosaic made by montage')

# 		if pipeline.enable_task(config, 'casa_regrid'):
# 			key = 'casa_regrid'
# 			step = '1'

# 			mosaic = 'masking/mosaic/mosaic.fits'
# 			beam = 'masking/'+pipeline.prefix+'_gauss_pbeam.fits'

# 			mosaic_casa = 'masking/mosaic/mosaic_casa.image'
# 			beam_casa = 'masking/'+pipeline.prefix+'_gauss_pbeam.image'
# 			mosaic_regrid_casa = 'masking/mosaic_regrid.image'
# 			mosaic_regrid = 'masking/mosaic_regrid.fits'

# 			recipe.add('cab/casa_importfits', step,
# 				{
# 					"fitsimage"         : mosaic+":output",
# 					"imagename"         : mosaic_casa+":output",
# 					"overwrite"         : True,
# 				},
# 				input=pipeline.input,
# 				output=pipeline.output,
# 				label='Mosaic in casa format')

# 			step =  '2'
# 			recipe.add('cab/casa_importfits', step,
# 					{
# 						"fitsimage"         : beam+':output',
# 						"imagename"         : beam_casa,
# 						"overwrite"         : True,
# 					},
# 					input=pipeline.input,
# 					output=pipeline.output,
# 					label='Beam in casa format')
			
# 			step = '3'
# 			recipe.add('cab/casa_imregrid', step,
# 					{
# 						"template"      : beam_casa+':output',
# 						"imagename"     : mosaic_casa+':output',
# 						"output"        : mosaic_regrid_casa,
# 						"overwrite"     : True,
# 					},
# 					input=pipeline.input,
# 					output=pipeline.output,
# 					label='Regridding mosaic to size and projection of dirty image')
			
# 			step = '4'
# 			recipe.add('cab/casa_exportfits', step,
# 					{
# 						"fitsimage"         : mosaic_regrid+':output',
# 						"imagename"         : mosaic_regrid_casa+':output',
# 						"overwrite"         : True,
# 					},
# 					input=pipeline.input,
# 					output=pipeline.output,
# 					label='Extracted regridded mosaic')
			
# 			key = 'build_beam'
# 			recipe.add(change_header, 'correct header from CASA errors',
# 					{ 'filename' : pipeline.output+'/'+mosaic_regrid,
# 					  'copy_head' : config[key].get('copyhead', False),
# 					  'headfile' : pipeline.output+'/'+str(config[key].get('headname','tmp_image_1.fits')),
# 					},
# 					input=pipeline.input,
# 					output=pipeline.output,
# 					label='Header corrected')

# 	if pipeline.enable_task(config, 'pb_correction'):

# 		key = 'build_beam'
# 		#if  config[key].get('copyhead') ==True:
# 	#		headerfile = pipeline.output+config[key].get('headname','tmp_image_1.fits')
# 		recipe.add(build_beam, 'build gaussian primary beam', 
# 				{ 
# 					'obs_freq' : config[key].get('frequency', 1.42014e9),
# 					'centre'   : centre,
# 					'cell'     : config[key].get('cell_size', 1), 
# 					'imsize'   : config[key].get('img_size', 1000),
# 					'out_beam' : pipeline.output+'/masking/'+pipeline.prefix+'_gauss_pbeam.fits',
# 					'copy_head' : config[key].get('copyhead', False),
# 					'headfile' : pipeline.output+'/'+str(config[key].get('headname','tmp_image_1.fits')),
# 				}, 
# 				input=pipeline.input, 
# 				output=pipeline.output)
		 
# 		mosaic_regrid= pipeline.output+'/masking/mosaic_regrid.fits'
# 		beam  = pipeline.output+'/masking/'+pipeline.prefix+'_gauss_pbeam.fits'
# 		mosaic_pbcorr = pipeline.output+'/masking/mosaic_pbcorr.fits'
# #   
# 		recipe.add(pbcorr, 'Correcting mosaic for primary beam',
# 			{
# 				"mosaic_regrid" : mosaic_regrid,
# 				"mosaic_pbcorr" : mosaic_pbcorr,
# 				"beam"          : beam,
# 			},
# 			input=pipeline.input,
# 			output=pipeline.output,
# 			label='Correcting mosaic for primary beam')

# 	if pipeline.enable_task(config, 'make_mask'):
# 		key = 'make_mask'
# 		mosaic_pbcorr = pipeline.output+'/masking/mosaic_pbcorr.fits'

# 		recipe.add(make_mask, 'Build mask from mosaic',
# 				{
# 					"mosaic_pbcorr" : mosaic_pbcorr,
# 					"mask"          : sumss_mask, 
# 					"contour"       : config[key].get('contour', 10e-3),  
# 				},
# 				input=pipeline.input,
# 				output=pipeline.output,
# 				label='Mask done')



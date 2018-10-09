import os, sys, math, shutil,glob
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

	mask_dir = pipeline.output+'/masking/'
	if os.path.exists(mask_dir) != True:
		os.mkdir(mask_dir)

	def query_catalog(catalog_table,centre,width_im,cat_name):

		Vizier.ROW_LIMIT = -1

		p = Vizier.query_region(coord.SkyCoord(centre[0], centre[1], unit=(u.hourangle, u.deg), frame='icrs'), 
			width=width_im, catalog=cat_name)
		tab = p[0]
		ascii.write(tab,catalog_table,overwrite=True)

	def set_mosaic_files(catalog_table,mask_dir,fields_dir):
		
		tab = ascii.read(catalog_table)
		unique, counts = np.unique(tab['Mosaic'], return_counts=True)

		mosaic_tmpdir = mask_dir+'/formosaic/'
		mosaic_outdir = mask_dir+'/mosaic/'
		
		if os.path.exists(mosaic_tmpdir) == False:
				os.mkdir(mosaic_tmpdir)
		if os.path.exists(mosaic_outdir) == True:
				shutil.rmtree(mosaic_outdir)

		for i in xrange(0,len(unique)):		
				summsfield = fields_dir+str(unique[i])+'.FITS'
				outfield = mosaic_tmpdir+str(unique[i])+'.FITS'         
				shutil.copy(summsfield,outfield)

	def cleanup_mosaic_files(catalog_name,mask_dir):

		montage_tmpdir = pipeline.output+'/mask_mosaic'
		if os.path.exists(montage_tmpdir):
			shutil.rmtree(montage_tmpdir)

		casafiles = glob.glob(mask_dir+'/*.image')
		for i in xrange(0,len(casafiles)):
			shutil.rmtree(casafiles[i])

	def move_files(catalog_name,mask_dir):
		montage_mosaic = pipeline.output+'/mosaic.fits'
		montage_mosaic_area = pipeline.output+'/mosaic_area.fits'
		cat_mosaic = mask_dir+catalog_name+'_mosaic.fits'
		cat_mosaic_area = mask_dir+catalog_name+'_mosaic_area.fits'
		if os.path.exists(montage_mosaic):
			shutil.move(montage_mosaic,cat_mosaic)
		if os.path.exists(montage_mosaic_area):
			shutil.move(montage_mosaic_area,cat_mosaic_area)		

	def build_beam(obs_freq,centre,cell,imsize,out_beam):

		#if copy_head == True:
		#	hdrfile = fits.open(headfile)
		#	hdr = hdrfile[0].header
		#elif copy_head == False:

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

	def pbcorr(beam,mosaic_regrid,mosaic_pbcorr):
						
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

	def merge_masks(extended_mask,catalog_mask,end_mask):

		catlist = fits.open(catalog_mask)
		catdata = catlist[0].data
		cathead = catlist[0].header

		forlist = fits.open(extended_mask)
		fordata = forlist[0].data
		forhead = forlist[0].header
		fordata = np.squeeze(fordata)
		fordata = np.squeeze(fordata)
		
		index_fornan = np.isnan(fordata)
		fordata[index_fornan] = catdata[index_fornan]
		index_forzero = np.where(fordata < 1)
		fordata[index_forzero] = catdata[index_forzero]

		fits.writeto(end_mask,fordata,cathead,overwrite=True)

################################################################################
#MAIN 
################################################################################

	centre = config.get('centre_coord', [0,0])	
	mask_cell = config.get('cell_size', 1.)
	mask_imsize = config.get('mask_size',3600.)

	final_mask = mask_dir+str(config.get('name_mask', 'final_mask.fits'))   
	catalog_name = config.get('catalog', 'SUMSS')	

	if pipeline.enable_task(config, 'query_catalog'):
		key = 'query_catalog'
	
		catalog_tab = mask_dir+catalog_name+'_'+pipeline.prefix+'_catalog.txt'			

		recipe.add(query_catalog, 'query_source_catalog', 
			{
				'centre'  : centre,
				'width_im': config[key].get('width_image', '2d'), 
				'cat_name': catalog_name,
				'catalog_table' : catalog_tab,
			}, 
			input=pipeline.input, 
			output=pipeline.output,
			label = 'Catalog pulled')

		if catalog_name == 'SUMSS':

			#read catalog table
			fields_dir = pipeline.input+'/fields/'

			step = '1' #set directories
			recipe.add(set_mosaic_files, step,
				{
			    	'catalog_table': catalog_tab,
			    	'mask_dir': mask_dir,
			    	'fields_dir': fields_dir,

				},
				input = pipeline.input,
				output = pipeline.output,
				label='Preparing folders')

			step = '2'
			recipe.add('cab/montage', step,
				{
			    	'input_dir': 'masking/formosaic'+':output',
				},
				input = pipeline.input,
				output = pipeline.output,
				label='Mosaicing catalog')

			step = '5' #cleanup
			recipe.add(move_files, step,
				{
				    'catalog_name': catalog_name,
				    'mask_dir': mask_dir,
				},
				input = pipeline.input,
				output = pipeline.output,
				label='Cleanup folders')

	if pipeline.enable_task(config, 'pb_correction'):

		recipe.add(build_beam, 'build gaussian primary beam', 
			{ 
				'obs_freq' : config['pb_correction'].get('frequency', 1.42014e9),
				'centre'   : centre,
				'cell'     : mask_cell, 
				'imsize'   : mask_imsize,
				'out_beam' : mask_dir+'/gauss_pbeam.fits',
			}, 
			input=pipeline.input, 
			output=pipeline.output)

		mosaic = 'masking/'+catalog_name+'_mosaic.fits'
		mosaic_casa = 'masking/mosaic_casa.image'

		beam = 'masking/gauss_pbeam.fits'
		beam_casa = 'masking/gauss_pbeam.image'

		mosaic_regrid_casa = 'masking/mosaic_regrid.image'
		mosaic_regrid = 'masking/'+catalog_name+'_mosaic_regrid.fits'
	
		mosaic_pbcorr = 'masking/'+catalog_name+'_mosaic_pbcorr.fits'

		step =  '1'
		recipe.add('cab/casa_importfits', step,
			{
				"fitsimage"         : mosaic+':output',
				"imagename"         : mosaic_casa+":output",
				"overwrite"         : True,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Mosaic in casa format')

		step =  '2'
		recipe.add('cab/casa_importfits', step,
			{
				"fitsimage"         : beam+':output',
				"imagename"         : beam_casa,
				"overwrite"         : True,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Beam in casa format')
		
		step = '3'
		recipe.add('cab/casa_imregrid', step,
			{
				"template"      : beam_casa+':output',
				"imagename"     : mosaic_casa+':output',
				"output"        : mosaic_regrid_casa,
				"overwrite"     : True,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Regridding mosaic to size and projection of dirty image')

		step = '4'
		recipe.add('cab/casa_exportfits', step,
			{
				"fitsimage"         : mosaic_regrid+':output',
				"imagename"         : mosaic_regrid_casa+':output',
				"overwrite"         : True,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Extracted regridded mosaic')

		recipe.add(pbcorr, 'Correcting mosaic for primary beam',
			{
				"mosaic_regrid" : pipeline.output+'/'+mosaic_regrid,
				"mosaic_pbcorr" : pipeline.output+'/'+mosaic_pbcorr,
				"beam"          : pipeline.output+'/'+beam,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Correcting mosaic for primary beam')

	if pipeline.enable_task(config, 'make_mask'):

		if config['make_mask'].get('mask_with', 'thresh') == 'thresh':
	
			if config['make_mask'].get('input_image', 'pbcorr') == 'pbcorr':
				in_image = 'masking/'+catalog_name+'_mosaic_pbcorr.fits'
			else:
				in_image = 'masking/'+ config['make_mask'].get('input_image', 'pbcorr')

			if pipeline.enable_task(config, 'merge_with_extended') == False:
				cat_mask = final_mask
			else:
				cat_mask = mask_dir+'/catalog_mask.fits'

			recipe.add(make_mask, 'Build mask from mosaic',
				{
					"mosaic_pbcorr" : pipeline.output+'/'+in_image,
					"mask"          : cat_mask, 
					"contour"       : config['make_mask'].get('thresh_lev', 10e-3),  
				},
				input=pipeline.input,
				output=pipeline.output,
				label='Mask done')
				

	if pipeline.enable_task(config, 'merge_with_extended'):

		key='merge_with_extended'

		ext_name = config[key].get('extended_source_input','Fornaxa_vla.fits')
		extended = 'fields/'+ext_name
		extended_casa = 'masking/extended.image'
		
		extended_regrid_casa = 'masking/Fornaxa_vla_regrid.image'
		extended_regrid = 'masking/Fornaxa_vla_regrid.fits'
		
		beam = 'masking/gauss_pbeam.fits'
		beam_casa = 'masking/gauss_pbeam.image'
		
		ext_name_root = ext_name.split('.fits')[0]
		extended_pbcorr = 'masking/'+ext_name_root+'_pbcorr.fits'

		extended_mask = '/masking/'+ext_name_root+'_mask.fits'

		step = '1'
		recipe.add('cab/casa_importfits', step,
			{
				"fitsimage"         : extended+":input",
				"imagename"         : extended_casa+":output",
				"overwrite"         : True,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Fornax A in casa format')
		
 		step = '2'    
 		recipe.add('cab/casa_imregrid', step,
 			{
 				"imagename"         : extended_casa+":output",
 				"template"          : beam_casa+":output",
 				"output"            : extended_regrid_casa,
 				"overwrite"         : True,
 			},
 			input=pipeline.input,
 			output=pipeline.output,
 			label='Regridding Fornax A')

 		step = '3'
 		recipe.add('cab/casa_exportfits', step,
 			{
 				"fitsimage"         : extended_regrid+":output",
 				"imagename"         : extended_regrid_casa+":output",
 				"overwrite"         : True,
 			},
 			input=pipeline.input,
 			output=pipeline.output,
 			label='Extracted regridded Fornax A')


		recipe.add(pbcorr, 'Correcting extended for primary beam',
			{
				"mosaic_regrid" : pipeline.output+'/'+extended_regrid,
				"mosaic_pbcorr" : pipeline.output+'/'+extended_pbcorr,
				"beam"          : pipeline.output+'/'+beam,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Correcting mosaic for primary beam')

		if config['merge_with_extended'].get('mask_with', 'thresh') == 'thresh':

			recipe.add(make_mask, 'Build mask for extended',
				{
					"mosaic_pbcorr" : pipeline.output+'/'+extended_pbcorr,
					"mask"          : pipeline.output+extended_mask, 
					"contour"       : config['merge_with_extended'].get('thresh_lev', 8e-4),  
				},
				input=pipeline.input,
				output=pipeline.output,
				label='Mask done')

		recipe.add(merge_masks, 'Merging VLA Fornax into catalog mask',
			{
				"extended_mask"	: pipeline.output+extended_mask,
				"catalog_mask"	: mask_dir+'/catalog_mask.fits',
				"end_mask"  	: final_mask,
			},
			input=pipeline.input,
			output=pipeline.output,
			label='Total mask done')
	 		

	step = '10' #cleanup
	recipe.add(cleanup_mosaic_files, step,
		{
	    	'catalog_name': catalog_name,
	    	'mask_dir': mask_dir,
		},
		input = pipeline.input,
		output = pipeline.output,
	 	label='Cleanup folders')

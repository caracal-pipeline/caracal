########################
### INITIAL SETTINGS ###
########################

# Where to find the Caracal schema directory
caracalDir = '../'

# Where to write the doc files
docsDir = 'sphinx/'

# List of workers sorted in the order we want them to be given in the docs
# Workers in this list but not in the schema directory will be dropped
# Workers in the schema directory that are missing from this list will be appended to it
# (I.e., this list could include all kind of garbage or be empty and the code would still work)
sortedWorkers = [
  'crosscal',
  'ddcal',
  'flag',
  'general',
  'getdata',
  'inspect',
  'line',
  'mask',
  'mosaic',
  'obsconf',
  'prep',
  'selfcal',
  'transform',
  ]



########################
### DEFINE FUNCTIONS ###
########################

# Get and adapt Caracal README.rst
def getCaracalReadme(pipeDr,dcsDr):
  print('  INFO: Getting and adapting {0:s}/README.rst ...'.format(pipeDr))
  f=open(pipeDr+'/README.rst')
  readme=f.readlines()
  f.close()
  f=open(dcsDr+'caracalREADME.rst','w')
  nn=readme.index('Installation & Run\n')
  while nn<len(readme) and 'Running' not in readme[nn]:
    f.write(readme[nn])
    nn+=1
  f.close()


# Write the worker general page index.rst files
def writeWorkerPageIndex(srtWrks,wrkDr):
  print('  INFO: Writing workers homepage index.rst ...')
  writeLines=[
    '.. caracal documentation master file, created by',
    '   sphinx-quickstart on Mon Feb 18 15:04:26 2019.',
    '   You can adapt this file completely to your liking, but it should at least',
    '   contain the root `toctree` directive.',
    ' ',
    '.. _workers:',
    ' ',
    '==================',
    'Workers parameters',
    '==================',
    ' ',
    '.. toctree::',
    '   :maxdepth: 1',
    ' ',
    ]

  for wrk in srtWrks: writeLines.append('   {0:s}/index.rst'.format(wrk))

  f=open(wrkDr+'index.rst','w')
  for ll in writeLines: f.write(ll+'\n')
  f.close()


# Write documentation for a given nested level of a worker's schema
def writeWorkerLevel(writeFile,parameter,indentLevel=''):

  # write parameter type, required, default
  if 'enum' in parameter:
    writeFile.write('{0:s}*{{'.format(indentLevel))
    for ee in range(len(parameter['enum'])):
      if ee: writeFile.write(', ')
      if parameter['enum'][ee]=='': writeFile.write('""')
      elif parameter['type']=='str': writeFile.write('"{0:s}"'.format(parameter['enum'][ee]))
      else: writeFile.write('{0:s}'.format(parameter['enum'][ee]))
    writeFile.write('}*')
    if 'required' in parameter and not parameter['required']:
      writeFile.write(', *optional*')
      if 'example' in parameter:
        if len(parameter['example']): writeFile.write(', *default = {0}*'.format(parameter['example']))
        else:  writeFile.write(', *default = \' \'*')
    writeFile.write('\n\n')
  elif 'type' in parameter and parameter['type']!='map':
    writeFile.write('{1:s}*{0:s}*'.format(parameter['type'],indentLevel))
    if 'required' in parameter and not parameter['required']:
      writeFile.write(', *optional*')
      if 'example' in parameter:
        if len(parameter['example']): writeFile.write(', *default = {0}*'.format(parameter['example']))
        else:  writeFile.write(', *default = \' \'*')
    writeFile.write('\n\n')
  elif 'seq' in parameter:
    writeFile.write('{0:s}*list*'.format(indentLevel))
    for ee in parameter['seq']:
      if 'type' in ee:
        writeFile.write(' *of {0:s}*'.format(ee['type']))
    if 'required' in parameter and not parameter['required']:
      writeFile.write(', *optional*')
      if 'example' in parameter:
        if len(parameter['example']): writeFile.write(', *default = {0}*'.format(parameter['example']))
        else:  writeFile.write(', *default = \' \'*')
    writeFile.write('\n\n')
  elif 'required' in parameter and not parameter['required']:
    writeFile.write('{0:s}*optional*'.format(indentLevel))
    if 'example' in parameter:
      if len(parameter['example']): writeFile.write('*default = {0}*'.format(parameter['example']))
      else:  writeFile.write(', *default = \' \'*')
    writeFile.write('\n\n')

  # write parameter description
  if 'desc' in parameter: writeFile.write('{1:s}{0:s}\n\n'.format(parameter['desc'].replace('*','\*'),indentLevel))


# Write the individual workers' index.rst files
def writeWorkersIndex(srtWrks,wrkDr,schms,schDr):
  print('  INFO: Writing individual workers index.rst based on the following schema files ...')
  # Loop over all workers
  for wrk in srtWrks:
    # Write preamble to index.rst
    writeLines=[
      '.. caracal documentation master file, created by',
      '   sphinx-quickstart on Mon Feb 18 15:04:26 2019.',
      '   You can adapt this file completely to your liking, but it should at least',
      '   contain the root `toctree` directive.',
      ' ',
      '.. _{0:s}:'.format(wrk),
      ' ',
      '==========================================',
      wrk,
      '==========================================',
      ' ',
      '.. toctree::',
      '   :maxdepth: 1',
      ' ',
      ]
    f=open(wrkDr+wrk+'/index.rst','w')
    for ll in writeLines: f.write(ll+'\n')

    # Find matching schema file and open it with ruamel.yaml
    matchingSchema=[]
    for nn in range(len(schms)):
      if wrk in schms[nn]: matchingSchema.append(schms[nn])
    if len(matchingSchema)>1:
      print('  ERROR: Multiple matching schemas for worker {0:s}'.format(wrk))
      sys.exit()
    elif len(matchingSchema)==0:
      print('  ERROR: No matching schemas for worker {0:s}'.format(wrk))
      sys.exit()
    else: matchingSchema=matchingSchema[0]
    print('        {0:s}{1:s}'.format(schDr,matchingSchema))
    
    # write worker description
    schm=ruamel.yaml.load(open(schDr+matchingSchema), ruamel.yaml.RoundTripLoader)['mapping'][wrk]
    f.write('{0:s}\n\n'.format(schm['desc']))

    # the initial type must be 'map'
    # if that's the case proceed reading the parameters
    # if that's not the case throw an error
    if 'type' in schm and schm['type']=='map':
      schm=schm['mapping']
    else:
      print('  ERROR: type of {0:s} in {1:s} is not "mapping", or no type was found; no idea what to do with this!'.format(wrk,matchingSchema))
      sys.exit()

    # enter the nested structure of the schema and get all relevant info
    for parLev1 in schm:
      f.write('\n\n')
      f.write('.. _{0:s}_{1:s}:\n'.format(wrk,parLev1))
      f.write('\n')
      f.write('--------------------------------------------------\n')
      f.write('**{0:s}**\n'.format(parLev1))
      f.write('--------------------------------------------------\n\n')
      writeWorkerLevel(f,schm[parLev1],indentLevel='  ')

      if 'type' in schm[parLev1] and schm[parLev1]['type']=='map':
        for parLev2 in schm[parLev1]['mapping']:
          f.write('  **{0:s}**\n\n'.format(parLev2))
          writeWorkerLevel(f,schm[parLev1]['mapping'][parLev2],indentLevel='    ')

          if 'type' in schm[parLev1]['mapping'][parLev2] and schm[parLev1]['mapping'][parLev2]['type']=='map':
            for parLev3 in schm[parLev1]['mapping'][parLev2]['mapping']:
              f.write('    **{0:s}**\n\n'.format(parLev3))
              writeWorkerLevel(f,schm[parLev1]['mapping'][parLev2]['mapping'][parLev3],indentLevel='      ')

              if 'type' in schm[parLev1]['mapping'][parLev2]['mapping'][parLev3] and schm[parLev1]['mapping'][parLev2]['mapping'][parLev3]['type']=='map':
                for parLev4 in schm[parLev1]['mapping'][parLev2]['mapping'][parLev3]['mapping']:
                  f.write('      **{0:s}**\n\n'.format(parLev4))
                  writeWorkerLevel(f,schm[parLev1]['mapping'][parLev2]['mapping'][parLev3]['mapping'][parLev4],indentLevel='        ')

    f.close()



######################
### IMPORT MODULES ###
######################

from glob import glob
import os
import sys
import ruamel.yaml
import shutil



##############
### DO IT! ###
##############

schemaDir=caracalDir+'/caracal/schema/'

print('  INFO: Browsing Caracal schema directory {0:s} ...'.format(schemaDir))
# Get list of workers and their .yml files from the schema directory
schemas=[ww.replace(schemaDir,'') for ww in glob(schemaDir+'*.yml')]
workers=[ww.split('_schema')[0] for ww in schemas]

# Remove from the sortedWorkers list all workers not in the current schema directory
nw=0
while nw<len(sortedWorkers):
  if sortedWorkers[nw] in workers: nw+=1
  else: del(sortedWorkers[nw])

# Add to the sortedWorkers list any missing worker found in the schema directory
for ww in workers:
  if ww not in sortedWorkers: sortedWorkers.append(ww)

# Write out message about which workers will be included in the docs
print('  INFO: Will include the following workers in the documentation:')
for ww in sortedWorkers: print('        {0:s}'.format(ww))

# For each docs (sub-)directories
print('  INFO: Creating "Download & Install" and "Workers" (sub-)directories ...')
[installDir,workersDir]=[docsDir+dd for dd in ['install','manual/workers']]
for dd in [installDir,workersDir]:
  shutil.rmtree(dd, ignore_errors=True)
  os.mkdir(dd)
[installDir,workersDir]=[docsDir+dd+'/' for dd in ['install','manual/workers']]

for ww in sortedWorkers: os.mkdir(workersDir+ww)

# Copy Caracal readme from caracalDir and edit 
getCaracalReadme(caracalDir,docsDir)

# Write index.rst files
writeWorkerPageIndex(sortedWorkers,workersDir)
writeWorkersIndex(sortedWorkers,workersDir,schemas,schemaDir)

print('  INFO: Done, all good!')
print('  INFO: Now type the following:')
print('        cd sphinx')
print('        make html')
print('        cd ../')
print('  INFO: You can now commit and push your changes to GitHub, and you are done!')

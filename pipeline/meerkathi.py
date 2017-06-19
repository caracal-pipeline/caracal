from  argparse import ArgumentParser
import sys
import os
pckgdir = os.path.dirname(__file__)

def main(argv):
    parser = ArgumentParser(description='MeerKAT HI pipeline : https://github.com/sphemakh/meerkathi \n \
Options set on the command line will overwrite options in the --pipeline-configuration file')
    add = parser.add_argument

    add('-pc', '--pipeline-configuration', 
help='Pipeline configuarion file (YAML/JSON format)')

    add('--id', '--input', 
help='Pipeline input directory')

    add('--od', '--output', 
help='Pipeline output directory')

    add('--od', '--msdir',
help='Pipeline MS directory. All MSs, for a given pipeline run, should/will be placed here')

    add('-wd', '--workers-directory', default='{:s}/workers'.format(pckgdir),
help='Directory where pipeline workers can be found. These are stimela recipes describing the pipeline')

    add('-dp', '--data-path', action='append',
help='Path where data can be found. This is where the file <dataid>.h5 should be located. Can be specified multiple times if --dataid(s) have different locations')

    add('-id', '--dataid', action='append',
help='Data ID of hdf5 file to be reduced. May be specified muliple times. Must be used in combination with --data-path')

    add('-p', '--prefix', default='meerkathi-pipeline',
help='Prefix for pipeline output products')

    add('-ra', '--reference-antenna', action='append',
help='Reference antenna. Can be specified multiple times if reference antenna is different for different --dataid(s)')

    add('-bc', '--bandpass-cal', 
help='Name or field ID of Bandpass calibrator source/field. Can be specified multiple times if different for different --dataid(s)')
    
    add('-bc', '--bandpass-cal', 
help='Name or field ID of gain calibrator source/field. Can be specified multiple times if different for different --dataid(s)')
 
    add('-t', '--target', 
help='Name or field ID of target field. Can be specified multiple times if different for different --dataid(s)')

    args = parser.parse_args(argv)

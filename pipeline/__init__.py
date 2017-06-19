import yaml

class MeerKATHI(object):
    def __init__(self, config):
        self.config = yaml.load(open(config))

        self.msdir = config['msdir']
        self.input = config['input']
        self.output = config['output']
        
        self.dataids = self.config['dataids']
        self.prefix = self.config['prefix']
        self.dataids = self.config['dataids']
        self.label = label or self.config.get('label', 'meerkathi')

        self.h5files = ['{:s}.h5'.format(dataid) for dataid in self.dataids]
        self.msnames = ['{:s}.ms'.format(os.path.basename(dataid)) for dataid in self.dataids]
        self.split_msnames = ['{:s}_split.ms'.format(os.path.basename(dataid)) for dataid in self.dataids]
        self.cal_msnames = ['{:s}_cal.ms'.format(os.path.basename(dataid)) for dataid in self.dataids]
        self.prefixes = ['meerkathi-{:s}'.format(os.path.basename(dataid)) for dataid in self.dataids]

        self.get_data = stimela.Recipe('Get data and do basic fixes', ms_dir=self.msdir)
        self.preflag = stimela.Recipe('Preflagging data', ms_dir=self.msdir)
        self.first_gen = stimela.Recipe('First generation calibration', ms_dir=self.msdir)
        self.second_gen = stimela.Recipe('Second generation calibration', ms_dir=self.msdir)
        self.contsub = stimela.Recipe('Continuum subtraction', ms_dir=self.msdir)
        self.imaging = stimela.Recipe('HI imaging', ms_dir=self.msdir)

	self.nobs = len(self.dataids)

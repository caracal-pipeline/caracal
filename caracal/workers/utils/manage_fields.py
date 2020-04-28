# -*- coding: future_fstrings -*-

def get_field(pipeline, i, field):
    """
        gets field ids parsed previously in the pipeline
        params:
            field: list of ids or comma-seperated list of ids where
                   ids are in bpcal, gcal, target, fcal or an actual field name
    """
    return ','.join(filter(lambda s: s != "", map(lambda x: ','.join(getattr(pipeline, x)[i].split(',')
                                                                     if isinstance(getattr(pipeline, x)[i], str) and getattr(pipeline, x)[i] != "" else getattr(pipeline, x)[i])
                                                  if x in ['bpcal', 'gcal', 'target', 'fcal', 'xcal']
                                                  else x.split(','),
                                                  field.split(',') if isinstance(field, str) else field)))

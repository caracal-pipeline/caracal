import os
import sys
import subprocess
import itertools
import meerkathi
import meerkathi.dispatch_crew.meerkat_archive_interface as mai
import stimela.dismissable as sdm
import warnings

NAME = "Get convert and extract data"

def worker(pipeline, recipe, config):
    # this bit is intrinsic to meerkathi because it changes the pipeline input parameters... cannot be run in stimela
    if pipeline.enable_task(config, 'meerkat_query_available'):
        cfg = config["meerkat_query_available"]
        if cfg["poll_mode"] == "override":
            meerkathi.log.info("User specified MeerKAT filenames manually. Will now contact MeerKAT archive for their metadata...")
            dids = config["dataid"] if isinstance(config["dataid"], list) else [config["dataid"]]
            mdata = []
            pipeline.metadata = []
            for did in dids:
                mdata = \
                itertools.chain(mdata, mai.query_metadatas(pipeline.data_path,
                                                           cfg["query_url"],
                                                           filename="".join([did, ".h5"]),
                                                           product_type_name=None,
                                                           product_num_channels=None,
                                                           time_extent=None,
                                                           required_intents=[],
                                                           required_minimum_duration=0,
                                                           required_fields=[],
                                                           required_description=".*"))
            for m in mdata:
                bn = os.path.splitext(os.path.basename(m["Filename"]))[0]
                mai.dump_observation_metadata(pipeline.data_path,
                                              bn + '.json',
                                              m)
                pipeline.metadata.append('{0:s}/{1:s}.json'.format(pipeline.data_path, bn))
            pipeline.init_names(config["dataid"])
        elif cfg["poll_mode"] == "poll":
            meerkathi.log.info("Polling the MeerKAT archive for the latest imaging observations...")
            mdata = mai.query_metadatas(pipeline.data_path,
                                        cfg["query_url"],
                                        filename=None,
                                        product_type_name=cfg["product_type_name"],
                                        product_num_channels=cfg["product_num_channels"],
                                        time_extent=cfg["when_observed"],
                                        required_intents=cfg["required_intents"],
                                        required_minimum_duration=cfg["minimum_observation_duration"],
                                        required_fields=cfg["required_fields"],
                                        required_description=cfg["description_matches"])
            for m in mdata:
                bn = os.path.splitext(os.path.basename(m["Filename"]))[0]
                mai.dump_observation_metadata(pipeline.data_path,
                                              bn + '.json',
                                              m)
            pipeline.init_names([os.path.splitext(os.path.basename(m["Filename"]))[0] for m in mdata])

            if pipeline.nobs == 0:
                meerkathi.log.warn("The archive says there is no new data matching your search parameters. "
                                   "Try adjusting the search parameters. There is no work left to do.")
                sys.exit(0)
        else:
            raise ValueError("MK Archive poll_mode only accepts 'override' and 'poll'. Check your config!")
    else: # not going to query MeerKAT archive for h5 file metadata.... user specifying MS directly
        meerkathi.log.warn("User specified dataset names manually and chose not to query the archive. "
                           "If this is MeerKAT data you better have metadata downloaded already!")
        pipeline.init_names(config["dataid"])

    for i in range(pipeline.nobs):
        msname = pipeline.msnames[i]
        h5file = pipeline.h5files[i]
        basename = os.path.splitext(os.path.basename(h5file))[0]
        prefix = pipeline.prefixes[i]
        if isinstance(pipeline.data_path, list):
            data_path = pipeline.data_path[i]
        else:
            data_path = pipeline.data_path

        if pipeline.enable_task(config, 'download'):
            cfg = config['download']
            if isinstance(cfg["data_url"], list):
                data_url = cfg["data_url"][i]
            else:
                data_url = cfg["data_url"]
            dm = cfg['download_mode'].upper()
            if dm == "MEERKAT":
                step = 'download_{:d}'.format(i)
                def checked_download(h5file, basename):
                    (data_url != "") and meerkathi.log.warn("Download mode set to MeerKAT, but user specified a data-url. This will be ignored")
                    if not os.path.exists(os.path.join(pipeline.data_path, basename + '.json')): # user is being silly... download metadata anyway
                        mdata = mai.query_metadatas(pipeline.data_path,
                                            config["meerkat_query_available"]["query_url"],
                                            filename=h5file,
                                            product_type_name=None,
                                            product_num_channels=None,
                                            time_extent=None,
                                            required_intents=[],
                                            required_minimum_duration=0,
                                            required_fields=[],
                                            required_description=".*")

                        if len(mdata) == 0:
                            raise RuntimeError("Invalid product specified. You may want to run the meerkat_archive_query step or double check your dataid.")
                        meta = mdata[0]
                    else:
                        meta = mai.load_observation_metadata(pipeline.data_path,
                                                             basename + '.json')
                    if not mai.check_observation_cache(pipeline.data_path,
                                                       h5file,
                                                       meta):
                        mai.download_observations(pipeline.data_path, [meta])
                # add function to recipe
                recipe.add(checked_download, step, 
                      {
                       "h5file"    : h5file,
                       "basename"  : basename,
                      },
                    label='{0:s}:: Downloading MeerKAT data'.format(step))

            elif dm == "MANUAL":
                step = 'download_{:d}'.format(i)
                if os.path.exists('{0:s}/{1:s}'.format(pipeline.data_path, msname)) \
                    and not config['download'].get('reset', False):
                    meerkathi.log.warn('ms already exists, and reset is not enabled. Will attempt to resume')
                    recipe.add('cab/curl', step, {
                        "url"   : data_url,
                        "output": msname if config['download'].get('untar', False) else msname + '.tar',
                        "continue-at": "-"
                    },
                    input=pipeline.input,
                    output=pipeline.data_path,
                    label='{0:s}:: Downloading data'.format(step))
                else:
                    os.system('rm -rf {0:s}/{1:s}'.format(pipeline.data_path, msname))

                    recipe.add('cab/curl', step, {
                        "url"   : data_url,
                        "output": msname if config['download'].get('untar', False) else msname + '.tar',
                    },
                    input=pipeline.input,
                    output=pipeline.data_path,
                    label='{0:s}:: Downloading data'.format(step))


        if pipeline.enable_task(config, 'h5toms'):
            step = 'h5toms_{:d}'.format(i)

            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            recipe.add('cab/h5toms', step,
                {
                    "hdf5files"     : [h5file],
                    "output-ms"     : msname,
                    "no-auto"       : False,
                    "tar"           : True,
                    "model-data"    : True,
                    "channel-range" : sdm.dismissable(config['h5toms'].get('channel_range', None)),
                    "full-pol"      : config['h5toms'].get('full_pol', False),
                },
                input=data_path,
                output=pipeline.output,
                label='{0:s}:: Convert hd5file to MS. ms={1:s}'.format(step, msname))


    if pipeline.enable_task(config, 'combine'):
        step = 'combine_data'
        newid = config['combine']['newid']
        msnames = pipeline.msnames
        metadata = pipeline.metadata[0]
        pipeline.init_names([newid])
        msname = pipeline.msnames[0]
        pipeline.metada = [metadata]

        if config['combine'].get('reset', True):
            if os.path.exists('{0:s}/{1:s}'.format(pipeline.msdir, msname)):
                os.system('rm -rf {0:s}/{1:s}'.format(pipeline.msdir, msname))

            recipe.add('cab/casa_concat', step, 
                {
                    "vis"       : msnames,
                    "concatvis" : msname,
                },
                input=pipeline.input,
                output=pipeline.output,
                label='{0:s}:: Combine datasets'.format(step))

        if config['combine'].get('tar', True):
            step = 'tar_{:d}'.format(i)
            tar_options = config['combine'].get('tar_options', 'cvf')
            # Function to untar Ms from .tar file
            def tar(ms):
                mspath = os.path.abspath(pipeline.msdir)
                subprocess.check_call(['tar', tar_options,
                    os.path.join(mspath, ms+'.tar'),
                    os.path.join(mspath, ms),
                    ])
            # add function to recipe
            recipe.add(tar, step, 
                 {
                  "ms"        : msname,
                 },
                 label='{0:s}:: Get MS from tarbal ms={1:s}'.format(step, msname))

    for i, msname in enumerate(pipeline.msnames):
        if pipeline.enable_task(config, 'untar'):
                step = 'untar_{:d}'.format(i)
                tar_options = config['untar'].get('tar_options', 'xvf')
                # Function to untar Ms from .tar file
                def untar(ms):
                    mspath = os.path.abspath(pipeline.msdir)
                    subprocess.check_call(['tar', tar_options,
                        os.path.join(mspath, ms+'.tar'),
                        '-C', mspath])
                # add function to recipe
                recipe.add(untar, step, 
                     {
                      "ms"        : msname,
                     },
                     label='{0:s}:: Get MS from tarbal ms={1:s}'.format(step, msname))

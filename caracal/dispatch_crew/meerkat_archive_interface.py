import collections
import itertools
import json
import os
import sys
import caracal
import pysolr
import requests
import progressbar
import re


def __standard_observation_query(filename=None,
                                 product_type_name="MeerKATAR1TelescopeProduct",
                                 product_num_channels=4096,
                                 time_extent="[NOW-3DAYS TO NOW]"):
    """ Constructs SOLR query based on arguments
        Specify None to remove criteria
        Returns SOLR query string
    """
    # Create a quick field+query type
    fq = collections.namedtuple('field_query', 'field query')

    query_list = [
        # Product name if specified
        fq('Filename', filename),
        # Only want MeerKAT AR1 Telescope Products
        fq('CAS.ProductTypeName', product_type_name),
        # Only want 4K mode
        fq('NumFreqChannels', product_num_channels),
        # Observations from the last 3 days
        fq('StartTime', time_extent)]

    # Construct the query
    query = ' AND '.join('%s:%s' % (fq.field, fq.query)
                         for fq in query_list if fq.query is not None)
    return query


def __query_filter(solr_url,
                   query=None,
                   required_intents=['gaincal', 'bpcal', 'target'],
                   required_minimum_duration=2,
                   required_fields=[],
                   required_description='.*'):
    """
    Find recent telescope observations suitable for imaging
    """

    ONE_MINUTE = 60
    ONE_HOUR = 60 * ONE_MINUTE

    search = query if query is not None else __standard_observation_query()

    caracal.log.info("Querying solr server '%s' "
                       "with query string '%s'." % (solr_url, search))

    archive = pysolr.Solr(solr_url)

    def _observation_filter(solr_result):
        """
        Filter out KAT observations that don't cut the mustard
        for imaging purposes.
        """

        # Create an observation string for logging
        filename, description = (solr_result.get(f, '') for f in
                                 ('Filename', 'Description'))
        observation = '{} {}'.format(filename, description[:50])

        # Its only worth imaging observations with
        # gain and bandpass calibrations
        targets = solr_result.get('KatpointTargets', [])
        gaincals = sum('gaincal' in s for s in targets)
        bandpasses = sum('bpcal' in s for s in targets)
        obs_targets = sum('target' in s for s in targets)

        if ('gaincal' in required_intents) and gaincals == 0:
            caracal.log.warn('Ignoring "{}", no gain calibrators '
                               'are present.'.format(observation))
            return False

        if ('bpcal' in required_intents) and bandpasses == 0:
            caracal.log.warn('Ignoring "{}", no band pass calibrators '
                               'are present.'.format(observation))
            return False

        if ('target' in required_intents) and obs_targets == 0:
            caracal.log.warn('Ignoring "{}", no target fields '
                               'are present.'.format(observation))
            return False

        # Check that the description matches
        prod_desc = solr_result.get('Description', '')
        if not re.match(required_description, prod_desc):
            caracal.log.warn('Ignoring "{}", observation description doesn\'t match '
                               'user query Regex.'.format(observation))
            return False

        # Check that required fields have been observed
        obs_fields = solr_result.get('Targets', [])
        for f in required_fields:
            if f not in obs_fields:
                caracal.log.warn('Ignoring "{}", observation doesn\'t contain '
                                   'field "{}" as required by user.'.format(observation,
                                                                            f))
                return False

        # Don't take anything less than 2 hours in duration
        duration = solr_result['Duration']

        if duration < required_minimum_duration * ONE_HOUR:
            caracal.log.warn('Ignoring "{}", observation is '
                               'only "{}"s long, need to be at minimum "{}"s.'.format(observation,
                                                                                      duration,
                                                                                      required_minimum_duration * ONE_HOUR))
            return False

        return True
    hits = []
    curr_cursor = "*"
    res = archive.search(
        search, sort='ProductName desc, id asc', rows=1000, cursorMark='*')
    # Step through query results - may be many pages
    while curr_cursor != res.nextCursorMark:
        curr_cursor = res.nextCursorMark
        hits += list(filter(_observation_filter, res))
        res = archive.search(
            search, sort='ProductName desc, id asc', rows=1000, cursorMark=curr_cursor)

    return hits


def load_observation_metadata(directory, filename):
    """ Load observation metadata """

    with open(os.path.join(directory, filename), 'r') as f:
        return json.load(f)


def dump_observation_metadata(directory, filename, observation):
    """ Dump observation metadata """

    with open(os.path.join(directory, filename), 'w') as f:
        return json.dump(observation, f)


def query_metadatas(input_dir,
                    solr_url,
                    filename=None,
                    product_type_name="MeerKATAR1TelescopeProduct",
                    product_num_channels=4096,
                    time_extent="[NOW-3DAYS TO NOW]",
                    required_intents=['gaincal', 'bpcal', 'target'],
                    required_minimum_duration=2,
                    required_fields=[],
                    required_description='.*'):
    """ Return a list of observation metadatas """

    # If a specific HDF5 file is specified, attempt to load local metadata first
    results = []
    if filename is not None:
        if isinstance(filename, str):
            lstfiles = [filename]
        elif isinstance(filename, list):
            lstfiles = filename
        else:
            raise ValueError("Expected filename list")
        for f in lstfiles:
            query = __standard_observation_query(filename=f,
                                                 product_type_name=product_type_name,
                                                 product_num_channels=product_num_channels,
                                                 time_extent=time_extent)

            basename = os.path.splitext(f)[0]
            metadata_filename = ''.join([basename, '.json'])
            metadata_path = os.path.join(input_dir, metadata_filename)

            # If it exists, load and return the metadata as our observation object
            if os.path.exists(metadata_path):
                caracal.log.info("Observation metadata exists locally at '{}'. "
                                   "Reusing it.".format(metadata_path))

                results = itertools.chain(results, [load_observation_metadata(input_dir,
                                                                              metadata_filename)])
            else:
                # Nope, need to download it from the server
                results = itertools.chain(results, __query_filter(solr_url,
                                                                  query,
                                                                  required_intents,
                                                                  required_minimum_duration,
                                                                  required_fields,
                                                                  required_description))

    else:
        query = __standard_observation_query(filename=None,
                                             product_type_name=product_type_name,
                                             product_num_channels=product_num_channels,
                                             time_extent=time_extent)

        results = itertools.chain(results, __query_filter(solr_url,
                                                          query,
                                                          required_intents,
                                                          required_minimum_duration,
                                                          required_fields,
                                                          required_description))
    return list(results)


def download_observations(directory, observations):
    """ Download the specified observations
        args:
        : observations: list of observation metadata
        returns: list of filenames downloaded
    """

    ONE_KB = 1024
    ONE_MB = ONE_KB*ONE_KB
    filenames = []
    for observation in observations:
        targets = observation.get('KatpointTargets', [])
        gaincals = sum('gaincal' in s for s in targets)
        bandpasses = sum('bpcal' in s for s in targets)
        obs_targets = sum('target' in s for s in targets)

        caracal.log.info('%s -- %-2d -- %-2d-- %-2d-- %s -- %s -- %s' % (
            observation['Filename'], gaincals, bandpasses, obs_targets,
            observation['StartTime'], observation['Description'], observation['Duration']))

        # Infer the HTTP location from the KAT archive file location
        location = observation['FileLocation'][0]
        location = location.replace(
            '/var/kat', 'http://kat-archive.kat.ac.za', 1)
        filename = observation['Filename']
        url = os.path.join(location, filename)

        filename = os.path.join(directory, filename)
        file_exists = os.path.exists(filename) and os.path.isfile(filename)
        local_file_size = os.path.getsize(filename) if file_exists else 0
        headers = {"Range": "bytes={}-".format(local_file_size)}

        r = requests.get(url, headers=headers, stream=True)

        # Server doesn't care about range requests and is just
        # sending the entire file
        if r.status_code == 200:
            caracal.log.info("Downloading '{}'")
            remote_file_size = r.headers.get('Content-Length', None)
            file_exists = False
            local_file_size = 0
        elif r.status_code == 206:
            if local_file_size > 0:
                caracal.log.info("'{}' already exists, "
                                   "resuming download from {}.".format(
                                       filename, local_file_size))

            # Create a fake range if none exists
            fake_range = "{}-{}/{}".format(local_file_size, sys.maxsize,
                                           sys.maxsize - local_file_size)

            remote_file_size = r.headers.get('Content-Range', fake_range)
            remote_file_size = remote_file_size.split('/')[-1]
        elif r.status_code == 416:
            caracal.log.info("'{}' already downloaded".format(filename))
            remote_file_size = local_file_size
            return filename
        else:
            raise ValueError("HTTP Error Code {}".format(r.status_code))

        caracal.log.info('%s %s %s' % (url, remote_file_size, r.status_code))

        f = open(filename, 'ab' if file_exists else 'wb')
        bar = (progressbar.ProgressBar(max_value=progressbar.UnknownLength)
               if remote_file_size is None else progressbar.ProgressBar(
            maxval=int(remote_file_size)))

        # Download chunks of file and write to disk
        try:
            with f, bar:
                downloaded = local_file_size
                for chunk in r.iter_content(chunk_size=ONE_MB):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        bar.update(downloaded)
        except KeyboardInterrupt as kbe:
            pass
            #log.warn("Quitting download on Keyboard Interrupt")
        filenames.append(filename)

    return filename


def check_observation_cache(directory, h5filename, observation):
    """
    Return True if both an h5file and the associated observation metadata
    exist. The file size indicated in the metadata must agree with that
    of the h5file.
    """

    # Does the h5 file exist?
    h5file = os.path.join(directory, h5filename)

    if not os.path.exists(h5file):
        return False

    # Compare metadata file size vs h5 file size
    h5_size = os.path.getsize(h5file)
    obs_size = observation["FileSize"][0]

    if not obs_size == os.path.getsize(h5file):
        caracal.log.warn("'{}' file size '{}' "
                           "differs from that in the observation metadata '{}'."
                           .format(h5filename, h5_size, obs_size))
        return False

    return True

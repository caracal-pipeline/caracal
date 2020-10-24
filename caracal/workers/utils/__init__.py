import os

def remove_output_products(files, directory=None, log=None):
    """
    Removes output products (given by a list of files), in a directory (if specified)
    """
    for fullpath in files:
        if directory:
            fullpath = os.path.join(directory, fullpath)
        if os.path.exists(fullpath):
            if log is not None:
                log.info(f'removing pre-existing {fullpath}')
            os.system(f'rm -rf {fullpath}')

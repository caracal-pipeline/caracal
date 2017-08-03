import meerkathi.dispatch_crew.catalog_parser as cp
import meerkathi
import os

__DB_FILENAME = os.path.join(meerkathi.pckgdir, "data/southern_calibrators.txt")

__CALIBRATOR_DB = None

def calibrator_database():
    """ Return the Southern standard calibrator database """

    global __CALIBRATOR_DB

    # Do a lazy load
    if __CALIBRATOR_DB is not None:
        return __CALIBRATOR_DB

    # OK its not loaded, read it in

    # There isn't a Southern standard in CASA
    # so construct a little database of them for reference
    meerkathi.log.info("Obtaining divine knowledge from: %s" % __DB_FILENAME)

    __CALIBRATOR_DB = cp.catalog_parser(__DB_FILENAME)

    return __CALIBRATOR_DB


import caracal.dispatch_crew.catalog_parser as cp
import caracal
import os

__DB_FILENAME = os.path.join(
    caracal.pckgdir, "data/southern_calibrators.txt")
__DB_CASA_FILENAME = os.path.join(
    caracal.pckgdir, "data/casa_calibrators.txt")

__CALIBRATOR_DB = None
__CASA_CALIBRATOR_DB = None


def calibrator_database():
    """ Return the Southern standard calibrator database """

    global __CALIBRATOR_DB

    # Do a lazy load
    if __CALIBRATOR_DB is not None:
        return __CALIBRATOR_DB

    # OK its not loaded, read it in

    # There isn't a Southern standard in CASA
    # so construct a little database of them for reference
    caracal.log.info("Obtaining divine knowledge from %s" % __DB_FILENAME)

    __CALIBRATOR_DB = cp.catalog_parser(__DB_FILENAME)
    # caracal.log.info("\n" + str(__CALIBRATOR_DB))
    return __CALIBRATOR_DB


def casa_calibrator_database():
    """ Return the CASA standard calibrator database """

    # same as in calibrator_database
    global __CASA_CALIBRATOR_DB
    if __CASA_CALIBRATOR_DB is not None:
        return __CASA_CALIBRATOR_DB
    caracal.log.info("Obtaining divine knowledge from %s" % __DB_CASA_FILENAME)
    __CASA_CALIBRATOR_DB = cp.catalog_parser(__DB_CASA_FILENAME)
    return __CASA_CALIBRATOR_DB

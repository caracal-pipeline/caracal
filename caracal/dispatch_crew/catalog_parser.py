import re
import numpy as np
import copy
from caracal.utils.requires import extras


class catalog_parser:
    def __init__(self, filename):
        """
            The all-knowning catalog class
            Give me a filename and I shall pass on divine knowledge
        """
        cls = self.__class__
        self._cat = cls.read_caltable(filename)

    # Comment by Josh: @property is a means to protect private
    # Variables
    # if a is an instance of catalog_parser, then the expression
    # a.db is transformed into a.db(). So it looks like a class
    # variable but it isn't. Something nasty like
    # a.db = XXX is then impossible.
    @property
    def db(self):
        """ Returns a copy of divine sky knowledge """
        return copy.deepcopy(self._cat)

    def __str__(self):
        """ Return multiline string describing the calibrator database """
        lines = [""]
        lines.extend(["\t%s\tEpoch:%d\tRA:%3.2f\tDEC:%3.2f\t"
                      "S_v0:%.4f\tv0:%.4e\ta:%.4f\tb:%.4f\tc:%.4f\td:%.4f\t"
                      "lsm:%s\tlsm epoch:%d" %
                      (str(name).ljust(15),
                       db["epoch"],
                       db["ra"],
                       db["decl"],
                       db["S_v0"],
                       db["v0"],
                       db["a_casa"],
                       db["b_casa"],
                       db["c_casa"],
                       db["d_casa"],
                       db.get("lsm", "<none>").ljust(30),
                       db.get("lsm_epoch", db["epoch"]))
                      for name, db in self._cat.items()])
        return '\n'.join(lines)

    @classmethod
    def read_caltable(cls, filename):
        """
        Read calibrator database (specified in MHz)
        and returns a dictionary containing the following
        :filename: filename of caltable database
        :returns: for every source (name = key):
                    Epoch, RA, Declination,
                    a_ghz, b_ghz, c_ghz, d_ghz,
                    a_mhz, b_mhz, c_mhz, d_mhz,
                    S_v0, a_casa, b_casa, c_casa, d_casa, v0
        :side-effects: none
        """
        calibrator_db = {}
        with open(filename) as f:
            line = f.readline()
            ln_no = 1
            while line:
                # discard comments
                command = line.split("//")[0]

                # empty line ?
                if command.strip() == "":
                    line = f.readline()
                    ln_no += 1
                    continue
                cmd = None
                # source ?
                valset = re.match(r"^name=(?P<name>[0-9A-Za-z\-+_ ]+)[ ]+"
                                  r"epoch=(?P<epoch>[0-9]+)[ ]+"
                                  r"ra=(?P<ra>[+\-]?[0-9]+h[0-9]+m[0-9]+(?:.[0-9]+)?s)[ ]+"
                                  r"dec=(?P<decl>[+\-]?[0-9]+d[0-9]+m[0-9]+(?:.[0-9]+)?s)[ ]+"
                                  r"a=(?P<a>[+\-]?[0-9]+(?:.[0-9]+)?)[ ]+"
                                  r"b=(?P<b>[+\-]?[0-9]+(?:.[0-9]+)?)[ ]+"
                                  r"c=(?P<c>[+\-]?[0-9]+(?:.[0-9]+)?)[ ]+"
                                  r"d=(?P<d>[+\-]?[0-9]+(?:.[0-9]+)?)$",
                                  command)
                # else alias ?
                if not valset:
                    valset = re.match(r"^alias src=(?P<src>[0-9A-Za-z\-+_ ]+)[ ]+"
                                      r"dest=(?P<dest>[0-9A-Za-z\-+_ ]+)$",
                                      command)
                    # else lsm?
                    if not valset:
                        valset = re.match(r"^lsm name=(?P<src>[0-9A-Za-z\-+_ ]+)[ ]+"
                                          r"epoch=(?P<epoch>[0-9]+)[ ]+"
                                          r"(?P<lsmname>[0-9a-zA-Z\-.]+)$",
                                          command)
                        if not valset:
                            valset = re.match(r"^crystal name=(?P<src>[0-9A-Za-z\-+_ ]+)[ ]+"
                                              r"epoch=(?P<epoch>[0-9]+)[ ]+"
                                              r"(?P<lsmname>[0-9a-zA-Z\-.]+)$",
                                              command)

                            if not valset:
                                raise RuntimeError("Illegal line encountered while parsing"
                                                   "southern standard at line %d:'%s'" %
                                                   (ln_no, line))
                            else:
                                cmd = "crystal"
                        else:
                            cmd = "lsm"
                    else:
                        cmd = "alias"
                else:
                    cmd = "add"

                if cmd == "add":
                    # parse sources (spectra in MHz)
                    name = valset.group("name").strip()
                    epoch = int(valset.group("epoch"))
                    ra = valset.group("ra")
                    valset_ra = re.match(r"^(?P<h>[+\-]?[0-9]+)h"
                                         r"(?P<m>[0-9]+)m"
                                         r"(?P<s>[0-9]+(?:.[0-9]+)?)s$",
                                         ra)
                    ra = np.deg2rad((float(valset_ra.group("h")) +
                                     float(valset_ra.group("m")) / 60.0 +
                                     float(valset_ra.group("s")) / 3600) / 24.0 * 360)
                    decl = valset.group("decl")
                    valset_decl = re.match(r"^(?P<d>[+\-]?[0-9]+)d"
                                           r"(?P<m>[0-9]+)m"
                                           r"(?P<s>[0-9]+(?:.[0-9]+)?)s$",
                                           decl)

                    signum = 1.
                    if decl[0] == '-':
                        signum = -1.
                    decl = np.deg2rad(float(valset_decl.group("d")) +
                                      signum * float(valset_decl.group("m")) / 60. +
                                      signum * float(valset_decl.group("s")) / 3600.)

                    a = float(valset.group("a"))
                    b = float(valset.group("b"))
                    c = float(valset.group("c"))
                    d = float(valset.group("d"))

                    # convert models to Perley Butler GHz format
                    k = np.log10(1000)
                    ag = a + (b * k) + (c * k ** 2) + (d * k ** 3)
                    bg = b + (2 * c * k) + (3 * d * k ** 2)
                    cg = c + (3 * d * k)
                    dg = d

                    # convert model components to CASA/MT format
                    s_v0, a_casa, b_casa, c_casa, d_casa = cls.convert_pb_to_casaspi(0.8, 1.8, 1.4,
                                                                                     ag, bg, cg, dg)
                    calibrator_db[name] = {"epoch": epoch, "ra": ra, "decl": decl,
                                           "a_ghz": ag, "b_ghz": bg, "c_ghz": cg, "d_ghz": dg,
                                           "a_mhz": a, "b_mhz": b, "c_mhz": c, "d_mhz": d,
                                           "S_v0": s_v0,
                                           "a_casa": a_casa, "b_casa": b_casa,
                                           "c_casa": c_casa, "d_casa": d_casa,
                                           "v0": 1.4e9}
                elif cmd == "alias":
                    src = valset.group("src")
                    dest = valset.group("dest")
                    if src not in calibrator_db:
                        raise RuntimeError("%s has not been defined. Cannot alias "
                                           "%s to %s in line %d" %
                                           (src, dest, src, ln_no))
                    calibrator_db[dest] = calibrator_db[src]
                elif cmd == "lsm":
                    src = valset.group("src")
                    epoch = valset.group("epoch")
                    lsm = valset.group("lsmname")
                    if src not in calibrator_db:
                        raise RuntimeError("%s has not been defined. Cannot link lsm "
                                           "%s to %s in line %d" %
                                           (src, lsm, ln_no))
                    calibrator_db[name]["lsm"] = lsm
                    calibrator_db[name]["lsm_epoch"] = int(epoch)
                elif cmd == "crystal":
                    src = valset.group("src")
                    epoch = valset.group("epoch")
                    crystal = valset.group("lsmname")
                    if src not in calibrator_db:
                        raise RuntimeError("%s has not been defined. Cannot link to crystalball model"
                                           "%s to %s in line %d" %
                                           (src, crystal, ln_no))
                    calibrator_db[name]["crystal"] = crystal
                    calibrator_db[name]["lsm_epoch"] = int(epoch)
                else:
                    raise RuntimeError(
                        "Invalid command processed. This is a bug")

                # finally parse next line
                line = f.readline()
                ln_no += 1

            return calibrator_db

    @classmethod
    def convert_pb_to_casaspi(cls, vlower, vupper, v0, a, b, c, d):
        """
        Coverts between the different conventions:
        PB: 10 ** [a + b * log10(v) + c * log10(v) ** 2 + d * log10(v) ** 3]
        CASA/Meqtrees SPI: S(v0) * (v/v0) ** [a' + b'*log10(v/v0) + c'*log10(v/v0) ** 2 + d'*log10(v/v0) ** 3]

        args:
        :vlower, vupper: range (same unit as a, b, c, d coefficients!) to fit for a',b',c',d'
        :v0: reference frequency (same unit as vlower, vupper!)
        :a,b,c,d: PB coefficients (for the unit used in vlower, vupper and v0!)

        side-effects: none
        """
        if vlower > vupper:
            raise ValueError("vlower must be lower than vupper")

        def pbspi(v, a, b, c, d):
            return 10 ** (a + b * np.log10(v) + c * np.log10(v) ** 2 + d * np.log10(v) ** 3)

        def casaspi(v, v0, I, a, b, c, d):
            return I * (v / v0) ** (a + b * np.log10(v / v0) + c * np.log10(v / v0) ** 2 + d * np.log10(v / v0) ** 3)

        I = pbspi(v0, a, b, c, d)

        if a == 0 and b == 0 and c == 0 and d == 0:
            popt = [0., 0., 0., 0.]
        else:
            # Wrap in useless function to hide scipy
            # Importing midway is non-kosher, but what you gonna do ¯\_('_')_/¯
            @extras("scipy.optimize")
            def needs_curve_fit():
                from scipy.optimize import curve_fit
                v = np.linspace(vlower, vupper, 10000)
                popt, pcov = curve_fit(lambda v, a, b, c, d: casaspi(
                    v, v0, I, a, b, c, d), v, pbspi(v, a, b, c, d))
                return popt, pcov

            popt, pcov = needs_curve_fit()
            perr = np.sqrt(np.diag(pcov))
            assert np.all(perr < 1.0e-6)

        # returns (S(v0), a', b', c', d')
        return I, popt[0], popt[1], popt[2], popt[3]

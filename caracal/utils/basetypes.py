# knicked from 

import os.path
from pathlib import Path
import uritools

# knicked from stimela2/scabha
class URI(str):
    def __init__(self, value):
        uri_components = uritools.urisplit(value)
        self.scheme = uri_components.scheme or "file"  # Protocol.
        self.authority = uri_components.authority
        self.query = uri_components.query
        self.fragment = uri_components.fragment

        # NOTE(JSKenyon): We assume that remote URIs are properly formed and
        # absolute i.e. we do not reason about relative paths for e.g. s3. The
        # following attempts to express paths relative to the cwd but will
        # prefer absolute paths when inputs are outside the cwd. This can be
        # changed when stimela's minimum Python >= 3.12 by using the newly
        # added `walk_up` option.
        if self.scheme == "file":
            cwd = Path.cwd().absolute()
            abs_path = Path(uri_components.path).expanduser().resolve()
            self.abs_path = str(abs_path)
            try:
                self.path = str(abs_path.relative_to(cwd))
            except ValueError as e:
                if "is not in the subpath" in str(e):
                    self.path = self.abs_path
                else:
                    raise e
        else:
            self.path = self.abs_path = uri_components.path

        self.full_uri = uritools.uricompose(
            scheme=self.scheme, authority=self.authority, path=self.abs_path, query=self.query, fragment=self.fragment
        )

        self.remote = self.scheme != "file"

    def __str__(self):
        return self.full_uri if self.remote else self.path

    def __repr__(self):
        return self.full_uri



class File(URI):
    @property
    def NAME(self):
        return File(os.path.basename(self))

    @property
    def PATH(self):
        return File(os.path.abspath(self))

    @property
    def DIR(self):
        return File(os.path.dirname(self))

    @property
    def BASEPATH(self):
        return File(os.path.splitext(self)[0])

    @property
    def BASENAME(self):
        return File(os.path.splitext(self.NAME)[0])

    @property
    def EXT(self):
        return os.path.splitext(self)[1]

    @property
    def EXISTS(self):
        return os.path.exists(self)


class Directory(File):
    pass


class MS(Directory):
    pass

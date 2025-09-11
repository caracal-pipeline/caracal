import json
from typing import Dict

class ObjDict(object):
    def __init__(self, items:Dict):
        """
        Converts a dictionary into an object. 

        """
        # First give this objects all the attributes of the input dicttionary
        for item in dir(dict):
            if not item.startswith("__"):
                setattr(self, item, getattr(items, item, None))
        # Now set the dictionary values as attributes
        for key, value in items.items():
            key = key.replace("-", "_")
            self.__dict__[key] = value
        
    @classmethod
    def from_dict(cls, d):
        # knicked from https://stackoverflow.com/a/34997118/27931152
        return json.loads(json.dumps(d), object_hook=ObjDict)
    

import stimela 
import json
import tempfile
import os
import codecs

def get_fields(pipeline, recipe, indir, caltable, 
        cab_name="get_field_info", label=""):

    _recipe = stimela.Recipe("get field info", ms_dir=recipe.ms_dir, 
            JOB_TYPE=recipe.JOB_TYPE, 
            singularity_image_dir=recipe.singularity_image_dir, log_dir=pipeline.logs)

    tfile = tempfile.NamedTemporaryFile(suffix=".json", dir=pipeline.output)
    tfile.flush
    _recipe.add("cab/pycasacore", cab_name, {
        "msname": caltable+":input",
        "script": """
from casacore.tables import table
import json
import os
import numpy
import codecs

INDIR = os.environ["INPUT"]
OUTPUT = os.environ["OUTPUT"]
tabname = os.path.join(INDIR, "{caltab:s}")

tab = table(tabname)

uf = numpy.unique(tab.getcol("FIELD_ID"))
fields = dict(field_id=list(map(int, uf)))

with codecs.open(OUTPUT+'/{fname:s}', 'w', 'utf8') as stdw:
        a = json.dumps(fields, ensure_ascii=False)
        stdw.write(a)

tab.close()
""".format(caltab=caltable, fname=os.path.basename(tfile.name)),
},
    input=indir,
    output=pipeline.output)

    _recipe.run()

    with codecs.open(tfile.name, "r", "utf8") as stdr:
        fields = json.load(stdr)
    tfile.close()

    return fields

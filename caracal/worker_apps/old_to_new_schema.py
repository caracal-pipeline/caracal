from omegaconf import OmegaConf
import click

OPT_MAP = {
    "type" : "dtype",
    "desc": "info",
    "required": "required",
    "example": "default",
    "enum": "choices",
    "seq" : None,
}

OPT_MAP_KEYS = list(OPT_MAP.keys())

def typecast(dtype, value):
    cast = getattr(__builtins__, dtype, False)
    if cast:
        return cast(value)
    else:
        return value


def caracal2scabha_option(option):
    opt = {}
    dtype = None
    seq = False
    default = None
    required = False
    for key, val in option.items():
        if key in OPT_MAP_KEYS:
            if key == "example":
                if val in ["", " ", "null", None, "none", "None"]:
                    continue
                else:
                    default = val
            elif key == "seq":
                seq = True
                dtype = getattr(val[0], "type", None)
                continue
            elif key == "type":
                dtype = val
            elif key == "required":
                val = bool(val)
                required = bool(val)
                
            opt[OPT_MAP[key]] = val
            
    if default not in [None]:
        if dtype:
            if seq:
                if not isinstance(default, list):
                    default = [default]
                opt["default"] = [typecast(dtype, _val) for _val in default]
                opt["dtype"] = f"List[{dtype}]"
            else:
                opt["default"] = typecast(dtype, default)
                opt["dtype"] = dtype

    # required options should not have defaults
    if required:
        del opt["default"]
    
    return opt


def caracal2scabha(schema_file, outfile=None):
    schema = OmegaConf.load(schema_file)

    worker_name = list(schema.mapping.keys())
    assert len(worker_name) == 1
    worker_name = worker_name[0]
    worker_info = schema.mapping[worker_name].desc
    worker_opts = schema.mapping[worker_name].mapping
    
    print(f"Converting worker: {worker_name} \n info: {schema.mapping[worker_name].desc}")

    new_worker = OmegaConf.create({
        "name": worker_name,
        "info": worker_info,
        "inputs": OmegaConf.create({}),
    })
    
    def convert_schema(section, options):
        if section == "cabs":
            assert len(options.seq) == 1
            cab_opts = options.seq[0].mapping
            new_opts = {}
            for _optname, _value in cab_opts.items():
                new_opts[_optname] = caracal2scabha_option(_value)
            return new_opts
            
        elif getattr(options, "type", None) == "map":
            new_opts = {}
            for _section, _options in options.mapping.items():
                new_opts[_section] = convert_schema(_section, _options)
            return new_opts
                
        elif getattr(options, "type", None) == "seq":
            pass
        elif OmegaConf.is_dict(options):
            return caracal2scabha_option(options)
    
    for section, options in worker_opts.items():
        new_worker.inputs[section] = convert_schema(section, options)

    if outfile:
        with open(outfile, "w") as stdw:
            new_schema = OmegaConf.to_yaml(new_worker)
            stdw.write(new_schema)

    return new_worker


@click.command()
@click.option("--caracal-schema", "-cs", help="Old caracal schema file to convert")
@click.option("--outfile", "-o", help="Output file")
def main(caracal_schema, outfile):
    caracal2scabha(caracal_schema, outfile)

if __name__ == '__main__':
    main()

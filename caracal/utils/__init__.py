from typing import Any, Union

from ruamel.yaml import YAML

yaml = YAML(typ="rt")


def load_yaml(fname: str) -> dict:
    with open(fname) as stdr:
        data = yaml.load(stdr)
    return to_regular_dict(data)


def write_yaml(data: dict, fname: str):
    # ensure ruglar dict before writing
    with open(fname, "w") as stdw:
        yaml.dump(to_regular_dict(data), stdw)


# from https://github.com/omry/omegaconf/discussions/1155#discussioncomment-8560712
def to_regular_dict(container: Any) -> Union[dict, list]:
    """Replace dict/list containers like OderedDicts to standard python dict/list objects

    Args:
        container (Any): Container

    Returns:
        _type_: _description_
    """
    if isinstance(container, dict):
        return {k: to_regular_dict(v) for k, v in container.items()}
    elif isinstance(container, list):
        return [to_regular_dict(k) for k in container]
    else:
        return container

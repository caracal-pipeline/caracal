import pytest

from caracal.utils.requirements import requires, OptionalImportError

def fn(a):
    return a + 1


def test_requires_no_import_error():
    """ Original function called when None or no ImportErrors """
    dec_fn = requires("pip install caracal[scipy]")(fn)
    assert dec_fn(1) == 2

    dec_fn = requires("pip install caracal[scipy]", None)(fn)
    assert dec_fn(1) == 2


def test_requires_import_error():
    """ Function raises OptionalImportError on ImportError """
    import_error = ImportError("scipy")
    dec_fn = requires("pip install caracal[scipy]", import_error)(fn)

    with pytest.raises(OptionalImportError) as e:
        assert dec_fn(1) == 2

    assert str(e.value) == 'pip install caracal[scipy]\n\nImportErrors\nscipy'

    dec_fn = requires(import_error)(fn)

    with pytest.raises(OptionalImportError) as e:
        assert dec_fn(1) == 2

    assert str(e.value) == 'Optional imports were missing\n\nImportErrors\nscipy'


def test_requires_import_error_skip():
    """ Function logs and skips on ImportError """
    e = ImportError("scipy")
    dec_fn = requires("pip install caracal[scipy]", e, skip=True)(fn)

    assert dec_fn(1) == 'pip install caracal[scipy]\n\nImportErrors\nscipy'

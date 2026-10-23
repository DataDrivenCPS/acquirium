"""acq.recipes: the loader, the file convention and the bound namespace."""

import inspect
import textwrap
from unittest.mock import MagicMock

import pytest

import acquirium
from acquirium import Acquirium, RecipeError
from acquirium.recipes import Recipes, recipes_in


def write(tmp_path, name, body):
    path = tmp_path / name
    path.write_text(textwrap.dedent(body))
    return path


@pytest.fixture
def acq():
    client = Acquirium(health_timeout=None)
    client.client = MagicMock()
    return client


@pytest.fixture
def recipes(acq):
    return Recipes(acq, builtin=False)


SIMPLE = '''
    """A test recipe file."""

    def greet(acq, who, *, loud=False):
        """Say hello.

        Longer explanation.
        """
        s = f"hello {who} from {acq.address}"
        return s.upper() if loud else s

    def no_doc(acq):
        return acq

    def _private(acq):
        return "hidden"

    def helper(x):
        return x

    def other_first(client, acq):
        return client
'''


class TestConvention:
    def test_public_acq_first_functions_only(self, recipes, tmp_path):
        names = recipes.add(write(tmp_path, "simple.py", SIMPLE))
        assert names == ["greet", "no_doc"]
        assert sorted(dir(recipes)) == ["add", "greet", "no_doc", "source"]
        assert "greet" in recipes and "helper" not in recipes and len(recipes) == 2

    def test_imported_functions_are_skipped(self, recipes, tmp_path):
        write(tmp_path, "base.py", SIMPLE)
        path = write(tmp_path, "importer.py", '''
            import sys
            sys.path.insert(0, %r)
            from base import greet

            def mine(acq):
                return greet(acq, "x")
        ''' % str(tmp_path))
        assert recipes.add(path) == ["mine"]

    def test_module_object(self, recipes):
        import types
        mod = types.ModuleType("made_up")
        exec("def alpha(acq): return 1\ndef beta(acq, k): return k", mod.__dict__)
        assert recipes.add(mod) == ["alpha", "beta"]
        assert next(iter(recipes)).origin == "made_up"

    def test_recipes_in_definition_order(self, tmp_path):
        import importlib.util
        path = write(tmp_path, "ordered.py", "def zeta(acq): pass\ndef alpha(acq): pass\n")
        spec = importlib.util.spec_from_file_location("ordered", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert list(recipes_in(mod)) == ["zeta", "alpha"]

    def test_not_a_python_file(self, recipes, tmp_path):
        with pytest.raises(RecipeError, match="not a Python file"):
            recipes.add(write(tmp_path, "notes.txt", "x"))
        with pytest.raises(RecipeError, match="not a Python file"):
            recipes.add(tmp_path / "missing.py")


class TestCollisions:
    def test_duplicate_name_raises_naming_both_origins(self, recipes, tmp_path):
        first = write(tmp_path, "first.py", "def dup(acq): return 1\n")
        second = write(tmp_path, "second.py", "def dup(acq): return 2\n")
        recipes.add(first)
        with pytest.raises(RecipeError) as err:
            recipes.add(second)
        assert str(first) in str(err.value) and str(second) in str(err.value)
        assert "replace=True" in str(err.value)
        assert recipes.dup() == 1

    def test_replace_overrides(self, recipes, tmp_path):
        recipes.add(write(tmp_path, "first.py", "def dup(acq): return 1\n"))
        recipes.add(write(tmp_path, "second.py", "def dup(acq): return 2\n"), replace=True)
        assert recipes.dup() == 2 and len(recipes) == 1

    def test_collision_registers_nothing_from_the_file(self, recipes, tmp_path):
        recipes.add(write(tmp_path, "first.py", "def dup(acq): return 1\n"))
        with pytest.raises(RecipeError):
            recipes.add(write(tmp_path, "second.py", "def fresh(acq): pass\ndef dup(acq): pass\n"))
        assert "fresh" not in recipes

    def test_reserved_names(self, recipes, tmp_path):
        with pytest.raises(RecipeError, match="reserved"):
            recipes.add(write(tmp_path, "bad.py", "def add(acq): pass\n"))
        with pytest.raises(RecipeError, match="reserved"):
            recipes.add(write(tmp_path, "bad2.py", "def source(acq): pass\n"))


class TestVersionCheck:
    def test_satisfied(self, recipes, tmp_path):
        assert recipes.add(write(tmp_path, "ok.py", 'ACQUIRIUM = ">=0.1"\ndef r(acq): pass\n')) == ["r"]

    def test_prerelease_counts(self, recipes, tmp_path):
        installed = acquirium.recipes._installed_version()
        spec = f"=={installed}"
        assert recipes.add(write(tmp_path, "ok.py", f'ACQUIRIUM = "{spec}"\ndef r(acq): pass\n')) == ["r"]

    def test_mismatch_raises(self, recipes, tmp_path):
        path = write(tmp_path, "old.py", 'ACQUIRIUM = "<0.0.1"\ndef r(acq): pass\n')
        with pytest.raises(RecipeError, match="requires acquirium <0.0.1") as err:
            recipes.add(path)
        assert str(path) in str(err.value)
        assert "r" not in recipes

    def test_malformed_raises(self, recipes, tmp_path):
        with pytest.raises(RecipeError, match="not a valid version specifier"):
            recipes.add(write(tmp_path, "bad.py", 'ACQUIRIUM = "latest"\ndef r(acq): pass\n'))

    def test_absent_constant_skips_check(self, recipes, tmp_path):
        assert recipes.add(write(tmp_path, "free.py", "def r(acq): pass\n")) == ["r"]


class TestBoundNamespace:
    def test_call_binds_acq_and_forwards_arguments(self, acq, recipes, tmp_path):
        recipes.add(write(tmp_path, "simple.py", SIMPLE))
        assert recipes.greet("ann") == f"hello ann from {acq.address}"
        assert recipes.greet("ann", loud=True) == f"HELLO ANN FROM {acq.address}".upper()
        assert recipes.no_doc() is acq

    def test_signature_hides_acq_and_keeps_doc(self, recipes, tmp_path):
        recipes.add(write(tmp_path, "simple.py", SIMPLE))
        assert str(inspect.signature(recipes.greet)) == "(who, *, loud=False)"
        assert recipes.greet.__doc__.startswith("Say hello.")
        assert recipes.greet.__name__ == "greet"

    def test_unknown_name_lists_available(self, recipes, tmp_path):
        recipes.add(write(tmp_path, "simple.py", SIMPLE))
        with pytest.raises(AttributeError, match="no recipe named 'nope'; available: greet, no_doc"):
            recipes.nope
        with pytest.raises(AttributeError):
            recipes._anything

    def test_source_returns_the_function_text(self, recipes, tmp_path):
        recipes.add(write(tmp_path, "simple.py", SIMPLE))
        text = recipes.source("greet")
        assert text.startswith("def greet(acq, who, *, loud=False):")
        assert "return s.upper() if loud else s" in text
        with pytest.raises(AttributeError):
            recipes.source("nope")

    def test_repr_lists_name_description_origin(self, recipes, tmp_path):
        assert repr(recipes) == "Recipes: none loaded"
        path = write(tmp_path, "simple.py", SIMPLE)
        recipes.add(path)
        text = repr(recipes)
        assert text.startswith("Recipes (2):")
        assert f"greet   Say hello.  [{path}]" in text
        assert f"no_doc    [{path}]" in text


class TestClientProperty:
    def test_lazy_and_cached(self, acq):
        assert "recipes" not in acq.__dict__
        first = acq.recipes
        assert isinstance(first, Recipes)
        assert acq.recipes is first

    def test_builtins_load_and_label_origin(self, acq):
        for recipe in acq.recipes:
            assert recipe.origin.startswith("acquirium.recipes.")

    def test_exported(self):
        assert acquirium.Recipes is Recipes
        assert issubclass(acquirium.RecipeError, ValueError)

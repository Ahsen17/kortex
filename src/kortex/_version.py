from importlib.metadata import PackageNotFoundError, version

__all__ = ("__version__",)

try:
    __version__ = version("kortex")
except PackageNotFoundError:
    import tomllib
    from pathlib import Path

    with Path("pyproject.toml").open("rb") as f:
        pyproject = tomllib.load(f)
        __version__ = pyproject["project"]["version"]

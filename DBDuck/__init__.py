from importlib.metadata import PackageNotFoundError, version

from .UDOM import UDOM

try:
    __version__ = version("DBDuck")
except PackageNotFoundError:
    __version__ = "0.1.0"

from .handler import handler as compiler

from importlib.metadata import version

__version__ = version("sck-core-deployspec")

__all__ = ["compiler"]

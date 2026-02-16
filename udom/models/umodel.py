# udom/models/umodel.py

class UModel:
    """Base class for all data models in UDOM."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    @classmethod
    def get_fields(cls):
        """Return class annotations (field names and types)"""
        return cls.__annotations__

    @classmethod
    def get_name(cls):
        """Return model name (class name) – used for table/collection/node"""
        return cls.__name__

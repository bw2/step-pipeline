from enum import Enum


class Backend(Enum):
    """Supported backends"""

    HAIL_BATCH_LOCAL = "hbl"
    HAIL_BATCH_SERVICE = "hbs"

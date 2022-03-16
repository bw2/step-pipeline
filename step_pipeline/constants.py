from enum import Enum


class Backend(Enum):
    """Constants that represent possible pipeline execution backends"""

    HAIL_BATCH_LOCAL = "hbl"
    HAIL_BATCH_SERVICE = "hbs"
    TERRA = "terra"
    CROMWELL = "cromwell"
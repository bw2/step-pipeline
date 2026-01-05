from enum import Enum


class Backend(Enum):
    """Constants that represent possible pipeline execution backends.

    Attributes:
        HAIL_BATCH_LOCAL: Run pipeline locally using Hail Batch LocalBackend.
        HAIL_BATCH_SERVICE: Run pipeline on the Hail Batch cloud service.
        TERRA: Generate WDL for execution on Terra platform.
        CROMWELL: Generate WDL for execution on Cromwell.
    """

    HAIL_BATCH_LOCAL = "hbl"
    HAIL_BATCH_SERVICE = "hbs"
    TERRA = "terra"
    CROMWELL = "cromwell"
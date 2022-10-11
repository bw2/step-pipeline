"""This module contains classes and methods related to data input & output."""

from abc import ABC, abstractmethod
from enum import Enum

import os
import re
import uuid


class Localize(Enum):
    """Constants that represent different options for how to localize files into the running container.
    Each 2-tuple consists of a name for the localization approach, and a subdirectory where to put files.
    """

    COPY = ("copy", "local_copy")
    """COPY uses the execution backend's default approach to localizing files"""

    GSUTIL_COPY = ("gsutil_copy", "local_copy")
    """GSUTIL_COPY runs 'gsutil cp' to localize file(s) from a google bucket path. This requires gsutil to be available 
    inside the execution container.
    """

    HAIL_HADOOP_COPY = ("hail_hadoop_copy", "local_copy")
    """HAIL_HADOOP_COPY uses the Hail hadoop API to copy file(s) from a google bucket path. This requires python3 and 
    Hail to be installed inside the execution container.
    """

    HAIL_BATCH_CLOUDFUSE = ("hail_batch_cloudfuse", "cloudfuse")
    """HAIL_BATCH_CLOUDFUSE use the Hail Batch cloudfuse function to mount a google bucket into the execution container 
    as a network drive, without copying the files. This Hail Batch service account must have read access to the bucket.
    """

    HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET = ("hail_batch_cloudfuse_via_temp_bucket", "cloudfuse")
    """HAIL_BATCH_CLOUDFUSE_VIA_TEMP_BUCKET is useful for situations where you'd like to use cloudfuse to localize files and 
    your personal gcloud account has read access to the source bucket, but the Hail Batch service account cannot be 
    granted read access to that bucket. Since it's possible to run 'gsutil cp' under your personal credentials within
    the execution container, but Hail Batch cloudfuse always runs under the Hail Batch service account credentials, this 
    workaround 1) runs 'gsutil cp' under your personal credentials to copy the source files to a temporary bucket that 
    you control, and where you have granted read access to the Hail Batch service account 2) uses cloudfuse to mount the 
    temporary bucket 3) performs computational steps on the mounted data 4) deletes the source files from the temporary 
    bucket when the Batch job completes.
    
    This localization approach may be useful for situations where you need a large number of jobs and each job processes 
    a very small piece of a large data file (eg. a few loci in a cram file). 
    
    Copying the large file(s) from the source bucket to a temp bucket in the same region is fast and inexpensive, and 
    only needs to happen once before the jobs run. Each job can then avoid allocating a large disk, and waiting for the 
    large file to be copied into the container. This approach requires gsutil to be available inside the execution 
    container.
    """

    def __init__(self, label, subdir="local_copy"):
        """Enum constructor.

        Args:
          label (str): a name
          subdir (str): subdirectory where files will be localized within the execution container
        """

        self._label = label
        self._subdir = subdir

    def __str__(self):
        return self._label

    def __repr__(self):
        return self._label

    def get_subdir_name(self):
        """Returns the subdirectory name passed to the constructor"""
        return self._subdir


class Delocalize(Enum):
    """Constants that represent different options for how to delocalize file(s) from a running container."""

    COPY = "copy"
    """COPY uses the execution backend's default approach to delocalizing files"""

    GSUTIL_COPY = "gsutil_copy"
    """GSUTIL_COPY runs 'gsutil cp' to copy the path to a google bucket destination. This requires gsutil to be 
    available inside the execution container.
    """

    HAIL_HADOOP_COPY = "hail_hadoop_copy"
    """HAIL_HADOOP_COPY uses the hail hadoop API to copy file(s) to a google bucket path. This requires python3 and 
    hail to be installed inside the execution container.
    """

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class InputType(Enum):
    """Constants that represent the type of a step.input_value(..) arg."""

    STRING = "string"
    FLOAT = "float"
    INT = "int"
    BOOL = "boolean"


class InputSpecBase(ABC):
    """This is the InputSpec parent class, with subclasses implementing specific types of input specs which contain
    metadata about inputs to a Pipeline Step.
    """

    def __init__(self, name=None):
        """InputSpec constructor

        Args:
            name (str): Optional name for this input.
        """

        self._uuid = str(uuid.uuid4())
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def uuid(self):
        return self._uuid

    @abstractmethod
    def __str__(self):
        return self._uuid


class InputValueSpec(InputSpecBase):
    """An InputValueSpec stores metadata about an input that's not a file path"""

    def __init__(
            self,
            value=None,
            name=None,
            input_type=InputType.STRING):
        """InputValueSpec constructor

        Args:
            value: The value.
            name (str): Optional name for this input.
            input_type (InputType): The input value's type.
        """
        super().__init__(name=name)

        self._value = value
        self._input_type = input_type

    def __str__(self):
        if self.value is not None:
            return self.value
        else:
            return f"[input:{self._uuid}]"

    @property
    def value(self):
        return self._value

    @property
    def input_type(self):
        return self._input_type


class InputSpec(InputSpecBase):
    """An InputSpec stores metadata about an input file or directory"""

    def __init__(
            self,
            source_path=None,
            name=None,
            localize_by=None,
            localization_root_dir=None,
            original_source_path=None,
    ):
        """InputSpec constructor

        Args:
            source_path (str): Source file or directory to localize. It can be a gs://, http(s)://, or a filesystem path.
                The path can include * wildcards where appropriate.
            name (str): Optional name for this input.
            localize_by (Localize): Approach to use to localize this path.
            localization_root_dir (str): This input will be localized to this directory within the container filesystem.
            original_source_path (str): Sometimes the file is copied to a different cloud location as part of
                localization. This is an optional arg to records its original location.
        """

        super().__init__(name=name)

        self._source_path = source_path
        self._localize_by = localize_by
        self._localization_root_dir = localization_root_dir
        self._original_source_path = original_source_path or source_path

        # these fields are computed based on the source_path
        self._source_bucket = None
        self._source_path_without_protocol = None
        self._source_dir = None
        self._filename = None
        self._local_dir = None
        self._local_path = None

        if localize_by and not isinstance(localize_by, Localize):
            raise ValueError(f"localize_by arg: {localize_by} is not an instance of the Localize enum")

        if localization_root_dir and not isinstance(localization_root_dir, str):
            raise ValueError(f"localization_root_dir arg: {localization_root_dir} is not a string")

        if source_path is not None:
            match = re.match("^([a-zA-Z-_]+)://(.*)", source_path)
            if match:
                self._source_path_without_protocol = match.group(2)
                self._source_bucket = self._source_path_without_protocol.split("/")[0]
            elif source_path.startswith("http://") or source_path.startswith("https://"):
                self._source_path_without_protocol = re.sub("^http[s]?://", "", source_path).split("?")[0]
            else:
                self._source_path_without_protocol = source_path

            self._source_dir = os.path.dirname(self._source_path_without_protocol)
            self._filename = os.path.basename(self._source_path_without_protocol).replace("*", "_._")

            self._name = self._name or self._filename

            subdir = localize_by.get_subdir_name()
            output_dir = os.path.join(localization_root_dir, subdir, self.source_dir.strip("/"))
            output_dir = output_dir.replace("*", "___")

            self._local_dir = output_dir
            self._local_path = os.path.join(output_dir, self.filename)

    def __str__(self):
        if self.local_path is not None:
            return self.local_path
        else:
            return f"[input:{self._uuid}]"

    @property
    def source_path(self):
        if self._source_path is None:
            raise ValueError("source_path not available for this input")
        return self._source_path

    @property
    def original_source_path(self):
        if self._original_source_path is None:
            raise ValueError("original_source_path not available for this input")
        return self._original_source_path

    @property
    def source_bucket(self):
        if self._source_bucket is None:
            raise ValueError("source_path not available for this input")
        return self._source_bucket

    @property
    def source_path_without_protocol(self):
        if self._source_path_without_protocol is None:
            raise ValueError("source_path not available for this input")
        return self._source_path_without_protocol

    @property
    def source_dir(self):
        if self._source_dir is None:
            raise ValueError("source_path not available for this input")
        return self._source_dir

    @property
    def filename(self):
        if self._filename is None:
            raise ValueError("source_path not available for this input")
        return self._filename

    @property
    def local_path(self):
        if self._local_path is None:
            raise ValueError("source_path not available for this input")
        return self._local_path

    @property
    def local_dir(self):
        if self._local_dir is None:
            raise ValueError("source_path not available for this input")
        return self._local_dir

    @property
    def localize_by(self):
        return self._localize_by

    @property
    def localization_root_dir(self):
        return self._localization_root_dir


class OutputSpec:
    """An OutputSpec stores metadata about an output file or directory from a Step"""

    def __init__(
            self,
            local_path=None,
            output_dir=None,
            output_path=None,
            name=None,
            delocalize_by=None):
        """OutputSpec constructor

        Args:
            local_path (str): Local (within container) path of file or directory to delocalize. The path can include *
                wildcards.
            output_dir (str): Optional destination directory.
            output_path (str): Optional destination path - either absolute, or relative to output_dir.
            name (str): Optional name for this output.
            delocalize_by (Delocalize): Approach to use to delocalize this path.
        """
        self._local_path = local_path
        self._local_dir = os.path.dirname(local_path)
        self._name = name
        self._delocalize_by = delocalize_by

        if delocalize_by and not isinstance(delocalize_by, Delocalize):
            raise ValueError(f"localize_by arg: {delocalize_by} is not an instance of the Delocalize enum")

        # define self._output_filename
        if output_path:
            self._output_filename = os.path.basename(output_path)
        elif "*" not in local_path:
            self._output_filename = os.path.basename(local_path)
        else:
            self._output_filename = None

        # define self._output_path and self._output_dir
        if output_path:
            self._output_path = output_path
            self._output_dir = os.path.dirname(self._output_path)
        elif output_dir:
            self._output_dir = output_dir
            if output_path:
                if os.path.isabs(output_path) or "://" in output_path:
                    self._output_path = output_path
                else:
                    self._output_path = os.path.join(output_dir, output_path)
            elif self._output_filename:
                self._output_path = os.path.join(output_dir, self._output_filename)
            else:
                self._output_path = output_dir

        else:
            raise ValueError("Neither output_dir nor output_path were specified.")

        if "*" in self._output_path:
            raise ValueError(f"output path ({output_path}) cannot contain wildcards (*)")

    def __str__(self):
        return self.output_path

    @property
    def output_path(self):
        return self._output_path

    @property
    def output_dir(self):
        return self._output_dir

    @property
    def filename(self):
        return self._output_filename

    @property
    def name(self):
        return self._name

    @property
    def local_path(self):
        return self._local_path

    @property
    def local_dir(self):
        return self._local_dir

    @property
    def delocalize_by(self):
        return self._delocalize_by

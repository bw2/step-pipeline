"""This module contains classes and methods related to data input & output."""

from enum import Enum

import os
import re


class Localize(Enum):
    """This class lists the different ways to localize files into the running container.
    Each 2-tuple consists of a name for the localization approach, and a subdirectory where to put files.
    """

    """COPY uses the execution backend's default approach to localizing files"""
    COPY = ("copy", "local_copy")

    """GSUTIL_COPY runs 'gsutil cp' to localize file(s) from a google bucket path. This requires gsutil to be available 
    inside the execution container.
    """
    GSUTIL_COPY = ("gsutil_copy", "local_copy")

    """HAIL_HADOOP_COPY uses the Hail hadoop API to copy file(s) from a google bucket path. This requires python3 and 
    Hail to be installed inside the execution container.
    """
    HAIL_HADOOP_COPY = ("hail_hadoop_copy", "local_copy")

    """HAIL_BATCH_GCSFUSE use the Hail Batch gcsfuse function to mount a google bucket into the execution container 
    as a network drive, without copying the files. This Hail Batch service account must have read access to the bucket.
    """
    HAIL_BATCH_GCSFUSE = ("hail_batch_gcsfuse", "gcsfuse")

    """HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET is useful for situations where you'd like to use gcsfuse to localize files and 
    your personal gcloud account has read access to the source bucket, but the Hail Batch service account cannot be 
    granted read access to that bucket. Since it's possible to run 'gsutil cp' under your personal credentials within
    the execution container, but Hail Batch gcsfuse always runs under the Hail Batch service account credentials, this 
    workaround 1) runs 'gsutil cp' under your personal credentials to copy the source files to a temporary bucket that 
    you control, and where you have granted read access to the Hail Batch service account 2) uses gcsfuse to mount the 
    temporary bucket 3) performs computational steps on the mounted data 4) deletes the source files from the temporary 
    bucket when the Batch job completes.
    
    This localization approach may be useful for situations where you need a large number of jobs and each job processes 
    a very small piece of a large data file (eg. a few loci in a cram file). 
    
    Copying the large file(s) from the source bucket to a temp bucket in the same region is fast and inexpensive, and 
    only needs to happen once before the jobs run. Each job can then avoid allocating a large disk, and waiting for the 
    large file to be copied into the container. This approach requires gsutil to be available inside the execution 
    container.
    """
    HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET = ("hail_batch_gcsfuse_via_temp_bucket", "gcsfuse")

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
        return self._subdir


class Delocalize(Enum):
    """This class lists the different ways to delocalize files from a running container."""

    """COPY uses the execution backend's default approach to delocalizing files"""
    COPY = "copy"

    """GSUTIL_COPY runs 'gsutil cp' to copy the path to a google bucket destination. This requires gsutil to be 
    available inside the execution container.
    """
    GSUTIL_COPY = "gsutil_copy"

    """HAIL_HADOOP_COPY uses the hail hadoop API to copy file(s) to a google bucket path. This requires python3 and 
    hail to be installed inside the execution container.
    """
    HAIL_HADOOP_COPY = "hail_hadoop_copy"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class _InputSpec:
    """An _InputSpec stores metadata about an input file or directory to a Step"""

    def __init__(self,
                 source_path,
                 name = None,
                 localize_by = None,
                 localization_root_dir = None,
                 ):
        """_InputSpec constructor

        Args:
            source_path (str): Source file or directory to localize. It can be a gs://, http(s)://, or a filesystem path.
                The path can include * wildcards where appropriate.
            name (str): Optional name for this input.
            localize_by (Localize): Approach to use to localize this path.
            localization_root_dir (str): This input will be localized to this directory within the container filesystem.
        """

        self._source_path = source_path
        self._localize_by = localize_by

        self._source_bucket = None
        if source_path.startswith("gs://"):
            self._source_path_without_protocol = re.sub("^gs://", "", source_path)
            self._source_bucket = self._source_path_without_protocol.split("/")[0]
        elif source_path.startswith("http://") or source_path.startswith("https://"):
            self._source_path_without_protocol = re.sub("^http[s]?://", "", source_path).split("?")[0]
        else:
            self._source_path_without_protocol = source_path

        self._source_dir = os.path.dirname(self._source_path_without_protocol)
        self._filename = os.path.basename(self._source_path_without_protocol).replace("*", "_._")

        self._name = name or self._filename

        subdir = localize_by.get_subdir_name()
        output_dir = os.path.join(localization_root_dir, subdir, self.source_dir.strip("/"))
        output_dir = output_dir.replace("*", "___")

        self._local_dir = output_dir
        self._local_path = os.path.join(output_dir, self.filename)

    def __str__(self):
        return self.local_path

    @property
    def source_path(self):
        return self._source_path

    @property
    def source_bucket(self):
        return self._source_bucket

    @property
    def source_path_without_protocol(self):
        return self._source_path_without_protocol

    @property
    def source_dir(self):
        return self._source_dir

    @property
    def filename(self):
        return self._filename

    @property
    def input_name(self):
        return self._name

    @property
    def local_path(self):
        return self._local_path

    @property
    def local_dir(self):
        return self._local_dir

    @property
    def localize_by(self):
        return self._localize_by


class _OutputSpec:
    """An _OutputSpec stores metadata about an output file or directory from a Step"""

    def __init__(self,
                 local_path: str,
                 output_dir: str = None,
                 output_path: str = None,
                 name: str = None,
                 delocalize_by: str = None):
        """_OutputSpec constructor

        Args:
            local_path (str): Local (within container) path of file or directory to delocalize. The path can include *
                wildcards.
            output_dir (str): Optional destination directory.
            output_path (str): Optional destination path - either absolute, or relative to output_dir.
            name (str): Optional name for this output.
            delocalize_by (Deocalize): Approach to use to delocalize this path.
        """
        self._local_path = local_path
        self._local_dir = os.path.dirname(local_path)
        self._name = name
        self._delocalize_by = delocalize_by

        # define self._output_filename
        if output_path:
            self._output_filename = os.path.basename(output_path)
        elif "*" not in local_path:
            self._output_filename = os.path.basename(local_path)
        else:
            self._output_filename = None

        # define self._output_path and self._output_dir
        if output_dir:
            self._output_dir = output_dir
            if output_path:
                if os.path.isabs(output_path) or output_path.startswith("gs://"):
                    self._output_path = output_path
                else:
                    self._output_path = os.path.join(output_dir, output_path)
            elif self._output_filename:
                self._output_path = os.path.join(output_dir, self._output_filename)
            else:
                self._output_path = output_dir

        elif output_path:
            self._output_path = output_path
            self._output_dir = os.path.dirname(self._output_path)
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
    def output_filename(self):
        return self._output_filename

    @property
    def output_name(self):
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


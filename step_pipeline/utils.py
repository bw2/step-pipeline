"""This module contains misc. utility functions used by other modules."""

from datetime import datetime, timezone
from dateutil import parser
import glob
import hail as hl
import os
import pytz
import subprocess
import tempfile

os.environ["HAIL_LOG_DIR"] = tempfile.gettempdir()
#hl.init(log="/dev/null", quiet=True)


HADOOP_EXISTS_CACHE = {}
HADOOP_STAT_CACHE = {}
GSUTIL_PATH_TO_FILE_STAT_CACHE = {}
BUCKET_LOCATION_CACHE = {}

LOCAL_TIMEZONE = pytz.timezone("US/Eastern") #datetime.now(timezone.utc).astimezone().tzinfo


def _generate_gs_path_to_file_stat_dict(gs_path_with_wildcards):
    """Takes a gs:// path that contains one or more wildcards ("*") and runs "gsutil ls -l {gs_path_with_wildcards}".
    This method then returns a dictionary that maps each gs:// file to its size in bytes. Running gsutil is currently
    faster than running hl.hadoop_ls(..) when the path matches many files.
    """
    if not isinstance(gs_path_with_wildcards, str):
        raise ValueError(f"Unexpected argument type {str(type(gs_path_with_wildcards))}: {gs_path_with_wildcards}")

    if not gs_path_with_wildcards.startswith("gs://"):
        raise ValueError(f"{gs_path_with_wildcards} path doesn't start with gs://")

    if gs_path_with_wildcards in GSUTIL_PATH_TO_FILE_STAT_CACHE:
        return GSUTIL_PATH_TO_FILE_STAT_CACHE[gs_path_with_wildcards]

    print(f"Listing {gs_path_with_wildcards}")
    try:
        gsutil_output = subprocess.check_output(
            f"gsutil -m ls -l {gs_path_with_wildcards}",
            shell=True,
            stderr=subprocess.STDOUT,
            encoding="UTF-8")
    except subprocess.CalledProcessError as e:
        if any(phrase in e.output for phrase in (
            "One or more URLs matched no objects",
            "bucket does not exist.",
        )):
            return {}
        else:
            raise _GoogleStorageException(e.output)

    # map path to file size in bytes and its last-modified date (eg. "2020-05-20T16:52:01Z")
    def parse_gsutil_date_string(date_string):
        #utc_date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        utc_date = parser.parse(date_string).replace(tzinfo=timezone.utc)
        return utc_date.astimezone(LOCAL_TIMEZONE)

    records = [r.strip().split("  ") for r in gsutil_output.strip().split("\n") if not r.startswith("TOTAL: ")]
    path_to_file_stat_dict = {
        r[2]: (int(r[0]), parse_gsutil_date_string(r[1])) for r in records
    }

    GSUTIL_PATH_TO_FILE_STAT_CACHE[gs_path_with_wildcards] = path_to_file_stat_dict

    return path_to_file_stat_dict


def _path_exists__cached(path):
    """Takes a local path or a gs:// Google Storage path and returns True if the path exists.
    The path can contain wildcards (*) in which case this method returns True if at least one matching file or directory
    exists.

    Args:
        path: local path or gs:// Google Storage path. The path can contain wildcards (*).

    Return:
        bool: True if the path exists.
    """
    if not isinstance(path, str):
        raise ValueError(f"Unexpected path type {type(path)}: {path}")

    if path in HADOOP_EXISTS_CACHE:
        return HADOOP_EXISTS_CACHE[path]

    if path.startswith("gs://"):
        if "*" in path:
            HADOOP_EXISTS_CACHE[path] = bool(_generate_gs_path_to_file_stat_dict(path))
        else:
            HADOOP_EXISTS_CACHE[path] = hl.hadoop_exists(path)
    else:
        if "*" in path:
            HADOOP_EXISTS_CACHE[path] = len(glob.glob(path)) > 0
        else:
            HADOOP_EXISTS_CACHE[path] = os.path.exists(path)

    return HADOOP_EXISTS_CACHE[path]


def _file_stat__cached(path):
    """Takes a local file path or gs:// Google Storage path and returns a list of file stats including the size in bytes
    and the modification time.

    Args:
        path (str): local file path or gs:// Google Storage path. The path can contain wildcards (*).

    Return:
        list: List of metadata dicts like: [
        {
            'path': 'gs://bucket/dir/file.bam.bai',
            'size_bytes': 2784,
            'modification_time': 'Wed May 20 12:52:01 EDT 2020',
        },
        ...
    ]
    """
    if path in HADOOP_STAT_CACHE:
        return HADOOP_STAT_CACHE[path]

    if path.startswith("gs://"):
        if "*" in path:
            path_to_file_stat_dict = _generate_gs_path_to_file_stat_dict(path)
            HADOOP_STAT_CACHE[path] = []
            for path_without_star, (size_bytes, modification_time) in path_to_file_stat_dict.items():
                HADOOP_STAT_CACHE[path].append({
                    "path": path_without_star,
                    "size_bytes": size_bytes,
                    "modification_time": modification_time,
                })
        else:
            try:
                stat_results = hl.hadoop_stat(path)
            except Exception as e:
                if "File not found" in str(e):
                    raise FileNotFoundError(f"File not found: {path}")
                else:
                    raise e


            """hl.hadoop_stat returns:
            {
                'path': 'gs://bucket/dir/file.bam.bai',
                'size_bytes': 2784,
                'size': '2.7K',
                'is_dir': False,
                'modification_time': 'Wed May 20 12:52:01 EDT 2020',
                'owner': 'weisburd'
            }
            """
            #stat_results["modification_time"] = datetime.strptime(
            #    stat_results["modification_time"], '%a %b %d %H:%M:%S %Z %Y').replace(tzinfo=LOCAL_TIMEZONE)
            stat_results["modification_time"] = LOCAL_TIMEZONE.localize(
                parser.parse(stat_results["modification_time"], ignoretz=True))
            HADOOP_STAT_CACHE[path] = [stat_results]
    else:
        if "*" in path:
            local_paths = glob.glob(path)
        else:
            local_paths = [path]

        print(f"Running stat on {local_paths}")
        for local_path in local_paths:
            stat = os.stat(local_path)
            if path not in HADOOP_STAT_CACHE:
                HADOOP_STAT_CACHE[path] = []
            HADOOP_STAT_CACHE[path].append({
                "path": local_path,
                "size_bytes": stat.st_size,
                "modification_time": datetime.fromtimestamp(stat.st_ctime).replace(tzinfo=LOCAL_TIMEZONE),
            })

    return HADOOP_STAT_CACHE[path]


def are_any_inputs_missing(step, verbose=False):
    """Returns True if any of the Step's inputs don't exist"""

    for input_spec in step._inputs:
        input_path = input_spec.source_path
        if not _path_exists__cached(input_path):
            if verbose:
                print(f"Input missing: {input_path}")
            return True

    return False


def are_outputs_up_to_date(step, verbose=False):
    """Returns True if all of the Step's outputs already exist and are newer than all inputs"""

    if len(step._outputs) == 0:
        return False

    latest_input_path = None
    latest_input_modified_date = datetime(2, 1, 1, tzinfo=LOCAL_TIMEZONE)
    for input_spec in step._inputs:
        input_path = input_spec.source_path
        if not _path_exists__cached(input_path):
            raise ValueError(f"Input path doesn't exist: {input_path}")

        stat_list = _file_stat__cached(input_path)
        for stat in stat_list:
            latest_input_modified_date = max(latest_input_modified_date, stat["modification_time"])
            latest_input_path = stat["path"]

    # check whether any outputs are missing
    oldest_output_path = None
    oldest_output_modified_date = datetime.now(LOCAL_TIMEZONE)
    for output_spec in step._outputs:
        if not _path_exists__cached(output_spec.output_path):
            return False

        stat_list = _file_stat__cached(output_spec.output_path)
        for stat in stat_list:
            oldest_output_modified_date = min(oldest_output_modified_date, stat["modification_time"])
            oldest_output_path = stat["path"]

    if verbose:
        print(f"Oldest output ({oldest_output_modified_date}): {oldest_output_path},  "
              f"newest input ({latest_input_modified_date}): {latest_input_path}")

    return latest_input_modified_date <= oldest_output_modified_date


class _GoogleStorageException(Exception):
    pass


def check_gcloud_storage_region(gs_path, expected_regions=("US", "US-CENTRAL1"), gcloud_project=None,
                                ignore_access_denied_exception=True, verbose=True):
    """Checks whether the given Google Storage path is located in one of the expected_regions. This is set to
    "US-CENTRAL1" by default since that's the region where the hail Batch cluster is located. Localizing data from
    other regions will be slower and result in egress charges.

    Args:
        gs_path (str): The google storage gs:// path to check. Only the bucket portion of the path matters, so other
            parts of the path can contain wildcards (*), etc.
        expected_regions (tuple): a set of acceptable storage regions. If gs_path is not in one of these regions, this
            method will raise a StorageRegionException.
        gcloud_project (str): (optional) if specified, it will be added to the gsutil command with the -u arg.
        ignore_access_denied_exception (bool): if True, this method return silently if it encounters an AccessDenied
            error.
        verbose (bool): print more logs

    Raises:
        StorageRegionException: If the given gs_path is not stored in one the expected_regions.
    """
    gs_path_tokens = gs_path.split("/")
    if not gs_path.startswith("gs://") or len(gs_path_tokens) < 3:
        raise ValueError(f"Invalid gs_path arg: {gs_path}")

    bucket = gs_path_tokens[2]

    if bucket in BUCKET_LOCATION_CACHE:
        location = BUCKET_LOCATION_CACHE[bucket]
    else:
        gsutil_command = f"gsutil"
        if gcloud_project:
            gsutil_command += f" -u {gcloud_project}"

        output = subprocess.check_output(
            f"{gsutil_command} ls -L -b gs://{bucket}; exit 0", shell=True, encoding="UTF-8", stderr=subprocess.STDOUT)

        for line in output.split("\n"):
            if "AccessDeniedException" in line:
                message = f"Unable to check google storage region for gs://{bucket}. Access denied."
                if ignore_access_denied_exception:
                    if verbose:
                        print(message)
                    return
                else:
                    raise _GoogleStorageException(f"ERROR: {message}")

            if "Location constraint:" in line:
                location = line.strip().split()[-1]
                BUCKET_LOCATION_CACHE[bucket] = location
                break
        else:
            raise _GoogleStorageException(f"ERROR: Couldn't determine gs://{bucket} bucket region.")

    if location not in expected_regions:
        raise _GoogleStorageException(f"ERROR: gs://{bucket} is located in {location} which is not one of the"
                                      f" expected regions {expected_regions}")
    if verbose:
        print(f"Confirmed gs://{bucket} is in {location}")

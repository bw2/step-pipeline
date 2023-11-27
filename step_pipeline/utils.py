"""This module contains misc. utility functions used by other modules."""

import collections
from datetime import datetime, timezone
from dateutil import parser
import glob
import hail as hl
import os
import pytz
import subprocess
import tempfile


os.environ["HAIL_LOG_DIR"] = tempfile.gettempdir()
hl.init(log="/dev/null", quiet=True, idempotent=True)

GOOGLE_STORAGE_CLIENT = None
PATH_EXISTS_CACHE = {}
PATH_STAT_CACHE = {}
GSUTIL_PATH_TO_FILE_STAT_CACHE = {}
BUCKET_LOCATION_CACHE = {}
_BUCKET_ERROR_COUNTER = collections.defaultdict(int)
_MAX_ERROR_MESSAGES_TO_PRINT_PER_BUCKET = 1000

LOCAL_TIMEZONE = pytz.timezone("US/Eastern") #datetime.now(timezone.utc).astimezone().tzinfo

DATE_STRFTIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _get_bucket_name(gs_path):
    """Get the Google bucket name from the given gs_path."""

    gs_path_tokens = gs_path.split("/")
    if not gs_path.startswith("gs://") or len(gs_path_tokens) < 3:
        raise ValueError(f"Invalid gs_path arg: {gs_path}")

    bucket_name = gs_path_tokens[2]

    return bucket_name


def _get_google_storage_client(gcloud_project):
    global GOOGLE_STORAGE_CLIENT
    if GOOGLE_STORAGE_CLIENT is None:
        from google.cloud import storage
        GOOGLE_STORAGE_CLIENT = storage.Client(project=gcloud_project)

    return GOOGLE_STORAGE_CLIENT


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
            raise GoogleStorageException(e.output)

    # map path to file size in bytes and its last-modified date (eg. "2020-05-20T16:52:01Z")
    def parse_gsutil_date_string(date_string):
        #utc_date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        utc_date = parser.parse(date_string).replace(tzinfo=timezone.utc)
        return utc_date.astimezone(LOCAL_TIMEZONE)

    records = [r.strip().split("  ") for r in gsutil_output.strip().split("\n") if "gs://" in r]
    try:
        path_to_file_stat_dict = {
            r[2]: (int(r[0]), parse_gsutil_date_string(r[1])) for r in records
        }
    except Exception as e:
        raise GoogleStorageException(f"Unable to parse gsutil output for {gs_path_with_wildcards}: {e}\n{gsutil_output}")

    print(f"Found {len(path_to_file_stat_dict):,d} matching paths in {gs_path_with_wildcards}")

    GSUTIL_PATH_TO_FILE_STAT_CACHE[gs_path_with_wildcards] = path_to_file_stat_dict

    return path_to_file_stat_dict


def _path_exists__cached(path, only_check_the_cache=False, verbose=False):
    """Takes a local path or a gs:// Google Storage path and returns True if the path exists.
    The path can contain wildcards (*) in which case this method returns True if at least one matching file or directory
    exists.

    Args:
        path (str): local path or gs:// Google Storage path. The path can contain wildcards (*).
        only_check_the_cache (bool): if True, only check the cache and don't run any file system commands.
        verbose (bool):
    Return:
        bool: True if the path exists.
    """
    if not isinstance(path, str):
        raise ValueError(f"Unexpected path type {type(path)}: {path}")

    if path in PATH_EXISTS_CACHE:
        return PATH_EXISTS_CACHE[path]
    elif only_check_the_cache:
        # if we're only checking the cache and the path isn't in the cache, then we can return False
        return False

    if path.startswith("gs://"):
        if "*" in path:
            path_dict = _generate_gs_path_to_file_stat_dict(path)
            PATH_EXISTS_CACHE[path] = bool(path_dict)
            for path_without_star in path_dict:
                PATH_EXISTS_CACHE[path_without_star] = True
        else:
            PATH_EXISTS_CACHE[path] = hl.hadoop_exists(path)
    else:
        if "*" in path:
            path_dict = glob.glob(path)
            PATH_EXISTS_CACHE[path] = len(path_dict) > 0
            for path_without_star in path_dict:
                PATH_EXISTS_CACHE[path_without_star] = True
        else:
            PATH_EXISTS_CACHE[path] = os.path.exists(path)

    if verbose:
        if PATH_EXISTS_CACHE[path]:
            print(f"Confirmed path exists: {path}")
        else:
            print(f"Missing path: {path}")

    return PATH_EXISTS_CACHE[path]


def _file_stat__cached(path, only_check_the_cache=False, verbose=False):
    """Takes a local file path or gs:// Google Storage path and returns a list of file stats including the size in bytes
    and the modification time.

    Args:
        path (str): local file path or gs:// Google Storage path. The path can contain wildcards (*).
        only_check_the_cache (bool): if True, only check the cache and don't run any file system commands.
        verbose (bool): print more detailed log output

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
    if path in PATH_STAT_CACHE:
        return PATH_STAT_CACHE[path]
    elif only_check_the_cache:
        # if we're only checking the cache and the path isn't in the cache, then we can return False
        return False

    if path.startswith("gs://"):
        if "*" in path:
            path_to_file_stat_dict = _generate_gs_path_to_file_stat_dict(path)
            PATH_STAT_CACHE[path] = []
            for path_without_star, (size_bytes, modification_time) in path_to_file_stat_dict.items():
                stat_results = {
                    "path": path_without_star,
                    "size_bytes": size_bytes,
                    "modification_time": modification_time,
                }
                PATH_STAT_CACHE[path].append(stat_results)
                PATH_STAT_CACHE[path_without_star] = [stat_results]
                PATH_EXISTS_CACHE[path_without_star] = True
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

            if isinstance(stat_results["modification_time"], float):
                stat_results["modification_time"] = datetime.fromtimestamp(
                    stat_results["modification_time"]).replace(tzinfo=LOCAL_TIMEZONE)
            elif isinstance(stat_results["modification_time"], str):
                try:
                    stat_results["modification_time"] = LOCAL_TIMEZONE.localize(
                        parser.parse(stat_results["modification_time"], ignoretz=True))
                except Exception as e:
                    raise Exception(f"Unable to parse 'modification_time' from {stat_results}: {e}")
            elif stat_results["modification_time"] == None:
                raise GoogleStorageException(f"hl.stat returned modification_time == None for {path}")
            else:
                raise GoogleStorageException(f"Unexpected modification_time type: {type(stat_results['modification_time'])} in {stat_results}")

            PATH_STAT_CACHE[path] = [stat_results]
            PATH_EXISTS_CACHE[path] = True

    else:
        if "*" in path:
            local_paths = glob.glob(path)
        else:
            local_paths = [path]

        for local_path in local_paths:
            stat = os.stat(local_path)
            if path not in PATH_STAT_CACHE:
                PATH_STAT_CACHE[path] = []
            stat_results = {
                "path": local_path,
                "size_bytes": stat.st_size,
                "modification_time": datetime.fromtimestamp(stat.st_ctime).replace(tzinfo=LOCAL_TIMEZONE),
            }
            PATH_STAT_CACHE[path].append(stat_results)
            PATH_STAT_CACHE[local_path] = [stat_results]
            PATH_EXISTS_CACHE[local_path] = True

    if verbose:
        for stat_results in PATH_STAT_CACHE[path]:
            try:
                file_size_str = "%0.1f Gb" % (int(stat_results["size_bytes"])/10**9)
            except Exception as e:
                file_size_str = "%s bytes  (%s)" % (stat_results["size_bytes"], str(e))

            last_modified_time = stat_results['modification_time']
            print(f"Checked path stats: {file_size_str:5}  {last_modified_time:15}   {path}")

    return PATH_STAT_CACHE[path]


def are_any_inputs_missing(step, only_check_the_cache=False, verbose=False):
    """Returns True if any of the Step's inputs don't exist"""
    for input_spec in step._input_specs:
        input_path = input_spec.source_path
        if not _path_exists__cached(input_path, only_check_the_cache=only_check_the_cache, verbose=verbose):
            print(f"WARNING: Input missing: {input_path}")
            return True

    return False


def all_outputs_exist(step, only_check_the_cache=False, verbose=False):
    """Returns True if all the Step's output files already exist"""
    paths_to_check = [output_spec.output_path_including_any_wildcards
                      for output_spec in step._output_specs if not output_spec._optional]
    return files_exist(paths_to_check, only_check_the_cache=only_check_the_cache, verbose=verbose)


def files_exist(file_paths, only_check_the_cache=False, verbose=False):
    """Returns True if all the files exist"""
    for file_path in file_paths:
        if not _path_exists__cached(file_path, only_check_the_cache=only_check_the_cache, verbose=verbose):
            return False

    return True


def are_output_files_up_to_date(input_paths, output_paths, only_check_the_cache=False, verbose=False):
    """Returns True if all the output files already exist and are newer than all the input files.

    input_paths (list): gs:// paths of input files
    output_paths (list): gs:// paths of output files
    only_check_the_cache (bool): if True, only check the cache and don't run any file system commands.
    verbose (bool): if True, print more logging output

    Returns:
        bool: True if all output files exist and are newer than all input files
    """
    if len(output_paths) == 0:
        return False

    latest_input_path = None
    latest_input_modified_date = datetime(2, 1, 1, tzinfo=LOCAL_TIMEZONE)
    for input_path in input_paths:
        if not _path_exists__cached(input_path, only_check_the_cache=only_check_the_cache, verbose=verbose):
            raise ValueError(f"Input path doesn't exist: {input_path}")

        stat_list = _file_stat__cached(input_path, only_check_the_cache=only_check_the_cache, verbose=verbose)
        for stat in stat_list:
            if stat["modification_time"] > latest_input_modified_date:
                latest_input_modified_date = stat["modification_time"]
                latest_input_path = stat["path"]

    # check whether any outputs are missing
    oldest_output_path = None
    oldest_output_modified_date = None
    for output_path in output_paths:
        if not _path_exists__cached(output_path, only_check_the_cache=only_check_the_cache, verbose=verbose):
            return False

        stat_list = _file_stat__cached(output_path, only_check_the_cache=only_check_the_cache, verbose=verbose)
        for stat in stat_list:
            if oldest_output_modified_date is None or stat["modification_time"] < oldest_output_modified_date:
                oldest_output_modified_date = stat["modification_time"]
                oldest_output_path = stat["path"]

    if verbose:
        print(f"Newest input    {latest_input_modified_date.strftime(DATE_STRFTIME_FORMAT):20}: {latest_input_path}")
        print(f"Oldest output   {oldest_output_modified_date.strftime(DATE_STRFTIME_FORMAT) if oldest_output_modified_date else '':20}: {oldest_output_path}")

    return oldest_output_modified_date is not None and latest_input_modified_date <= oldest_output_modified_date


def are_outputs_up_to_date(step, only_check_the_cache=False, verbose=False):
    """Returns True if all the Step's outputs already exist and are newer than all inputs"""

    input_paths = [input_spec.original_source_path for input_spec in step._input_specs]
    output_paths = [output_spec.output_path_including_any_wildcards
                    for output_spec in step._output_specs if not output_spec._optional]

    return are_output_files_up_to_date(input_paths, output_paths, only_check_the_cache=only_check_the_cache, verbose=verbose)


class GoogleStorageException(Exception):
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
        verbose (bool): print more detailed log output

    Raises:
        StorageRegionException: If the given gs_path is not stored in one the expected_regions.
    """
    bucket_name = _get_bucket_name(gs_path)
    if _BUCKET_ERROR_COUNTER[bucket_name] > _MAX_ERROR_MESSAGES_TO_PRINT_PER_BUCKET:
        return

    if bucket_name in BUCKET_LOCATION_CACHE:
        location = BUCKET_LOCATION_CACHE[bucket_name]
    else:
        try:
            client = _get_google_storage_client(gcloud_project=gcloud_project)
            bucket = client.get_bucket(bucket_name)
            location = bucket.location
            BUCKET_LOCATION_CACHE[bucket_name] = location
        except Exception as e:
            if not ignore_access_denied_exception or "access" not in str(e).lower():
                raise GoogleStorageException(f"ERROR: Could not determine gs://{bucket_name} bucket region: {e}")

            _BUCKET_ERROR_COUNTER[bucket_name] += 1
            print(f"WARNING: Unable to check bucket region for gs://{bucket_name}: {e}")
            return

    if location not in expected_regions:
        raise GoogleStorageException(
            f"ERROR: gs://{bucket_name} is located in {location} which is not one of the "
            f"expected regions {expected_regions}")
    if verbose:
        print(f"Confirmed gs://{bucket_name} is in {location}")

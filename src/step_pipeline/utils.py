from datetime import datetime, timezone
from dateutil import parser
import hail as hl
import os
import pytz
import subprocess

HADOOP_EXISTS_CACHE = {}
HADOOP_STAT_CACHE = {}
GSUTIL_PATH_TO_FILE_STAT_CACHE = {}

LOCAL_TIMEZONE = pytz.timezone("US/Eastern") #datetime.now(timezone.utc).astimezone().tzinfo


def _generate_gs_path_to_file_stat_dict(glob_path):
    """Runs "gsutil ls -l {glob}" and returns a dictionary that maps each gs:// file to
    its size in bytes. This appears to be faster than running hl.hadoop_ls(..).
    """
    if not isinstance(glob_path, str):
        raise ValueError(f"Unexpected glob_path type {str(type(glob_path))}: {glob_path}")

    if not glob_path.startswith("gs://"):
        raise ValueError(f"{glob_path} path doesn't start with gs://")

    if glob_path in GSUTIL_PATH_TO_FILE_STAT_CACHE:
        return GSUTIL_PATH_TO_FILE_STAT_CACHE[glob_path]

    print(f"Listing {glob_path}")
    try:
        gsutil_output = subprocess.check_output(
            f"gsutil -m ls -l {glob_path}",
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

    GSUTIL_PATH_TO_FILE_STAT_CACHE[glob_path] = path_to_file_stat_dict

    return path_to_file_stat_dict


def _file_exists__cached(path):
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
        HADOOP_EXISTS_CACHE[path] = os.path.exists(path)

    return HADOOP_EXISTS_CACHE[path]


def _file_stat__cached(path):
    """
    Example:

    :param path:
    :return: list of metadata dicts like: [
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
        stat = os.stat(path)
        HADOOP_STAT_CACHE[path] = [{
            "path": path,
            "size_bytes": stat.st_size,
            "modification_time": datetime.fromtimestamp(stat.st_ctime).replace(tzinfo=LOCAL_TIMEZONE),
        }]

    return HADOOP_STAT_CACHE[path]


def are_any_inputs_missing(step, verbose=False) -> bool:
    for input_spec in step._inputs:
        input_path = input_spec["source_path"]
        if not _file_exists__cached(input_path):
            if verbose:
                print(f"Input missing: {input_path}")
            return True

    return False


def are_outputs_up_to_date(step, verbose=False) -> bool:
    """Returns True if all outputs already exist and are newer than all inputs"""

    if len(step._outputs) == 0:
        return False

    latest_input_path = None
    latest_input_modified_date = datetime(2, 1, 1, tzinfo=LOCAL_TIMEZONE)
    for input_spec in step._inputs:
        input_path = input_spec["source_path"]
        if not _file_exists__cached(input_path):
            raise ValueError(f"Input path doesn't exist: {input_path}")

        stat_list = _file_stat__cached(input_path)
        for stat in stat_list:
            latest_input_modified_date = max(latest_input_modified_date, stat["modification_time"])
            latest_input_path = stat["path"]

    # check whether any outputs are missing
    oldest_output_path = None
    oldest_output_modified_date = datetime.now(LOCAL_TIMEZONE)
    for output_spec in step._outputs:
        output_path = output_spec["destination_path"]
        if not _file_exists__cached(output_path):
            return False

        stat_list = _file_stat__cached(output_path)
        for stat in stat_list:
            oldest_output_modified_date = min(oldest_output_modified_date, stat["modification_time"])
            oldest_output_path = stat["path"]

    if verbose:
        print(f"Oldest output ({oldest_output_modified_date}): {oldest_output_path},  "
              f"newest input ({latest_input_modified_date}): {latest_input_path}")

    return latest_input_modified_date <= oldest_output_modified_date


class _GoogleStorageException(Exception):
    pass


def check_gcloud_storage_region(
    gs_path: str,
    expected_regions: tuple = ("US", "US-CENTRAL1"),
    gcloud_project: str = None,
    ignore_access_denied_exception: bool = True,
    verbose: bool = True,
):
    """Checks whether the given google storage path(s) are stored in US-CENTRAL1 - the region where the hail Batch
    cluster is located. Localizing data from other regions will be slower and result in egress charges.

    :param gs_paths: a gs:// path or glob.
    :param gcloud_project: (optional) if specified, it will be added to the gsutil command with the -u arg.
    :param ignore_access_denied_exception: if True, it will ignore
    :raises StorageRegionException: If the given path(s) is not stored in the same region as the Batch cluster.

    """
    if "*" in gs_path:
        gs_paths = [stat["path"] for stat in _file_stat__cached(gs_path)]
    else:
        gs_paths = [gs_path]

    buckets = set([path.split("/")[2] for path in gs_paths])
    for bucket in buckets:
        gsutil_command = f"gsutil"
        if gcloud_project:
            gsutil_command += f" -u {gcloud_project}"

        output = subprocess.check_output(
            f"{gsutil_command} ls -L -b gs://{bucket}; exit 0", shell=True, encoding="UTF-8", stderr=subprocess.STDOUT)

        for line in output.split("\n"):
            if "AccessDeniedException" in line:
                message = f"Unable to check google storage region for gs://{bucket}. Access denied."
                if ignore_access_denied_exception:
                    location = None
                    if verbose:
                        print(message)
                    break
                else:
                    raise _GoogleStorageException(f"ERROR: {message}")
            if "Location constraint:" in line:
                location = line.strip().split()[-1]
                break
        else:
            raise _GoogleStorageException(f"ERROR: Couldn't determine gs://{bucket} bucket region.")

        if location is not None:
            if location not in expected_regions:
                raise _GoogleStorageException(f"ERROR: gs://{bucket} is located in {location} which is not one of the"
                                                    f" expected regions {expected_regions}")

            if verbose:
                print(f"Confirmed gs://{bucket} is in {location}")

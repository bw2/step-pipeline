import datetime

import hail as hl
import logging
import subprocess

HADOOP_EXISTS_CACHE = {}
HADOOP_STAT_CACHE = {}
GSUTIL_PATH_TO_FILE_STAT_CACHE = {}


def _generate_path_to_file_stat_dict(glob_path):
    """Runs "gsutil ls -l {glob}" and returns a dictionary that maps each gs:// file to
    its size in bytes. This appears to be faster than running hl.hadoop_ls(..).
    """
    if glob_path in GSUTIL_PATH_TO_FILE_STAT_CACHE:
        return GSUTIL_PATH_TO_FILE_STAT_CACHE[glob_path]

    logging.info(f"Listing {glob_path}")
    try:
        gsutil_output = subprocess.check_output(
            f"gsutil -m ls -l {glob_path}",
            shell=True,
            stderr=subprocess.STDOUT,
            encoding="UTF-8")
    except subprocess.CalledProcessError as e:
        if "One or more URLs matched no objects." in e.output:
            return {}
        else:
            raise e

    # map path to file size in bytes and its last-modified date (eg. "2020-05-20T16:52:01Z")
    records = [r.strip().split("  ") for r in gsutil_output.strip().split("\n") if not r.startswith("TOTAL: ")]
    path_to_file_stat_dict = {
        r[2]: (int(r[0]), datetime.strptime(r[1], "%Y-%m-%dT%H:%M:%SZ")) for r in records
    }

    GSUTIL_PATH_TO_FILE_STAT_CACHE[glob_path] = path_to_file_stat_dict

    return path_to_file_stat_dict


def _file_exists__cached(path):
    if path not in HADOOP_EXISTS_CACHE:
        if "*" in path:
            HADOOP_EXISTS_CACHE[path] = bool(_generate_path_to_file_stat_dict(path))
        else:
            HADOOP_EXISTS_CACHE[path] = hl.hadoop_exists(path)

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

    if "*" in path:
        path_to_file_stat_dict = _generate_path_to_file_stat_dict(path)
        HADOOP_STAT_CACHE[path] = []
        for path, (size_bytes, modification_time) in path_to_file_stat_dict.items():
            HADOOP_STAT_CACHE[path].append({
                "path": path,
                "size_bytes": size_bytes,
                "modification_time": modification_time,
            })
    else:
        stat_results = hl.hadoop_stat(path)
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
        stat_results["modification_time"] = datetime.datetime.strptime(
            stat_results["modification_time"], '%a %B %d %H:%M:%S %Z %Y')
        HADOOP_STAT_CACHE[path] = [stat_results]

    return HADOOP_STAT_CACHE[path]


def are_any_inputs_missing(step, verbose=False) -> bool:
    for input_path in step._inputs:
        if not _file_exists__cached(input_path):
            if verbose:
                logging.info(f"Input missing: {input_path}")
            return True

    return False


def are_outputs_up_to_date(step, verbose=False) -> bool:
    """Returns True if all outputs already exist and are newer than all inputs"""

    if len(step._outputs) == 0:
        # if a step doesn't have any outputs defined, always run it
        return False

    latest_input_modified_date = datetime.datetime(1, 1, 1)
    for input_path in step._inputs:
        if not _file_exists__cached(input_path):
            raise ValueError(f"Input path doesn't exist: {input_path}")

        stat_list = _file_stat__cached(input_path)
        for stat in stat_list:
            if verbose:
                logging.info(f"Input last modified: {stat['path']}: {input_path}")
            latest_input_modified_date = min(latest_input_modified_date, stat["modification_time"])

    # check whether any outputs are missing
    oldest_output_modified_date = datetime.datetime.now()
    for output_path in step._outputs:
        if not _file_exists__cached(output_path):
            return False

        stat_list = _file_stat__cached(output_path)
        for stat in stat_list:
            if verbose:
                logging.info(f"Output last modified: {stat['path']}: {stat['modification_time']}")
            oldest_output_modified_date = min(oldest_output_modified_date, stat["modification_time"])

    return latest_input_modified_date < oldest_output_modified_date


class _StorageBucketRegionException(Exception):
    pass


def _check_storage_region(
    google_storage_path: str,
    expected_regions: tuple = ("US", "US-CENTRAL1"),
    gcloud_project: str = None,
    verbose: bool = True,
):
    """Checks whether the given google storage path(s) are stored in US-CENTRAL1 - the region where the hail Batch
    cluster is located. Localizing data from other regions will be slower and result in egress charges.

    :param google_storage_paths: a gs:// path or glob.
    :param gcloud_project: (optional) if specified, it will be added to the gsutil command with the -u arg.
    :raises StorageRegionException: If the given path(s) is not stored in the same region as the Batch cluster.

    """
    if "*" in google_storage_path:
        google_storage_paths = [stat["path"] for stat in _file_stat__cached(google_storage_path)]
    else:
        google_storage_paths = [google_storage_path]

    buckets = set([path.split("/")[2] for path in google_storage_paths])
    for bucket in buckets:
        gsutil_command = f"gsutil"
        if gcloud_project:
            gsutil_command += f" -u {gcloud_project}"

        output = subprocess.check_output(f"{gsutil_command} ls -L -b gs://{bucket}", shell=True, encoding="UTF-8")
        for line in output.split("\n"):
            if "Location constraint:" in line:
                location = line.strip().split()[-1]
                break
        else:
            raise _StorageBucketRegionException(f"ERROR: Couldn't determine gs://{bucket} bucket region.")

        if location not in expected_regions:
            raise _StorageBucketRegionException(f"ERROR: gs://{bucket} is located in {location} which is not one of the"
                                                f" expected regions {expected_regions}")

        if verbose:
            print(f"Confirmed gs://{bucket} is in {location}")

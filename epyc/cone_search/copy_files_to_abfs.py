import os

from hipscat.io.file_io.file_io import get_fs


def copy_files_to_abfs():
    target_so = {
        "account_name": "linccdata",
        "account_key": "ezBADSIGArKcI0JNHFdRfLF5S/64ZJcdrbXKbK5GJikF+YAC0hDAhMputN59HA4RS4N3HmjNZgdc+AStBFuQ6Q==",
    }

    cloud_root = "abfs://hipscat/benchmarks/catwise2020/"
    local_root = "/epyc/projects3/sam_hipscat/catwise2020/catwise2020/"

    paths_to_copy = ["catalog_info.json", "_metadata", "_common_metadata"]

    destination_fs, _ = get_fs(cloud_root, storage_options=target_so)
    chunksize = 1024 * 1024

    for rel_path in paths_to_copy:
        local_file = os.path.join(local_root, rel_path)
        destination_fname = os.path.join(cloud_root, rel_path)
        print(f"Copying file {local_file} to {destination_fname}")
        with open(local_file, "rb") as source_file:
            with destination_fs.open(destination_fname, "wb") as destination_file:
                while True:
                    chunk = source_file.read(chunksize)
                    if not chunk:
                        break
                    destination_file.write(chunk)


if __name__ == "__main__":
    # pylint: disable=invalid-name,duplicate-key
    copy_files_to_abfs()

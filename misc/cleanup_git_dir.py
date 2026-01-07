import subprocess
from datetime import datetime
from pathlib import Path

import human_readable


def get_newest_dir_update(directory_path: Path) -> Path | None:
    """
    Finds the newest file (based on modification time) in a given directory.

    Args:
        directory_path: A Path object representing the directory to search.

    Returns:
        A Path object of the newest file, or None if the directory is empty
        or does not exist.
    """
    if not directory_path.is_dir():
        print(f"Error: '{directory_path}' is not a valid directory.")
        return None

    newest_mtime = 0

    for file_path in directory_path.iterdir():
        try:
            mtime = file_path.stat().st_mtime
            if mtime > newest_mtime:
                newest_mtime = mtime
        except OSError as e:
            print(f"Error accessing file '{file_path}': {e}")

    human_readable_mtime = datetime.fromtimestamp(newest_mtime)

    return human_readable.date_time(datetime.now() - human_readable_mtime)


def git_status(sub_dir):
    if sub_dir.is_dir():
        total_content = subprocess.check_output(
            f"git -C {sub_dir} status", stderr=subprocess.STDOUT, text=True, shell=True
        )
        if "nothing to commit, working tree clean" not in total_content:
            print("   *", sub_dir.name)
            for line in total_content.split("\n"):
                print(f"           \033[91m{line}\033[0m")


def do():
    start_path = Path("/home/delucchi/git/")
    paths = [path for path in start_path.iterdir()]
    paths.sort()
    for file_path in paths:
        if file_path.is_dir():
            single_dir = (file_path / ".git").exists()
            sub_dirs = [path for path in file_path.iterdir() if path.is_dir()]
            sub_dirs.sort()

            if single_dir:
                print(file_path.relative_to(start_path))
            else:
                sub_dir_strs = [str(sub_dir.relative_to(file_path)) for sub_dir in sub_dirs]
                print(file_path.relative_to(start_path), "(", ", ".join(sub_dir_strs), ")")

            mtime_timestamp = get_newest_dir_update(file_path)
            print("   * last updated", mtime_timestamp)

            if (file_path / ".git").exists():
                git_status(file_path)
            else:
                for sub_dir in sub_dirs:
                    git_status(sub_dir)

    pass


if __name__ == "__main__":
    do()

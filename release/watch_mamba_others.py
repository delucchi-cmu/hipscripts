import sys

from read_conda_version import wait_for_package, send_email

if __name__ == "__main__":
    if len(sys.argv) == 1:
        raise ValueError("version required")
    if len(sys.argv) == 2:
        version = sys.argv[1]

    wait_for_package("lsdb", version)
    wait_for_package("hats-import", version)

    send_email(f"lsdb {version} found.\nhats-import {version} found.\n\ngood to go.")
    
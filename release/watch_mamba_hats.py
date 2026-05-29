import sys

from read_conda_version import wait_for_package, send_email

if __name__ == "__main__":
    if len(sys.argv) == 1:
        raise ValueError("version required")
    if len(sys.argv) == 2:
        version = sys.argv[1]

    wait_for_package("hats", version)

    send_email(f"hats {version} found.\n\ngood to go.")
    
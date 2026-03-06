import subprocess
import time
import smtplib
from email.message import EmailMessage

def send_email(content_string):
    message = EmailMessage()
    message["From"] = "updates@lsdb.io"
    message["To"] = 'delucchi@andrew.cmu.edu'

    message["Subject"] = "conda build found"
    message.set_content(content_string)

    # Send the message via our own SMTP server.
    with smtplib.SMTP("localhost") as server:
        server.send_message(message)

def wait_for_package(package_name, version):
    found = False
    while(not found):
        found_version = get_version(package_name)
    
        if found_version == version:
            print("great news! found", package_name, version)
            found = True
        else:
            print("found", package_name, found_version, "keep waiting")
            time.sleep(120)

def get_version(package_name):
    total_content = subprocess.check_output(
            f"mamba search {package_name} --channel conda-forge --override-channels", stderr=subprocess.STDOUT, text=True, shell=True
        )
    for line in total_content.split("\n"):
        line = line.strip()
        # print(line)
        if line.startswith(package_name):
            # print(line)
            tokens = line.split()
            return tokens[1]

    return "0.0.0"
import requests
import sys
import re


def reformat_line(input):
    line_pattern = re.compile(r"\* (.*) by @([\w\-]*) in (.*/(\d*))")
    match = line_pattern.match(input)
    if match:
        description = match.group(1)
        pr_author = match.group(2)
        pr_url = match.group(3)
        pr_num = match.group(4)
        if pr_author not in ("dependabot"):
            ## Suppress bot authors
            print(f"* [[{pr_num}]({pr_url})] {description}")
    else:
        print(input)


def reformat(github_org, github_project):
    api_url = f"https://api.github.com/repos/{github_org}/{github_project}/releases/latest"
    response = requests.get(api_url)
    response_json = response.json()
    print("======================")
    print("ORIGINAL CONTENT")
    print("======================")
    print(response_json["html_url"])
    print(response_json["body"])
    print("======================")
    print("REFORMATTED CONTENT")
    print("======================")
    print(f"[{github_project} {response_json['tag_name']}]({response_json['html_url']})")

    change_lines = response_json["body"].splitlines()
    change_lines = [cl for cl in change_lines if cl.startswith("* ")]

    for line in change_lines:
        reformat_line(line)


if __name__ == "__main__":
    github_org = "astronomy-commons"
    github_project = "hats"
    if len(sys.argv) == 2:
        github_project = sys.argv[1]
    elif len(sys.argv) == 3:
        github_org = sys.argv[1]
        github_project = sys.argv[2]

    reformat(github_org, github_project)

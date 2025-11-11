import requests
import sys
import re

## TODO
## - can we link the PRs to the issues they close?
## - Can we cross-link releases in hats to releases in lsdb? or the other way around at least?

def reformat_all(github_org, github_project):
    api_url = f"https://api.github.com/repos/{github_org}/{github_project}/releases"
    response = requests.get(api_url)
    response_json = response.json()
    line_pattern = re.compile(r"\* (.*) by @([\w\-]*) in (.*/(\d*))")

    for release_json in response_json:
        print("")
        print(f"{github_project.upper()} {release_json['tag_name']}  ({release_json['published_at'][0:10]})")
        print("==========================================")
        print("")
        for category in ["Features","Bugfixes","Deprecations and Removals","Misc","Improved Documentation"]:
            print(category)
            print("-------------------------")
            print("")


        change_lines = release_json["body"].splitlines()
        change_lines = [cl for cl in change_lines if cl.startswith("* ")]

        extra_lines = []
        print("TODO - CATEGORIZE")
        print("")
        for line in change_lines:
            match = line_pattern.match(line)
            if match:
                description = match.group(1)
                pr_author = match.group(2)
                pr_url = match.group(3)
                pr_num = match.group(4)
                if pr_author not in ("dependabot"):
                    ## Suppress bot authors
                    print(f"- {description} (`#{pr_num} <{pr_url}>`__)")
            elif "dependabot" not in line:
                extra_lines.append(line)

        if extra_lines:
            print("")
            print("TODO - CONFIRM")
            print("")
            print("New Contributors")
            print("-------------------------")
            print("")
            for line in extra_lines:
                print(line)
        print("")


if __name__ == "__main__":
    github_org = "astronomy-commons"
    github_project = "hats"
    if len(sys.argv) == 2:
        github_project = sys.argv[1]
    elif len(sys.argv) == 3:
        github_org = sys.argv[1]
        github_project = sys.argv[2]

    reformat_all(github_org, github_project)

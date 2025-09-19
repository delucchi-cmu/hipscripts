import os

import questionary
from contextlib import chdir
project_list = ["hats", "hats-import", "lsdb"]
github_organization = "astronomy-commons"



def do_questions():
    release_semver = questionary.text("Give me a release semver:").ask()
    subdir=f"release_{release_semver}"
    project_choice = questionary.checkbox(
        "Which projects are you releasing today?",
        choices=project_list,
    ).ask()

    os.system(f"mkdir {subdir}")
    with chdir(subdir):

        for project in project_choice:
            print("Initializing release for", project)
            os.system(f"git clone http://github.com/{github_organization}/{project}")
            ## Add to the markdown release notes.
            with open(f"towncrier_md_{project}.toml", "w", encoding="utf-8") as file_handle:
                file_handle.write(f"""
[tool.towncrier]
directory = "{project}/changes"
filename = "NEWS.rst"
package = "{project}"
package_dir = "{project}/src"
start_string = "<!-- towncrier release notes start -->"
underlines = ["", "", ""]
issue_format = "[#{{issue}}](https://github.com/{github_organization}/{project}/issues/{{issue}})"
template="../template.md"

[[tool.towncrier.type]]
directory = "feature"
name = "Features"
showcontent = true

[[tool.towncrier.type]]
directory = "removal"
name = "Deprecations and Removals"
showcontent = true

[[tool.towncrier.type]]
directory = "misc"
name = "Misc"
showcontent = true

[[tool.towncrier.type]]
directory = "bugfix"
name = "Bugfixes"
showcontent = true

[[tool.towncrier.type]]
directory = "doc"
name = "Improved Documentation"
showcontent = true""")
            with chdir(project):
                os.system(f"towncrier build --version {release_semver} --draft --config ../towncrier_md_{project}.toml >> ../news.md")
                os.system(f"towncrier build --version {release_semver}")



if __name__ == "__main__":
    do_questions()

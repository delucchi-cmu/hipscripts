import os

import questionary

project_list = ["hats", "hats-import", "lsdb", "hats-cloudtests"]


def do_questions():
    subdir = questionary.text("Give me a subdirectory name:").ask()
    project_choice = questionary.checkbox(
        "Which projects are you installing today?",
        choices=project_list,
    ).ask()
    run_pytest = questionary.confirm(
        "Should we run pytest on each project?", default=False
    ).ask()
    using_branches = questionary.confirm(
        "Using any special branches today?", default=False
    ).ask()
    if using_branches:
        branches = []
        for project in project_choice:
            branches.append(
                questionary.text(f"What's the branch for  --   {project}").ask()
            )

    commands = []
    ## 1 - create parent dir
    os.system(f"mkdir {subdir}")
    os.chdir(subdir)

    ## 2- Check everything out
    if using_branches:
        for pair in zip(project_choice, branches):
            project = pair[0]
            branch = pair[1]
            if not branch:
                commands.append(
                    f"git clone http://github.com/astronomy-commons/{project}"
                )
            else:
                commands.append(
                    f"git clone -b {pair[1]} http://github.com/astronomy-commons/{project}"
                )
    else:
        for project in project_choice:
            commands.append(f"git clone http://github.com/astronomy-commons/{project}")

    ## 3 - pip install everything
    for project in project_choice:
        commands.append(f"pip install -e ./{project}")

    ## 4 - optionally run all unit tests
    if run_pytest:
        for project in project_choice:
            commands.append(f"pytest ./{project}")

    for command in commands:
        print("RUNNING COMMAND:", command)
        os.system(command)


if __name__ == "__main__":
    do_questions()

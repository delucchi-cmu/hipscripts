import numpy as np
import pandas as pd
from tabulate import tabulate


def make_guesses():
    schedule_in = pd.read_csv("./interrupts.csv")
    past_schedule = schedule_in["Name"]

    team = ["Doug", "Kostya", "Melissa", "Olivia", "Sandro", "Sean", ""]
    past_schedule = past_schedule[past_schedule.isin(team)]

    for new_index in range(past_schedule.index[-1] + 1, schedule_in.index[-1] + 1):
        print(new_index, "- Finding best fit for ", schedule_in.loc[new_index, "Week"])
        conflicts = schedule_in.loc[new_index, "Conflict"]
        if isinstance(conflicts, str) and len(conflicts) > 0:
            conflicts = [item.strip() for item in conflicts.split(",")]
            print("   - respecting conflicts for", conflicts)
        else:
            conflicts = []

        line_up = [name for name in past_schedule.dropna().to_numpy() if name not in conflicts]
        names, counts = np.unique(line_up, return_counts=True)

        ## Find folks who have been on interrupts the fewest times, and break ties with
        ## who has gone the longest without a rotation.
        min_count = counts.min()

        next_ups = [names[i] for i, count in enumerate(counts) if count == min_count]
        if len(next_ups) == 0:
            print("Well shit")

        reversed_arr_slicing = list(past_schedule[::-1].to_numpy())

        weeks_since_on = [reversed_arr_slicing.index(name) for name in next_ups]

        next_up = next_ups[np.argmax(weeks_since_on)]
        print("   - selected", next_up, "from list", ", ".join(next_ups))
        next_up_series = pd.Series([next_up], index=[new_index])
        past_schedule = pd.concat([past_schedule, next_up_series])

    schedule_in["Name"] = past_schedule
    schedule_in = schedule_in.where(pd.notnull(schedule_in), None)
    print(tabulate(schedule_in[["Week", "Name"]], headers="keys", showindex=False, missingval=""))


if __name__ == "__main__":
    make_guesses()

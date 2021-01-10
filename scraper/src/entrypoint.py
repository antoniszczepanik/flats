#!/usr/bin/env python3
"""
Runs a series or one of tasks.

Usage:
    run_task.py TASK OFFER_TYPE [--use-remote]

Options:
    --use-remote  Use remote connection, not local minio instance
Arguments:
    TASK  name of the task to be executed.
    OFFER_TYPE for which offer type processing should be executed (rent/sale)

Examples:
    run_task.py clean sale

Tasks available:
{tasks}
"""
import os
import pathlib
import subprocess
import sys
from docopt import docopt

TASK_FUNCTIONS = {
    # Scrape data, get newest scraping date from last scrape file
    "scrape": "from pipelines.scrape_task import task; task('{offer_type}')",
    # Do all processing, get all files newer then last final file
    "process": "from pipelines.process import process; process('{offer_type}')",
    # on demand
    "coord-map": "from pipelines.on_demand.coords_map_task import coords_map_task; coords_map_task('{offer_type}')",
    "unify-raw": "from pipelines.on_demand.unify_raw_task import unify_raw_data_task; unify_raw_data_task('{offer_type}')",
    #"monitor": "from pipelines.on_demand.monitor import monitor; monitor('{offer_type}')",
}

def run_command(task_function: str, offer_type: str = None):
    if offer_type:
        task_function = task_function.format(offer_type=offer_type)
    cmd = ["python3", "-c", task_function]
    print(" ".join(cmd))
    subprocess.run(cmd)

if __name__ == "__main__":
    args = docopt(
        __doc__.format(
            tasks='    ' + '\n    '.join(TASK_FUNCTIONS.keys())
        )
    )
    task = args['TASK']
    if task not in TASK_FUNCTIONS and task != "all":
        print(f'Invalid task name ({task}). See --help for list of supported tasks.')
        exit(0)

    offer_type = args.get('OFFER_TYPE')
    if offer_type not in ('sale', 'rent', None):
        print(f'Invalid offer type ({offer_type}).')

    if not args['--use-remote']:
        os.environ["USE_MINIO"] = 'true'

    if task == "all":
        for task_fn in ALL_TASKS:
            run_command(task_fn, offer_type=offer_type)
    else:
        task_fn = TASK_FUNCTIONS[task]
        run_command(task_fn, offer_type=offer_type)

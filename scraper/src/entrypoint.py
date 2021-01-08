#!/usr/bin/env python3
"""
Runs a series or one of tasks.

Usage:
    run_task.py TASK OFFER_TYPE [--use-remote]

Options:
    --use-remote  Use remote connection, not local minio instance
Arguments:
    TASK  name of the task to be executed.

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
    "scrape": "from pipelines.scrape_task import scrape_task; scrape_task('{offer_type}')",
    "concat": "from pipelines.concat_task import concat_data_task; concat_data_task('{offer_type}')",
    "clean": "from pipelines.cleaning_task import cleaning_task; cleaning_task('{offer_type}')",
    "features": "from pipelines.feature_engineering_task import feature_engineering_task; feature_engineering_task('{offer_type}')",
    "apply": "from pipelines.apply_task import apply_task; apply_task('{offer_type}')",
    "prepare-final": "from pipelines.prepare_final_data import task; task()",
    # not under cron
    "coord-map": "from pipelines.coords_map_task import coords_map_task; coords_map_task('{offer_type}')",
    "unify-raw": "from pipelines.unify_raw_task import unify_raw_data_task; unify_raw_data_task('{offer_type}')",
    "monitor": "from pipelines.monitor import monitor; monitor('{offer_type}')",
}

ALL_TASKS = [TASK_FUNCTIONS[task] for task in ["scrape", "concat", "clean", "features", "apply", "prepare-final"]]


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

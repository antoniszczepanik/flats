#!/usr/bin/env python3
"""
Runs task in Airflow container.

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
try:
    from docopt import docopt
except ModuleNotFoundError as e:
    print("Please install docopt first (run `[sudo] pip install docopt`).")
    sys.exit(1)

TASK_FUNCTIONS = {
    "scrape": "from pipelines.scrape_task import scrape_task; scrape_task('{offer_type}')",
    "concat": "from pipelines.concat_task import concat_data_task; concat_data_task('{offer_type}')",
    "clean": "from pipelines.cleaning_task import cleaning_task; cleaning_task('{offer_type}')",
    "features": "from pipelines.feature_engineering_task import feature_engineering_task; feature_engineering_task('{offer_type}')",
    "apply": "from pipelines.apply_task import apply_task; apply_task('{offer_type}')",
    "update-data": "from pipelines.update_website_data import update_website_data_task; update_website_data_task()",
    # on demand - not run on airflow
    "coord-map": "from pipelines.coords_map_task import coords_map_task; coords_map_task('{offer_type}')",
    "unify-raw": "from pipelines.unify_raw_task import unify_raw_data_task; unify_raw_data_task('{offer_type}')",
    "monitor": "from pipelines.monitor import monitor; monitor('{offer_type}')",
}

if __name__ == "__main__":
    args = docopt(
        __doc__.format(
            tasks='    ' + '\n    '.join(TASK_FUNCTIONS.keys())
        )
    )
    task = args['TASK']
    if task not in TASK_FUNCTIONS:
        print(f'Invalid task name ({task}). See --help for list of supported tasks.')
        exit(0)

    offer_type = args.get('OFFER_TYPE')
    if offer_type not in ('sale', 'rent', None):
        print(f'Invalid offer type ({offer_type}).')

    cmd = ["docker", "run", "-it", "--rm"]

    cwd = os.getcwd()
    home = str(pathlib.Path.home())

    cmd.extend(["-v", f"{cwd}/code:/usr/local/code"])
    cmd.extend(["-v", f"{home}/.aws/credentials:/usr/local/airflow/.aws/credentials:ro"])
    if not args['--use-remote']:
        cmd.extend(["-e", "USE_MINIO=true"])
    cmd.extend(["--network=flats"])
    cmd.extend(["--name=flats"])
    cmd.extend(["flats"])


    python_command = TASK_FUNCTIONS[task]
    if offer_type == None:
        cmd.extend(["python3", "-c", python_command])
    else:
        cmd.extend(["python3", "-c", python_command.format(offer_type=offer_type)])

    print(" ".join(cmd))
    subprocess.run(cmd)

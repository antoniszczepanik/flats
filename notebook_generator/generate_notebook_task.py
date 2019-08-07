""" See analysis_report dag for description. """
import datetime
import logging
from pathlib import Path

import nbformat as nbf
from nbconvert import HTMLExporter
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor

log = logging.getLogger(__name__)

SELF_DIR = Path(__file__).resolve().parent
MD_DIR = SELF_DIR / 'markdown_descriptions'


def notebook_generator():
    """
	Generate notebook with stats from "clean" data.
    """
    now = datetime.datetime.now()
    title = f'# Analysis Report - **{vessel_slug}**\n{now}'
    header = read_md('header.md')

    first_cell_code = (
        '%matplotlib inline\n'
        'from IPython.core.display import display, HTML\n'
        'display(HTML("<style>.container {width:90% !important;}</style>"))'
        "\nimport sys\n"
        f"if '{SELF_DIR}' not in sys.path:\n"
        f"    sys.path.append('{SELF_DIR}')\n"
        "import plot_charts\n"
        "import print_stats\n"
        "import get_stats\n"
        "from read_data import get_vessel_data\n"
        "vessel_data = get_vessel_data.get_vessel_data("
        f"    vessel_slug, vessel_imo, '{customer_slug}')\n"
        "stats = get_stats.get_vessel_stats(vessel_slug, vessel_data)\n"
        "events = stats['events']\n"
        "final_stats = stats['final']\n"
        "imported_stats = stats['imported']\n"
        "advanced_stats = stats['advanced']\n")

    cell_pairs = (
        ('advanced_stats.md',
         "print_stats.print_header(vessel_slug, vessel_imo)\n"
         "print_stats.print_advanced_stats(advanced_stats)"),
        ('imported_stats.md',
         "print_stats.print_imported_stats(imported_stats)"),
        ('final_stats.md', "print_stats.print_final_stats(final_stats)"),
        ('events.md', "print_stats.print_events(events)"),
        ('possible_fa.md',
         "plot_charts.plot_missing_signals(vessel_slug, events, vessel_data)"),
        ('power_fuel_speed_scatter.md',
         "plot_charts.plot_power_fuel_to_speed(vessel_slug, vessel_data, "
         "stats['limits'])"),
        ('synchronization.md',
         "plot_charts.plot_synchronization(vessel_slug, vessel_data)"),
        ('histograms.md', "plot_charts.plot_histograms(vessel_data)"),
        ('event_plots.md', "plot_charts.plot_before_and_after_events_scatters("
         "vessel_slug, events, vessel_data)"))

    nb = nbf.v4.new_notebook()
    nb['cells'] = [
        nbf.v4.new_markdown_cell(title),
        nbf.v4.new_markdown_cell(header),
        nbf.v4.new_code_cell(first_cell_code)
    ]

    for md_file, code_cell in cell_pairs:
        nb['cells'].append(nbf.v4.new_markdown_cell(read_md(md_file)))
        nb['cells'].append(nbf.v4.new_code_cell(code_cell))

    ep = ExecutePreprocessor(kernel='python3', timeout=600)
    try:
        ep.preprocess(nb, {})
    except CellExecutionError as e:
        log.error('Error executing the notebook for %s. Error: %s',
                  vessel_slug, e)
        raise

    html_exporter = HTMLExporter()
    html_exporter.exclude_input = True
    html_data, _ = html_exporter.from_notebook_node(nb)

    path = config.S3_ANALYSIS_REPORT.format(vessel_slug)
    utils.write_html_to_s3(html_data, path)

    log.info('Done')


def read_md(filename: str):
    with open(MD_DIR / filename, 'r') as f:
        return f.read()

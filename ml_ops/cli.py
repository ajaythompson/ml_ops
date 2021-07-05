"""Console script for ml_ops."""
from pyspark.sql.session import SparkSession
from ml_ops.workflow import InMemoryWFRepository, \
    SparkWorkflowManager, Workflow
import sys

import click
import yaml


@click.command()
def main(args=None):
    """Console script for ml_ops."""
    click.echo("Replace this message by putting your code into "
               "ml_ops.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


@click.command()
@click.option('--workflow-path', required=True, help='Path of workflow.')
def run_workflow(workflow_path):

    spark = SparkSession.builder.getOrCreate()

    with open(workflow_path) as f:
        workflow_config = yaml.load(f, Loader=yaml.SafeLoader)

    workflow = Workflow.get_workflow(workflow_config)

    wf_repo = InMemoryWFRepository()
    wf_manager = SparkWorkflowManager(wf_repo)

    wf_repo.update_workflow(workflow.id, workflow)
    wf_manager.run(workflow.id, spark)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

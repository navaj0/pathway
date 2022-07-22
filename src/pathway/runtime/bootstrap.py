import boto3
import os

from pathway.util import check_output, Timer
from pathway.runtime.dataset import parse_s3_url


def bootstrap(runtime_env_definition: str):
    base_name = os.path.basename(runtime_env_definition)
    _download_env_definition(runtime_env_definition, base_name)

    if base_name == 'requirements.txt':
        _pip_install(base_name)
    elif base_name == 'env.yml':
        _conda_update(base_name)


def _conda_update(config_yaml: str):
    package_manager = os.environ.get('PACKAGEMANAGER', 'conda')
    with Timer(name=f'{package_manager} update'):
        check_output(f'{package_manager} env update -f {config_yaml} --prune')
    with Timer(name='pip install pathway'):
        check_output(f'pip install .pathway.tgz')


def _pip_install(requirements: str):
    with Timer(name='pip install'):
        check_output(f'pip install -r {requirements}')


def _download_env_definition(s3_location: str, local_path: str):
    with Timer(name=f"loading env definition {s3_location}"):
        bucket, object_key = parse_s3_url(s3_location)

        session = boto3.session.Session()
        s3_resource = session.resource('s3')

        s3_resource.Bucket(bucket).download_file(object_key, local_path)
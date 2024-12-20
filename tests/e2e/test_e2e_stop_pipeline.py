import tempfile

import yaml

from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.models import PipelineReturnStatus


def test_e2e_pipeline_should_stop():

    with tempfile.NamedTemporaryFile(delete=False) as temp:

        BIZON_CONFIG_DUMMY_TO_FILE = f"""
        name: test_job_3

        source:
          name: dummy
          stream: creatures

          sleep: 2 # Add sleep to make sure the pipeline is stopped before it finishes

          authentication:
            type: api_key
            params:
              token: dummy_key

        destination:
          name: file
          config:
            filepath: {temp.name}

        transforms:
          - label: transform_data
            python: |
              if 'name' in data:
                data['name'] = data['this_key_doesnt_exist'].upper()
        """

        runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))

        status = runner.run()

    assert status.producer == PipelineReturnStatus.KILLED_BY_RUNNER
    assert status.consumer == PipelineReturnStatus.TRANSFORM_ERROR

import os
from time import sleep

import yappi

from bizon.cli.parser import parse_from_yaml
from bizon.engine.runner import RunnerFactory

raw_config = parse_from_yaml("/Users/antoineballiet/Documents/GitHub/bizon/bizon/sources/hubspot/config/hubspot.yml")

runner = RunnerFactory.create_from_config_dict(config=raw_config)

yappi.start()
runner.run()

while runner.is_running:
    sleep(1)

yappi.stop()

# retrieve thread stats by their thread id (given by yappi)
threads = yappi.get_thread_stats()

for thread in threads:
    print("Function stats for (%s) (%d)" % (thread.name, thread.id))
    yappi.get_func_stats(ctx_id=thread.id).print_all()

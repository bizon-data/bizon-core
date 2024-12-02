import os

from bizon.engine.engine import RunnerFactory

if __name__ == "__main__":
    runner = RunnerFactory.create_from_yaml(
        filepath=os.path.abspath("bizon/sources/kafka/config/kafka_teams_users.yml")
    )
    runner.run()

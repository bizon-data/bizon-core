from google.cloud import bigquery


class BigQueryClient:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=self.project_id)

from typing import Optional

from ...resolver import AbstractReferenceResolver, ReferenceResolutionError


class GSMResolver(AbstractReferenceResolver):
    """Resolves ``gsm://`` references against Google Secret Manager using ADC.

    Path forms:
    - ``<id>``                         -> projects/<project>/secrets/<id>/versions/latest
    - ``<id>/versions/<N>``            -> projects/<project>/secrets/<id>/versions/<N>
    - ``projects/.../versions/<N>``    -> used as-is (full resource name, pins everything)

    Authentication uses Application Default Credentials (workload identity / ambient creds).
    """

    scheme = "gsm"

    def __init__(self, project_id: Optional[str] = None):
        try:
            from google.cloud import secretmanager
        except ImportError as error:
            raise ReferenceResolutionError(
                "gsm:// references require the Google Secret Manager client. "
                "Install it with: pip install 'bizon[secretmanager]'."
            ) from error

        self._client = secretmanager.SecretManagerServiceClient()
        self._project_id = project_id or self._default_project()

    @staticmethod
    def _default_project() -> Optional[str]:
        try:
            import google.auth

            _, project = google.auth.default()
            return project
        except Exception:
            return None

    def _resource_name(self, path: str) -> str:
        path = path.strip()
        if path.startswith("projects/"):
            return path

        if "/versions/" in path:
            secret_id, version = path.split("/versions/", 1)
        else:
            secret_id, version = path, "latest"

        if not self._project_id:
            raise ReferenceResolutionError(
                f"cannot resolve gsm://{path}: no GCP project. Set 'secrets.gsm.project_id' "
                "in the config, the GOOGLE_CLOUD_PROJECT env var, or use a full "
                "'gsm://projects/<project>/secrets/<id>/versions/<n>' path."
            )
        return f"projects/{self._project_id}/secrets/{secret_id}/versions/{version}"

    def resolve(self, path: str) -> str:
        name = self._resource_name(path)
        try:
            response = self._client.access_secret_version(name=name)
        except Exception as error:
            raise ReferenceResolutionError(f"could not access '{name}': {error}") from error
        return response.payload.data.decode("UTF-8")

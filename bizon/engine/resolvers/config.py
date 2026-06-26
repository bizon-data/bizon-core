from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class GSMSettings(BaseModel):
    """Provider defaults for Google Secret Manager (gsm://) references."""

    model_config = ConfigDict(extra="forbid")

    project_id: Optional[str] = Field(
        default=None,
        description="GCP project hosting the secrets. Optional: falls back to the "
        "Application Default Credentials project / GOOGLE_CLOUD_PROJECT when omitted.",
    )


class SecretsConfig(BaseModel):
    """Optional provider defaults for reference resolution.

    The reference scheme (gsm://, env://, ...) identifies the provider, so this block
    only carries optional per-provider settings and is itself optional.
    """

    model_config = ConfigDict(extra="forbid")

    gsm: Optional[GSMSettings] = Field(
        default=None,
        description="Defaults for Google Secret Manager (gsm://) references.",
    )
    # awssm: Optional[AWSSMSettings] = None  # future

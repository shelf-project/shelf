"""Runtime configuration for the Shelf trainer.

Loaded from environment variables (``SHELF_TRAINER_*``) and/or a ``.env``
file. Values are validated via pydantic. Nothing in this module does I/O
at import time.

The promotion-threshold defaults come from ADR-0003 (LightGBM escape hatch):
a candidate model is promoted only if replay benchmarks show ≥ 5 pp hit-rate
lift over size-threshold with < 50 µs p99 inference latency.
"""

from __future__ import annotations

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class TrainerSettings(BaseSettings):
    """Top-level trainer config.

    All fields are overridable via environment variables prefixed
    ``SHELF_TRAINER_``. Example: ``SHELF_TRAINER_TRINO_HOST=rep0.trino.svc``.
    """

    model_config = SettingsConfigDict(
        env_prefix="SHELF_TRAINER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    trino_host: str = Field(
        default="localhost",
        description=(
            "Trino coordinator host used for query-log reads. Set via "
            "SHELF_TRAINER_TRINO_HOST or the .env file in production."
        ),
    )
    trino_port: int = Field(default=443)
    trino_user: str = Field(default="shelf_trainer")
    trino_catalog: str = Field(default="cdp")
    trino_schema: str = Field(default="trino_logs")
    trino_http_scheme: str = Field(default="https")

    s3_config_bucket: str = Field(
        default="shelf-config-dev",
        description="Bucket holding pin_list.json and admission_v*.{txt,meta.json}.",
    )
    s3_config_prefix: str = Field(default="shelf/")
    s3_region: str = Field(default="us-east-1")

    pin_list_top_n: int = Field(
        default=200,
        description="Top-N tables (intersection of 7/30/90d windows) to emit.",
    )
    pin_list_exclude_users: tuple[str, ...] = Field(
        default=("airflow_user", "dbt_user"),
        description="ETL writer users whose tables are never pinned.",
    )

    canary_fraction: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Fraction of admission decisions served by candidate model.",
    )
    promote_hit_rate_delta_pp: float = Field(
        default=5.0,
        description="Minimum replay-benchmark hit-rate lift (pp) required to promote (ADR-0003).",
    )
    promote_p99_latency_us_max: float = Field(
        default=50.0,
        description="Maximum admissible p99 inference latency in microseconds (ADR-0003).",
    )
    promote_coverage_min: float = Field(
        default=0.95,
        description="Minimum fraction of large-miss decisions the candidate must cover.",
    )

    grafana_url: HttpUrl | None = Field(default=None)

    def trino_dsn(self) -> str:
        """Return a string that ``trino.dbapi.connect`` can consume via ``host=``.

        Kept as a method (not a property) so mypy --strict does not trip on
        pydantic's computed-field surface.
        """
        return f"{self.trino_http_scheme}://{self.trino_host}:{self.trino_port}"

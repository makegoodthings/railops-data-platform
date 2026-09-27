from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    source_db_host: str = "localhost"
    source_db_port: int = 5433
    source_db_name: str = "railops"
    source_db_user: str = "railops"
    source_db_password: str = "railops_local_only"
    warehouse_path: Path = Path("data/warehouse.duckdb")
    log_level: str = "INFO"

    @property
    def source_dsn(self) -> str:
        return (
            f"host={self.source_db_host} port={self.source_db_port} "
            f"dbname={self.source_db_name} user={self.source_db_user} "
            f"password={self.source_db_password}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


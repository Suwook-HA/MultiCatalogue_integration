from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    cache_ttl_seconds: int = 300
    portal_timeout_seconds: int = 10
    field_mapping_threshold: float = 0.75
    data_go_kr_api_key: str = ""
    debug: bool = False

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()

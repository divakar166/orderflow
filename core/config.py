from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    KAFKA_BOOTSTRAP_SERVERS: str

    KAFKA_TOPIC_CREATED: str = "orders.created"
    KAFKA_TOPIC_PROCESSING: str = "orders.processing"
    KAFKA_TOPIC_FAILED: str = "orders.failed"
    KAFKA_TOPIC_COMPLETED: str = "orders.completed"

    VALIDATION_PASS_RATE: float = 0.90
    INVENTORY_PASS_RATE: float = 0.85
    PAYMENT_PASS_RATE: float = 0.80

    MAX_RETRY_COUNT: int = 3

    class Config:
        env_file = ".env"


settings = Settings()
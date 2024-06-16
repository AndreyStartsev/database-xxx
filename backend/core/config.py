from starlette.config import Config
from starlette.datastructures import Secret

APP_VERSION = "0.0.1"
APP_NAME = "DB-XXX"
API_PREFIX = "/api"

config = Config(".env")

API_KEY_USER: Secret = config("API_USER_KEY", cast=Secret, default="secret")
API_KEY_APP: Secret = config("API_APP_KEY", cast=Secret, default="admin")
API_DB_KEY: Secret = config("API_DB_KEY", cast=Secret, default="admin")
DEBUG: bool = config("DEBUG", cast=bool, default=False)

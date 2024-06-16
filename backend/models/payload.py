from pydantic import BaseModel


class DatabaseDataPayload(BaseModel):
    data: str


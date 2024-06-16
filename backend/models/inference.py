from pydantic import BaseModel, Field
from typing import List, Union
from backend.services.ml_model import ENTITIES_TO_HIDE, DatabaseDataPayload


class ConfigNER(BaseModel):
    aggressive: bool = Field(default=False, description="Whether to use aggressive NER search (max recall).")
    placeholder: Union[str, None] = Field(default=None, description="The placeholder to use for anonymization, "
                                                                    "if None - then entity name will be used.")
    entities_to_hide: List[str] = Field(default=ENTITIES_TO_HIDE, description="A list of entities to hide.")
    fuzzy_match: bool = Field(default=False, description="Whether to use fuzzy matching for names.")
    per_list_label: bool = Field(default=False, description="Whether to use different label for names from the list")
    remove_html: bool = Field(default=False, description="Whether to remove html tags from the text.")


class BatchInput(BaseModel):
    texts: List[DatabaseDataPayload] = Field(default_factory=list, description="A list of texts to anonymize.")


class TextInput(DatabaseDataPayload):
    data: str = Field(default="", description="A text to anonymize.")


class NameList(BaseModel):
    names: List[str] = Field(default_factory=list, description="A list of predefined names to use for anonymization.")

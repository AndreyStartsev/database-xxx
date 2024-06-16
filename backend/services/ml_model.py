import os
import spacy
from spacy import displacy
from typing import List, Union

from backend.core.messages import NO_VALID_PAYLOAD
from backend.models.payload import DatabaseDataPayload
from backend.services.pd_generator import PersonalDataGenerator
from backend.services.hide_data import hide_ents_in_doc

MODELS_PATH = os.getenv("MODELS_PATH", "./backend/checkpoints/")
DEFAULT_MODEL = os.getenv("SPACY_MODEL", "ru_core_news_md")
BEST_MODEL = os.getenv("BEST_MODEL", "model_018")
ENTITIES_TO_HIDE = ["SENSITIVE", "CONTACTS", "DATE", "LOC", "ORG", "PER"]


class ModelConfig(object):
    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name


class TestModel(object):
    def __init__(self):
        self.model = {"model": "test"}
        self.model_name = "test"

    def __call__(self, text, *args, **kwargs):
        return text


class SpacyModel:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SpacyModel, cls).__new__(cls)
        return cls._instance

    def __init__(self, model_name: str = BEST_MODEL):
        print("Initializing Spacy Model...")
        self.model_name = model_name
        model_full_path = MODELS_PATH + model_name
        self.model = spacy.load(model_full_path)
        self.pd_generator = PersonalDataGenerator(consistency=True)
        self.is_loaded = True

    def __enter__(self):
        # Add any setup actions here if necessary
        print("Entering context: SpacyModel is ready to use.")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Add any teardown or cleanup actions here
        print("Exiting context: Cleaning up SpacyModel resources.")
        # For instance, you can handle exceptions here if needed
        if exc_type:
            print(f"An exception occurred: {exc_value}")
        # Return False to propagate exceptions outside of the context manager
        return False

    def predict(self, payload: DatabaseDataPayload, use_rules=True, use_base_model=False,
                placeholder=None, ents_to_hide=None, checklist=None, filters=None, fuzzy_match=False,
                per_list_label=False, remove_html=False):
        if not self.is_loaded:
            raise ValueError("Model not loaded")
        if not payload:
            raise ValueError(NO_VALID_PAYLOAD)
        if payload.data is None:
            return {"personal_data": [], "text": "", "original_text": ""}

        # Process text with the model
        doc = self.model(payload.data)

        # Post-process the results with custom rules
        ents = self._post_process(doc, jsonify=True, use_rules=use_rules, use_base_model=use_base_model,
                                  checklist=checklist, filters=filters, fuzzy_match=fuzzy_match,
                                  per_list_label=per_list_label)

        # Hide sensitive data
        txt = hide_ents_in_doc(doc, placeholder=placeholder, ents_to_hide=ents_to_hide)

        return {"personal_data": ents, "text": txt, "original_text": doc.text}

    def predict_batch(self, batch: List[DatabaseDataPayload], use_rules=True, use_base_model=False,
                      placeholder=None, ents_to_hide=None, checklist=None, filters=None, fuzzy_match=False,
                      per_list_label=False, remove_html=False):
        if not self.is_loaded:
            raise ValueError("Model not loaded")
        if not batch:
            raise ValueError(NO_VALID_PAYLOAD)
        docs = self.model.pipe(batch)
        ents_list, txt_list = [], []
        for doc in docs:
            ents = self._post_process(doc, jsonify=True, use_rules=use_rules, use_base_model=use_base_model,
                                      checklist=checklist, filters=filters, fuzzy_match=fuzzy_match,
                                      per_list_label=per_list_label)
            txt = hide_ents_in_doc(doc, placeholder=placeholder, ents_to_hide=ents_to_hide,
                                   pd_generator=self.pd_generator)
            ents_list.append(ents)
            txt_list.append(txt)

        return {"personal_data": ents_list, "text": txt_list, "original_text": batch}

    @staticmethod
    def _post_process(spacy_doc, base_doc=None, use_rules=True, use_base_model=False, jsonify=False,
                      checklist=None, filters=None, fuzzy_match=False, per_list_label=False):
        if checklist is None:
            checklist = 'DEFAULT'
        if use_base_model:
            pass
        ents = [(ent.start_char, ent.end_char, ent.label_, ent.text) for ent in spacy_doc.ents]

        if jsonify:
            ents = [{"start": start, "end": end, "label": label, "text": text}
                    for start, end, label, text in ents]
        return ents

    @staticmethod
    def _render_html(spacy_doc):
        return displacy.render(spacy_doc, style="ent", jupyter=False)

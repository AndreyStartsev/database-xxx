import os.path
from typing import List, Union
from loguru import logger

from .contacts.rules_contacts import find_contacts
from .other.rules_other import find_sensitive
from .dates.rules_dates import find_dates
from .per.rules_names import find_names
# from .per.base_nlp import find_names_base
from .org.rules_org import find_orgs
from .loc.rules_loc import find_locations


PER_LIST = []


def _print_ents_from_doc(doc):
    """
    Print entities from spacy doc
    """
    ents = dict()
    for ent in doc.ents:
        if ent.label_ not in ents:
            ents[ent.label_] = [ent.text, ]
        else:
            ents[ent.label_].append(ent.text)
    for k, v in ents.items():
        print(f"\033[094m{k}:\033[090m {v}\033[0m")


def _print_ents(ents, txt, label='ENT'):
    """
    Print entities from list of entities
    """
    ent_str = f"\033[094m{label}\033[090m: | "
    for s, e, ent in ents:
        ent_str += f"{txt[s:e]} | "
    print(f"{ent_str}\033[0m")


def _print_ents_to_file(ents, txt, label='ENT', file_path='/code/logs/ents.txt'):
    """
    Print entities from list of entities
    """
    ent_str = f"[{label}]: | "
    for s, e, ent in ents:
        ent_str += f"{txt[s:e]} | "
    with open(file_path, 'a') as f:
        f.write(f"{ent_str}\033[0m\n")


def print_entities(ents_dict):
    for k, v in ents_dict.items():
        _print_ents(v, k)


def _add_entities(entities, doc, alignment_mode="expand"):
    """
    Add entities to spacy doc
    :param entities: sorted list of entities in format (start, end, label)
    :param doc: spacy doc to add entities to
    :param alignment_mode: mode to align entities with tokens: strict, expand, contract
    :return: spacy doc with added entities
    """
    existing_entities = list(doc.ents)
    new_entities = []

    # Function to check if a span overlaps with any in a given list
    def is_overlapping(span, spans):
        return any(span.start < e.end and span.end > e.start for e in spans)

    for start, end, label in sorted(entities, key=lambda x: x[0]):
        span = doc.char_span(start, end, label=label, alignment_mode=alignment_mode)
        if span is None:
            logger.info(f"Skipping misaligned entity: {label} - {doc.text[start:end]}")
            continue

        if is_overlapping(span, existing_entities) or is_overlapping(span, new_entities):
            logger.info(f"Skipping overlapping entity: {label} - {doc.text[start:end]}")
            continue

        new_entities.append(span)

    # Combine existing and new entities, remove duplicates and sort
    try:
        doc.ents = sorted(set(existing_entities + new_entities), key=lambda x: x.start_char)
        logger.info(f"\033[090mAdded entities: {doc.ents}\033[0m")
    except Exception as e:
        logger.error(f"\033[091mError adding entities: {e}\033[0m: {doc.ents} | {new_entities}")
    return doc


def add_custom_entities_to_doc(doc,
                               base_doc=None,
                               verbose: bool = False,
                               checklist: Union[None, List[str], str] = None,
                               filters: Union[None, List[str]] = None,
                               fuzzy_match: bool = False,
                               per_list_label: bool = False, ):
    """
    Add custom entities to spacy doc

    Args:
        doc (spacy.tokens.doc.Doc): spacy doc to add entities to
        base_doc (spacy.tokens.doc.Doc): spacy doc to use entities from
        verbose (bool): whether to print entities found
        checklist (list): list of names to add as entities
        filters (list): list of filters to apply to entities
        fuzzy_match (bool): whether to use fuzzy matching
        per_list_label (bool): whether to use PER_LIST for entities from checklist

    Returns:
        spacy.tokens.doc.Doc: spacy doc with added entities
    """
    text = doc.text

    _print_ents_from_doc(doc) if verbose else None
    print("-" * 100) if verbose else None

    sensitive_entities = find_sensitive(text)
    contacts_entities = find_contacts(text)
    date_entities = find_dates(text)
    org_entities = find_orgs(text)
    loc_entities = find_locations(text)

    name_entities = find_names(text, filter_list=filters)
    _print_ents_to_file(name_entities, text, 'NAME', file_path='/code/logs/ents.txt')

    if verbose:
        _print_ents(date_entities, text, 'DATE')
        _print_ents(contacts_entities, text, 'CONTACTS')
        _print_ents(name_entities, text, 'NAME')
        _print_ents(org_entities, text, 'ORG')
        _print_ents(loc_entities, text, 'LOC')
        _print_ents(sensitive_entities, text, 'SENSITIVE')

    # checklist = prepare_names_list()
    # name_entities += find_names_from_list(text, checklist, label=list_label)

    _print_ents(name_entities, text, 'NAME') if verbose else None
    print("-" * 100) if verbose else None

    entities = [(ent.start_char, ent.end_char, ent.label_) for ent in doc.ents]
    entities += contacts_entities + date_entities + name_entities + org_entities + loc_entities + sensitive_entities
    doc = _add_entities(entities, doc)

    return doc

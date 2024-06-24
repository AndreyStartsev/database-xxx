import re
from typing import List, Union
from .patterns.general import PATTERN_GENERAL, PATTERN_GENERAL_LATIN
from .patterns.filters import FILTER_LIST
from .filter_ents import filter_ents_by_pattern, filter_ents_by_names_list


def remove_overlapping_ents(
        ents,
        proximity_threshold=1
):
    # Sort entities by start position and then by end position in reverse
    ents.sort(key=lambda x: (x[0], -x[1]))

    non_overlapping_ents = []
    current_start, current_end = None, None

    for ent in ents:
        start, end, label = ent

        if current_end is None:
            current_start, current_end = start, end
        elif start - current_end <= proximity_threshold:
            # Entities are close enough to be considered part of the same entity
            current_end = max(current_end, end)
        else:
            non_overlapping_ents.append((current_start, current_end, label))
            current_start, current_end = start, end

    # Add the last entity
    if current_end is not None:
        non_overlapping_ents.append((current_start, current_end, label))

    return non_overlapping_ents


def find_names(
        text,
        specific: bool = True,
        general: bool = True,
        filter_list=Union[None, List[str]]
):
    if filter_list is None:
        filter_list = FILTER_LIST

    name_entities = []

    if general:
        # General Patterns
        name_entities += [(m.start(0), m.end(0), 'PER') for m in re.finditer(PATTERN_GENERAL, text)]
        name_entities += [(m.start(0), m.end(0), 'PER') for m in re.finditer(PATTERN_GENERAL_LATIN, text)]

    # Select words that start with a capital letter
    pattern = r'\(\s*[А-ЯЁ][а-яё]+\s*\)|'  # Format like '( Иванов )'
    pattern += r'\(\s*[A-Z][a-z]+\s*\)'  # Format like '( Ivanov )'
    name_entities += [(m.start(0), m.end(0), 'PER') for m in re.finditer(pattern, text)]

    # Combine and sort the results
    # name_entities = filter_ents_by_pattern(text, name_entities, '[A-Za-z]')
    name_entities = filter_ents_by_names_list(text, name_entities, filter_list)

    name_entities = remove_overlapping_ents(name_entities)

    return name_entities

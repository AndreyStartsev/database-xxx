import re


def filter_ents_by_pattern(text, name_entities, patterns_to_filter):
    filtered_entities = []
    for start, end, label in name_entities:
        entity_text = text[start:end]
        latin_in_word = bool(re.search('[A-Za-z]', entity_text))
        cyrillic_in_word = bool(re.search('[А-Яа-яЁё]', entity_text))
        if not latin_in_word and latin_in_word:
            filtered_entities.append((start, end, label))

    return filtered_entities


def filter_ents_by_names_list(text, name_entities, list_of_name_roots):
    filtered_entities = []
    for start, end, label in name_entities:
        overlaps = False
        for root in list_of_name_roots:
            root_regex = re.escape(root)
            if re.search(r'\b' + root_regex + r'[а-яА-ЯёЁ]*', text[start:end]):
                print(f"\033[091m{label}\033[090m: | {text[start:end]} | is filtered by {root_regex}\033[0m")
                overlaps = True
                break
        if not overlaps:
            filtered_entities.append((start, end, label))
    return filtered_entities

import re
from ..match_dict import find_names_from_list

LOC_LIST = [
    "Москва",
    "Воронеж",
    "Грозный",
    "Санкт-Петербург",
    "С. Петербург",
    "СПб",
    "Новосибирск",
    "Новосиб",
    "Краснодар",
    "Казань",
    "Екатеринбург",
    "Самара",
    "Челябинск",
    "Омск",
    "Ростов-на-Дону",
    "Ростов",
    "Ростов на Дону",
    "Уфа",
    "Красноярск",
    "Пермь",
    "Волгоград",
    "Чебоксары",
    "Ростовская",
    "Томская",
    "Нижегородская",
    "Воронежская",
    "Саратовская",
    "Омская",
    "Красноярская",
    "Иркутская",
    "Новосибирская",
]


def find_locations(text):
    loc_entities = find_names_from_list(text, LOC_LIST, label='LOC')
    loc_entities += find_regions(text)
    return loc_entities


def find_regions(text):
    pattern = (
        r'\b(\w+ого|\w+ому|\w+ым|\w+ом|\w+ая|\w+ой|\w+ую)\s+кра[еёюи]\b|'
        r'\b(\w+ой|\w+ая|\w+ую|\w+ой)\s+област[иью]\b'
    )
    ents = [(m.start(0), m.end(0), 'LOC') for m in re.finditer(pattern, text)]
    return ents

# TODO: refactor this code to use the same approach as in rules_per.py, rules_org.py

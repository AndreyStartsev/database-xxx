import re


def find_sensitive(text):
    ents = find_ids(text, label='SENSITIVE')
    ents += find_sequences_with_digits(text, label='SENSITIVE')
    return ents


def find_ids(text: str, label: str = 'ID'):
    pattern = r'\b\d{4}[-\s/]?\d{6}\b'
    ents = [(m.start(), m.end(), label) for m in re.finditer(pattern, text)]
    return ents


def find_sequences_with_digits(text: str, label: str = 'SEQUENCE_WITH_DIGITS'):
    pattern = r'\b(?:[a-zA-Z]*\d{4,}[a-zA-Z]*)+\b'
    ents = [(m.start(0), m.end(0), label) for m in re.finditer(pattern, text)]
    return ents
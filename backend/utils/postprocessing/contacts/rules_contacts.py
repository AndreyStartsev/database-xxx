import re


def find_contacts(text):
    ents = find_phone_with_extension(text, label='CONTACTS')  # first bcs more specific
    ents += find_contacts_with_pattern(text, label='CONTACTS')
    return ents


def find_contacts_with_pattern(text, label='CONTACTS'):
    pattern = (
        # email
        r'[A-Za-z0-9._%+-]*@[A-Za-z0-9.-]*\.[A-Z|a-z]{2,7}|'
        r'[А-Яа-яЁё]*@\w+|'
        # phone
        r'8-\d{3}-\d{3}-\d{2}-\d{2}(\.|)|'
        r'(?<=\D)(\+7|8)?(\()?(\d{3,4})(\))?[\s-]?(\d{2,3})[\s-]?(\d{2})[\s-]?(\d{2})|'
        r'[A-Za-zА-Яа-яЁё0-9!#$%&\'*+/=?^_`{|}~.-]+@[A-Za-z0-9!#$%&\'*+/=?^_`{|}~.-]+|'
        # foreign phone and email
        r'\b(\+?\d{1,4}[\s-]?)?(\()?(\d{1,4})(\))?[\s-]?(\d{2,4})[\s-]?(\d{2,4})[\s-]?(\d{2,4})\b|'
        r'[A-Za-zА-Яа-яЁё0-9!#$%&\'*+/=?^_`{|}~.-]+@[A-Za-z0-9!#$%&\'*+/=?^_`{|}~.-]+'  # email
    )
    ents = [(m.start(0), m.end(0), label) for m in re.finditer(pattern, text)]
    return ents


def find_phone_with_extension(text, label='PHONE_WITH_EXTENSION'):
    pattern = (
        r'\b(\+?\d{1,4}[\s-]?)?(\()?(\d{1,4})(\))?[\s-]?(\d{2,4})[\s-]?(\d{2,4})[\s-]?(\d{2,4})'
        r'(\s*,\s*(доб\.|#?ext\.|внутренний номер)\s*(\d{1,5}))?\b'
    )
    ents = [(m.start(0), m.end(0), label) for m in re.finditer(pattern, text)]
    return ents

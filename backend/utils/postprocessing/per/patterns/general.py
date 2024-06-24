PATTERN_GENERAL = (
    r'\b(?:'
    r'\(?[А-ЯЁ][а-яё]{2,}\s[А-ЯЁ]\.?[А-ЯЁ]?\.?\)?|'  # Formats like 'И. Иванов', 'П. Петров'
    r'\b[А-ЯЁ][а-яё]+\s[А-ЯЁ]{2,}\b|'  # Format like 'Иван ИВАНОВ'
    
    # Format like 'ИВАНОВА Ивана Ивановича'
    r'\b[А-ЯЁ]{2,}\s[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+\b|'
    
    r'\b[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+\b|'  # Format like 'Иван Иванов'
    r'\b[А-ЯЁ]\.[А-ЯЁ]\.\s?[А-ЯЁ][а-яё]+\b|'  # Format like 'И.П. Иванов'
    r'\b[А-ЯЁ]\.\s?[А-ЯЁ][а-яё]+\b|'  # Format like 'И. Иванов'
    r'\([А-ЯЁ][а-яё]{2,}\s[А-ЯЁ][а-яё]{2,}\)|'  # Formats in parentheses like '(Иван Иванов)'
    r'[А-ЯЁ][а-яё]+\s[А-ЯЁ][а-яё]+|'  # Full names like 'Иван Иванов'
    r'\([А-ЯЁ][а-яё]+\s[А-ЯЁ]{2}\)|'  # Format like '(Иванов ИИ)'
    r'\([А-ЯЁ][а-яё]+\s[А-ЯЁ]\)|'  # Format like '(Иванов И)'
    r'\([А-ЯЁ][а-яё]+\s[А-ЯЁ]\.[А-ЯЁ]\.\)'  # Format like '(Иванов И.И.)'
    r')\b'
)

PATTERN_GENERAL_LATIN = (
    r'\b(?:'
    r'[A-Z][A-Z]+\s[A-Z][a-z]+|'  # Format like 'XXXXX Xxxxxx'
    r'[A-Z][a-z]+\s[A-Z][A-Z]+-[A-Z][A-Z]+|'  # 'Xxxxx XXXXX-XXXXX'
    r'[A-Z][A-Z]+-[A-Z][A-Z]+\s[A-Z][a-z]+|'  # 'XXXXX-XXXXX Xxxxx'
    r'[A-Z][a-z]+-[A-Z][a-z]+\s[A-Z][a-z]+|'  # 'Xxxx-Xxxx Xxxx'
    r'[A-Z][a-z]+\s[A-Z][A-Z]+|'  # 'Xxxxx XXXXX'
    r'\b[A-Z][a-z]+\s[A-Z][a-z]+\b|'  # 'Xxxxx Xxxxx'
    r'[A-Z][a-z]+\s[A-Z]\.?(?:\s[A-Z][a-z]+)?|'  # 'Xxxxx X.' or 'Xxxxx X. Xxxxx'
    r'[A-Z]\.\s?[A-Z][a-z]+|'  # 'X. Xxxxx'
    r'[A-Z]\.[A-Z]\.\s?[A-Z][a-z]+|'  # 'X.X. Xxxxx'
    r'[A-Z][A-Z]+\.[A-Z]|'  # 'XXXXX.X'
    r'[A-Z]\.[A-Z][A-Z]+|'  # 'X.XXXXX'
    r'\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)?\s[A-Z]+\b'  # 'Xxxx Xxxx XXXXX'
    r')'
)

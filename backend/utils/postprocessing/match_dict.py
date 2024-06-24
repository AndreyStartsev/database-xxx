import re


def find_names_from_list(text,
                         match_list,
                         label='SENSITIVE',
                         case_sensitive=False,
                         space_before=True,
                         space_after=True):
    named_entities = []

    # Prepare the match list
    match_list = set(match_list)
    if not case_sensitive:
        modified_match_list = {name.lower() for name in match_list}
        modified_match_list.update(name.upper() for name in match_list)
        modified_match_list.update(name.capitalize() for name in match_list)
        modified_match_list.update(set(match_list))
        match_list = set(modified_match_list)

    # Prepare regex components
    prefix = r'\b' if space_before else r'\B'
    suffix = r'\b' if space_after else r'\B'

    # Combine regex patterns to minimize regex compilations
    combined_regex_pattern = '|'.join([re.escape(org_name) for org_name in match_list])
    full_regex_pattern = prefix + '(' + combined_regex_pattern + ')' + suffix

    # Perform regex search
    for match in re.finditer(full_regex_pattern, text):
        start, end = match.span()
        named_entities.append((start, end, label))

    return named_entities


def enrich_list_from_text_file(raw_list, file_path, min_word_lenght=3):
    with open(file_path, 'r') as f:
        # read and filter (only > 3 chars) the list
        more_names = [name for name in f.read().splitlines() if len(name) > min_word_lenght]
        raw_list += more_names
    return list(set(raw_list))


if __name__ == '__main__':
    # Example usage
    def _print_ents(ents, txt, label='ENT'):
        ent_str = f"\033[094m{label}\033[090m: | "
        for s, e, ent in ents:
            ent_str += f"{txt[s:e]} | "
        print(f"{ent_str}\033[0m")


    name_list = ['Бор',
                 'Боров ЕИ',
                 'Боярского КЮ',
                 'Волобуев А.В.', ]
    text = "Бо предписал Боровкову и боярскому подойти к Волобуеву от боярского"

    matched_names = find_names_from_list(text, name_list)
    _print_ents(matched_names, text, 'PER')

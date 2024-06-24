import re

from ..match_dict import find_names_from_list
from ..load_lists import prepare_names_list
from ..per.patterns.default_list import ORG_FILES_PATHS


def find_orgs(text, org_list=None):
    org_list = org_list or []
    # Prepare org names list to check against
    org_list = prepare_names_list(org_list, ORG_FILES_PATHS)
    # Add entities from list if found in text
    orgs = find_orgs_with_pattern(text)
    orgs += find_names_from_list(text, org_list, label='ORG')
    return orgs


def find_orgs_with_pattern(text):
    return []


if __name__ == "__main__":
    # Test find_orgs
    text = """
    Обследована в НИИТО им. Гельмгольца
    направлен в клинику им. Семашко
    медси, гемотест или емс
    """

    orgs = find_orgs(text)
    print(orgs)
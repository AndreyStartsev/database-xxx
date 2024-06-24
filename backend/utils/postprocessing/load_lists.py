import os
from loguru import logger

from .match_dict import enrich_list_from_text_file
from .per.patterns.default_list import PER_FILES_PATHS


def prepare_names_list(names_list=None, pd_files=PER_FILES_PATHS):
    names_list = names_list or []
    try:
        for file in pd_files:
            if os.path.exists(file):
                names_list = enrich_list_from_text_file(names_list, file)
            else:
                logger.error(f"\033[091mFile not found: {file}\033[0m")
        names_list = list(set(names_list))
        # logger.info(f"\033[092mTotal number of names: {len(names_list)}\033[0m")
        return names_list
    except Exception as e:
        logger.error(f"\033[091mError preparing names list: {e}\033[0m")
        return names_list

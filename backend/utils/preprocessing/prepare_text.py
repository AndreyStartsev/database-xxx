from .html_clean import extract_text_from_html


def preprocess(text, remove_html_tags=False):

    if remove_html_tags:
        text = extract_text_from_html(text)

    return text

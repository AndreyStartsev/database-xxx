def hide_ents(text, entities, placeholder=None, ents_to_hide=None, pd_generator=None):
    # ent.start_char, ent.end_char, ent.label_
    for ent in reversed(entities):
        if ents_to_hide is not None and ent.label_ not in ents_to_hide:
            continue
        replacement = placeholder if placeholder is not None else f"[{ent.label_}]"
        if pd_generator is not None:
            replacement = pd_generator.generate(ent.text, ent.label_)
        text = text[:ent.start_char] + replacement + text[ent.end_char:]
    return text


def hide_ents_in_doc(doc, placeholder=None, ents_to_hide=None, pd_generator=None):
    return hide_ents(doc.text, doc.ents, placeholder=placeholder, ents_to_hide=ents_to_hide, pd_generator=pd_generator)
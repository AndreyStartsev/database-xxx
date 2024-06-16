from bs4 import BeautifulSoup


def extract_text_from_html(html_content):
    """
    Extracts and returns the text from HTML content.

    :param html_content: A string containing HTML content.
    :return: A string with the extracted text, free of HTML tags.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    for script in soup(["script", "style"]):  # Remove script and style elements
        script.decompose()

    text = soup.get_text(separator=' ')
    lines = [line.strip() for line in text.splitlines()]
    chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text


if __name__ == '__main__':
    html_content = "<html><head><title>Test</title></head><body><p>Hello world.</p></body></html>"
    clean_text = extract_text_from_html(html_content)
    print(clean_text)

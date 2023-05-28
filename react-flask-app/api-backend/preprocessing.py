import re
def remove_punctuation(text):
    text = re.sub(r'[_]', ' ', text)
    return re.sub(r'[^\w\s]', ' ', text)


def remove_numbers(text):
    return re.sub(" \d+", " ", text)


def remove_newline(text):
    return re.sub(r'\s+|\\n', ' ', text)


def remove_html_tags(text):
    return re.sub(r'<[^<]+?>', ' ', text)


def remove_emails(text):
    return re.sub(r'[A-Za-z0-9]+@[a-zA-z].[a-zA-Z]+', ' ', text)


def remove_single_characters(text):
    return re.sub(r'\b\w\b', ' ', text)


def remove_mentions(text):
    return re.sub(r'@+[A-Za-z0-9_]+', ' ', text)


def remove_website_extra(text):
    text = re.sub(r'(http[s]*:[/][/])[a-zA-Z0-9./]+', ' ', text)
    return re.sub(r'(http|https):\/\/[a-zA-Z0-9]+([\-\.]{1}[a-zA-Z0-9]+)*\.[a-zA-Z]{2,5}(:[0-9]{1,5})?(\/.*)?', ' ', text)


def remove_website(text):
    return re.sub(r'www.(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)


def remove_multiple_spaces(text):
    return re.sub(r'\s+', ' ', text)


def remove_urls(text):
    text = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)
    return re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)

def remove_hashtags(text):
    return re.sub(r'#+[A-Za-zα-ωΑ-Ω0-9_άέύίόώήϊ]+', "", text)

def lower_text(text):
    return text.lower()


def preCleanEnglishText(df, column, new_column):
    #df = df.drop_duplicates(subset=[column])
    df[new_column] = df[column].apply(lambda x: remove_urls(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_emails(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_website(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_website_extra(str(x)))
    #df[new_column] = df[new_column].apply(lambda x: remove_punctuation(str(x)))
    df[new_column] = df[new_column].apply(lambda x: lower_text(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_newline(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_numbers(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_single_characters(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_multiple_spaces(str(x)))
    df[new_column] = df[new_column].apply(lambda x: remove_punctuation(str(x)))
    return df
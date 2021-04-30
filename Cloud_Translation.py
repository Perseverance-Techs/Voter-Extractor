from google.cloud import translate

def sample_translate_text(text, project_id):
    """Translating Text."""

    client = translate.TranslationServiceClient()

    parent = client.location_path(project_id, "global")

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    response = client.translate_text(
        parent=parent,
        contents=[text],
        mime_type="text/plain",  # mime types: text/plain, text/html
        target_language_code="en-US",
    )
    # Display the translation for each input text provided
    for translation in response.translations:
        print(u"Translated text: {}".format(translation.translated_text))
        
sample_translate_text("പേര്: മുഹമ്മദ് അഷറഫ് കെ എ അച്ഛന്റെ പേര്: അബ്ദുൾ റഹിമാൻ വീട്ടു നമ്പർ: 4/67 തുമിനാട് ഹൗസ് വയസ്സ്. 39 ലിംഗം: പുരുഷൻ","ocr-text-extraction-260613")
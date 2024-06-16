# endpoint markdown descriptions:

ANONIMIZATION_DESCRIPTION = """
# ✅ Anonymizes a batch of text inputs.

**Request Body**
batch_input: a list of texts to anonymize
config: configuration for the NER model
<br>
    - `model`: the name of the model to use for NER
<br>
    - `aggressive`: whether to use aggressive NER search (max recall)
<br>
    - `placeholder`: the placeholder to use for anonymization (if None - then entity name will be used)

**Response**
predictions: a list of predictions for each text in the batch 
    list of dicts: `{text: str, entities: list}`, 
    where entities is a list of dicts: `{start: int, end: int, label: str}`
"""

TABLE_ANALYSIS_DESCRIPTION = """
# ✅ Analyzes a table and returns the column names and estimation if columns contain personal or sensitive data.

**Request Body**
table_name: the name of the table to analyze
analysis_type: the type of analysis to perform (e.g. 'personal_data', 'sensitive_data', 'all')

> **Note:** The `analysis_type` parameter is optional. If not provided, the API will return all available analysis types.
> **Note:** 'personal_data' includes columns that contain personal data such as names, addresses, emails, etc.
> **Note:** 'sensitive_data' includes columns that contain sensitive data such as financial information, health records, etc.

**Response**
table_info: a dictionary containing the column names and estimation if columns contain personal or sensitive data as a list of dicts:
    `{column_name: str, personal_data: bool, sensitive_data: bool, entities: list}`
"""

TABLE_PROCESSING_DESCRIPTION = """
# ✅ Processes a table and creates a new table with anonymized data.

**Request Body**
table_name: the name of the table to process
config: configuration for the NER model
<br>
    - `model`: the name of the model to use for NER
<br>
    - `placeholder_strategy`: the strategy to use for anonymization (e.g. 'random', 'entity_name', 'masking')
<br>
    - `entities_to_hide`: a list of entities to hide (e.g. 'PER', 'ORG', 'LOC')
<br>
    - `prefix`: the prefix to use for the new table name
<br>
    - `entries_limit`: the limit of entries to process (default: 0 - all entries)
    
**Response**
table_name: the name of the new table with anonymized data
"""


HEALTHCHECK_DESCRIPTION = """
# ✅ Healthcheck endpoint, returns the current status of the API.
"""

MODEL_STATUS_DESCRIPTION = """
# ✅ Model status endpoint, returns the current status of the model, including its name and loading status.
"""
from google.cloud import bigquery
from setup import PROJECT_ID

client = bigquery.Client(project=PROJECT_ID)

query = """
    SELECT *
    FROM ``
    LIMIT 10
"""

df = client.query(query).to_dataframe()
print(df)

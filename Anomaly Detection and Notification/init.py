import datetime
import logging
import os
import azure.functions as func
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from dotenv import load_dotenv 
import pandas as pd
import numpy as np
import smtplib
import requests


def main(mytimer: func.TimerRequest): 
    load_dotenv()

    print(os.environ.get("KUSTO_USERNAME"))

    cluster = [REDACTED] 
    database = [REDACTED]  
    tenant_id = [REDACTED]  
    username = os.environ.get('KUSTO_USERNAME')
    password = os.environ.get('KUSTO_PASSWORD')

    KCSB = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, username, password, tenant_id) 
    client = KustoClient(KCSB) 

    query = [REDACTED] 
  
    response = client.execute(database, query)
    data = response.primary_results[0]

    df = dataframe_from_result_table(data)
    table = pd.DataFrame(df)      

    print(table.to_string(index=False)) 

    table_html = table.to_html(index=False)

    print(table_html)

    webhook_url = [REDACTED]

    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "Table from Azure Function",
        "title": "Azure Function Table",
        "text": "Please find the table below:",
        "sections": [
            {
                "activityTitle": "",
                "activitySubtitle": "",
                "activityImage": "",
                "facts": [],
                "markdown": True,
                "text": table_html
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        print("Message sent to Teams successfully!")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message to Teams: {str(e)}")

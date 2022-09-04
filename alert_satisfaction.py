import logging
import datetime
import os
import pandas as pd
import numpy as np
from scipy import stats
from gspread_dataframe import set_with_dataframe
import warnings

import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
from airflow.providers.google.common.hooks.base_google import BaseHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

from airflow.models import Variable
import math


warnings.filterwarnings("ignore")
client = bigquery.Client(project="pretto-apis")
add_GS = True

GS_URL = "https://docs.google.com/spreadsheets/d/1xA3mggHyfBb0TWGAj0a7QkuOzcwNBdvhOVfpoS7STmI/edit#gid=143788854"
query_latest_alerts = """

  SELECT
  date_pred,
  sfid,
  sf,
  ROUND(100*proba_satisfaction) proba_satisfaction,
FROM (
  SELECT
    scoring_satisfaction.*,
    ROW_NUMBER() OVER(PARTITION BY scoring_satisfaction.sfid ORDER BY date_pred DESC) AS RN,
  FROM
    `pretto-apis.salesforce.scoring_satisfaction` scoring_satisfaction
  LEFT JOIN
    salesforce.opportunity opp
  ON
    scoring_satisfaction.sfid=opp.sfid
  WHERE
    DATE_DIFF(CURRENT_DATE(),DATE(date_pred),day)<=1
    AND opp.lost_reason__c IS NULL
    AND opp.date_perdue__c IS NULL
    AND opp.date_fonds__c IS NULL
    AND opp.stagename not in ("Offre éditée", "Offre signée", "Accordé", "Perdue", "Fonds débloqués")
    AND opp.is_pro__c <> TRUE
    AND opp.lead_owner__c NOT IN ("LEAD_OWNER_JUNIOR",
      "LEAD_OWNER_SENIOR")
    )
WHERE
  RN=1
ORDER BY
  proba_satisfaction ASC
    """


def get_client(conn_id="google_sheets_default"):
    """
    Extract the credentials object without loading the json service account
    to invoke client.
    """
    gcp_hook = GoogleCloudBaseHook(gcp_conn_id=conn_id)
    creds = gcp_hook._get_credentials()
    client = gspread.authorize(creds)

    return client


def get_data(query):
    client = bigquery.Client("pretto-apis")
    df = client.query(query).to_dataframe()
    return df


def add_alerte(row, current_text):

    current_text += "https://pretto.lightning.force.com/lightning/r/Opportunity/" + str(row["sfid"]) + "/view" + " \n "
    current_text += (
        "On estime une probabilité d'insatifaction  de : " + str(round(100 * row["proba_satisfaction"])) + "% \n "
    )
    return current_text


def check_satisfaction():
    latest_alerts = get_data(query_latest_alerts)

    if add_GS == True:

        client = client = get_client()
        data_GS = client.open_by_url(GS_URL)
        data_historic_sheet = data_GS.worksheet("historique")
        data_historic = pd.DataFrame(data_historic_sheet.get_all_records())

        data_previous_day_sheet = data_GS.worksheet("new_alert")
        data_previous_day = pd.DataFrame(data_previous_day_sheet.get_all_records())
        data_previous_day = data_previous_day.loc[data_previous_day["traitement"].notnull()]
        data_previous_day = data_previous_day.loc[data_previous_day["traitement"] != ""]

        data_historic = pd.concat([data_historic, data_previous_day], axis=0)

        latest_alerts["date_pred"] = pd.to_datetime(latest_alerts["date_pred"])
        data_historic["date_pred"] = pd.to_datetime(data_historic["date_pred"])
        data_historic["how_old_alert"] = (latest_alerts["date_pred"].max() - data_historic["date_pred"]).dt.days
        data_historic_recent = data_historic.loc[data_historic["how_old_alert"] < 8]
        latest_alerts = latest_alerts.loc[~latest_alerts["sfid"].isin(data_historic_recent["sfid"].unique())]
        latest_alerts["traitement"] = ""
        data_historic.drop("how_old_alert", axis=1, inplace=True)
        data_historic.sort_values("date_pred", ascending=False, inplace=True)
        set_with_dataframe(data_historic_sheet, data_historic)
        data_new_alert_sheet = data_GS.worksheet("new_alert_extract_python")
        set_with_dataframe(data_new_alert_sheet, latest_alerts)

        cell_list = data_previous_day_sheet.range("E2:E1000")
        # on efface les traitements qui ont déjà été renseignés sur le jour précédent. Cela évitera de penser qu'on a bien traité un dossier alors qu'en fait non pas du tout
        for cell in cell_list:
            cell.value = ""
        data_previous_day_sheet.update_cells(cell_list)


# if __name__ == "__main__":
#    check_satisfaction()

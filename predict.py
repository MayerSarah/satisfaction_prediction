# Data librairies
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import time
import sklearn
import warnings
import datetime as dt
import joblib


from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc

from datetime import timedelta, date, timezone

from six import StringIO

# Google Cloud / Airflow libraries
from google.cloud import bigquery, storage
from tempfile import TemporaryFile


# Display
warnings.filterwarnings("ignore")
client = bigquery.Client(project="pretto-apis")
pd.options.display.float_format = "{:.3f}".format


from score_satisfaction_V2.preprocessing import create_all_features_ringover, create_features_salesforce

from score_satisfaction_V2.load_data import load_ringover_df_test, load_salesforce_df_test, target_definition_test

from score_satisfaction_V2.train import merge_all_features

bucket_name = "pretto-dataflow"

model_name = "trustpilot/models/model_v0.joblib"


def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a blob from the bucket.
    """

    client = storage.Client(project="pretto-apis")

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


def load_model_fom_GCS(bucket_name, model_name):
    if ".joblib" in model_name:

        temporary_file_path = "temp_model.joblib"
        download_file_from_gcs(bucket_name, model_name, temporary_file_path)
        model = joblib.load(temporary_file_path)
    else:
        raise ValueError
    return model


def upload_file_to_gcs(bucket, source_file, destination_file):

    client = storage.Client(project="pretto-apis")
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(destination_file)

    # loop until connection is done (useful for large file):
    flag = None
    while flag is not True:
        try:
            # connect
            blob.upload_from_filename(source_file)
            flag = True
        except ConnectionError:
            print("\n Retry to upload file ...\n")
            pass


def upload_csv_to_bq(file_path, dataset, table_id, schema, bucket=None, write_disposition="WRITE_APPEND"):
    """
    Upload a csv file to BQ table.
    """

    client = bigquery.Client("pretto-apis")
    table_id_full = f"pretto-apis.{dataset}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=write_disposition,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id_full, job_config=job_config)
        job.result()
        print(f"Full dataset from '{file_path}' uploaded to BQ at '{table_id_full}'")


def get_predict_probability_recent():
    NPS_Avis = target_definition_test()
    model = load_model_fom_GCS(bucket_name, model_name)
    features_generales = merge_all_features(NPS_Avis)

    cols_for_model = [
        "accountid",
        "sfid",
        "nombre appels_global",
        "prct_in_global",
        "nombre appels_buzdev",
        "prct_in_buzdev",
        "prct_out_buzdev",
        "attente moyenne entre deux appels",
        "Durée moyenne des appels",
        "Nb_appels_in_sans_reponse",
        "Nb appel out sans reponse",
        "Nb_Jours_Plusieurs_appels_global",
        "Nb_Jours_Plusieurs_appels_7_derniers_jours",
        "Nb_Jours_Plusieurs_appels_14_derniers_jours",
        "Nb_Appel_Sup_10_min",
        "Is_Dernier_Appel_Out",
        "Nb experts différents",
        "Nb interlocuteurs différents",
        "Pas de rappel",
        "nombre messages repondeur",
        "Duree_AppelsEntrants",
        "Pourcentage appels manqués in",
        "Pourcentage appels manqués out",
        "Nb de fois sans appel depuis 14j par Pretto",
        "Experience_EC_jours",
        "Attente_depuis_dernier_appel",
        "Attente depuis premier appel",
        "prt_appel_manque_sur_appels",
        "ppt_appelmanque_sans_rappel",
        "duree_in_sur_duree_total",
        "delta_date_comm_date_mandat",
        "date_montage__c-date_r1__c",
        "dt_commentaire_suspensive",
        "dt_creation_envoi",
        "no_show",
        "on_time",
        "plus_15_min_retard",
        "nb_appel_sup_3_min",
        "expert_en_vacs",
    ]
    features_generales_model = features_generales[cols_for_model]
    features_generales_model["Valeurs_manquantes"] = ""

    features_generales_model.drop_duplicates(subset=["accountid", "sfid"], inplace=True)
    features_generales_model.reset_index(drop=True, inplace=True)

    for col in [
        "nombre appels_global",
        "prct_in_global",
        "nombre appels_buzdev",
        "prct_in_buzdev",
        "prct_out_buzdev",
        "attente moyenne entre deux appels",
        "Durée moyenne des appels",
        "Nb_appels_in_sans_reponse",
        "Nb appel out sans reponse",
        "Nb_Jours_Plusieurs_appels_global",
        "Nb_Jours_Plusieurs_appels_7_derniers_jours",
        "Nb_Jours_Plusieurs_appels_14_derniers_jours",
        "Nb_Appel_Sup_10_min",
        "Is_Dernier_Appel_Out",
        "Nb experts différents",
        "Nb interlocuteurs différents",
        "Pas de rappel",
        "nombre messages repondeur",
        "Duree_AppelsEntrants",
        "Pourcentage appels manqués in",
        "Pourcentage appels manqués out",
        "Nb de fois sans appel depuis 14j par Pretto",
        "Experience_EC_jours",
        "Attente_depuis_dernier_appel",
        "Attente depuis premier appel",
        "prt_appel_manque_sur_appels",
        "ppt_appelmanque_sans_rappel",
        "duree_in_sur_duree_total",
        "date_montage__c-date_r1__c",
        "dt_commentaire_suspensive",
        "dt_creation_envoi",
        "no_show",
        "on_time",
        "plus_15_min_retard",
        "nb_appel_sup_3_min",
        "expert_en_vacs",
    ]:
        index_to_strings = list(features_generales_model.loc[pd.isna(features_generales_model[col]), :].index)
        med = features_generales_model[col].median()
        features_generales_model[col].fillna(med, inplace=True)

        for i in index_to_strings:
            features_generales_model["Valeurs_manquantes"].loc[i] = (
                features_generales_model["Valeurs_manquantes"].loc[i] + " / " + str(col)
            )

    features_generales_model.dropna(inplace=True)
    features_generales_model.reset_index(drop=True, inplace=True)

    X_base_test = features_generales_model.copy()
    X_base_test.drop(["accountid", "sfid", "Valeurs_manquantes"], axis=1, inplace=True)

    X_base_test["proba_satisfaction"] = model.predict_proba(X_base_test)[:, 1]

    df_final = features_generales_model.copy()

    proba_final = pd.DataFrame(X_base_test["proba_satisfaction"])

    df_final = pd.concat([df_final, proba_final], axis=1)
    df_final.rename(columns={"delta_date_comm_date_mandat": "deltatime_date_mandat"}, inplace=True)

    df_final = df_final[
        [
            "accountid",
            "sfid",
            "proba_satisfaction",
            "deltatime_date_mandat",
            "prct_in_global",
            "Attente_depuis_dernier_appel",
            "Experience_EC_jours",
            "ppt_appelmanque_sans_rappel",
            "Nb_appels_in_sans_reponse",
            "Valeurs_manquantes",
        ]
    ]
    df_final["proba_satisfaction"] = df_final["proba_satisfaction"].round(3)
    df_final["sf"] = "https://pretto.lightning.force.com/lightning/r/Opportunity/" + df_final["sfid"] + "/view"
    df_final["date_pred"] = str(date.today())
    df_final.to_csv("temp.csv", index=False)
    df_final.drop_duplicates(inplace=True)
    source_file = "temp.csv"
    destination_file = f"trustpilot/predictions/predictions.csv"
    destination_file_bis = f"trustpilot/predictions/prediction" + "_" + str(date.today()) + ".csv"

    format_bq = [
        {"type": "STRING", "name": "accountid", "mode": "NULLABLE"},
        {"type": "STRING", "name": "sfid", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "proba_satisfaction", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "deltatime_date_mandat", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "prct_in_global", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "Attente_depuis_dernier_appel", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "Experience_EC_jours", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "ppt_appelmanque_sans_rappel", "mode": "NULLABLE"},
        {"type": "FLOAT", "name": "Nb_appels_in_sans_reponse", "mode": "NULLABLE"},
        {"type": "STRING", "name": "Valeurs_manquantes", "mode": "NULLABLE"},
        {"type": "STRING", "name": "sf", "mode": "NULLABLE"},
        {"type": "DATETIME", "name": "date_pred", "mode": "NULLABLE"},
    ]

    upload_file_to_gcs("pretto-dataflow", source_file, destination_file)
    upload_file_to_gcs("pretto-dataflow", source_file, destination_file_bis)
    download_file_from_gcs(bucket_name, "trustpilot/predictions/predictions.csv", "file_to_bq.csv")
    upload_csv_to_bq("file_to_bq.csv", "salesforce", "scoring_satisfaction", format_bq)

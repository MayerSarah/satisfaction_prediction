# Data librairies
import matplotlib.pyplot as plt
import pandas as pd
import time
import warnings
import datetime as dt
from collections import Counter
import os
import joblib

from datetime import timedelta, date, timezone
from six import StringIO
from sklearn.ensemble import RandomForestClassifier
from joblib import dump

# Google Cloud / Airflow libraries
from google.cloud import bigquery, storage
from tempfile import TemporaryFile
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc

# Display
warnings.filterwarnings("ignore")
client = bigquery.Client(project="pretto-apis")
pd.options.display.float_format = "{:.3f}".format


from score_satisfaction_V2.preprocessing import create_all_features_ringover, create_features_salesforce

from score_satisfaction_V2.load_data import load_ringover_df_train, load_salesforce_df_train, target_definition_train

CORRELATION_THRESHOLD = 0.85


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    storage_client = storage.Client("pretto-apis")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print("File {} uploaded to {}.".format(source_file_name, destination_blob_name))


def load_CSV_to_dataframe_fom_GCS(bucket_name, model_name):
    storage_client = storage.Client("pretto-apis")
    bucket_name = bucket_name
    model_bucket = model_name

    bucket = storage_client.get_bucket(bucket_name)
    # select bucket file
    blob = bucket.blob(model_bucket)
    with TemporaryFile() as temp_file:
        # download blob into temp file
        blob.download_to_file(temp_file)
        temp_file.seek(0)
        # load into joblib
        df = pd.read_csv(temp_file)

    return df


def compute_auc(y_test, y_proba_test):
    """
    Compute AUC metrics.
    """

    fpr, tpr, roc_thresholds = roc_curve(y_test, y_proba_test)
    roc_auc = auc(fpr, tpr)

    return roc_auc


# X dataset recomposition
def merge_all_features(Note):

    ringover_note = load_ringover_df_train(Note)
    features_Salesforce = load_salesforce_df_train(Note)

    # Features creation
    all_features_ringover = create_all_features_ringover(ringover_note)
    print("fr", all_features_ringover.shape)
    features_Salesforce = create_features_salesforce(features_Salesforce)
    print("fs", features_Salesforce.shape)
    # Jointure

    features_generales = pd.merge(
        all_features_ringover,
        features_Salesforce,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    return features_generales


# y definition
def get_label_Is_Good(data):
    data["IsGood"] = data["Note"] > 3

    # Suppression des notes égales à 4 ou à 3.5

    data = data.loc[(data["Note"] != 4) & (data["Note"] != 3.5)]

    return data


def get_y(data):
    y = data["IsGood"]
    return y


# MODEL implementation
def train_model():
    # Get NPS and Comments"
    NPS_Avis = target_definition_train(
        "pretto-dataflow",
        "trustpilot/formated_input/NPS_corrected.csv",
        "trustpilot/formated_input/Avis_Google_Facebook.csv",
        "trustpilot/formated_input/Avis_Trustpilot.csv",
    )

    features_generales_init = merge_all_features(NPS_Avis)
    features_generales_init = get_label_Is_Good(features_generales_init)

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
        "nb_appel_sup_3_min",
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
        "IsGood",
        "expert_en_vacs",
    ]
    features_generales = features_generales_init[cols_for_model]
    features_generales.dropna(inplace=True)
    features_generales.reset_index(inplace=True, drop=True)
    y = get_y(features_generales)

    X = features_generales.copy()
    X.drop(["IsGood"], axis=1, inplace=True)

    # Create correlation matrix
    # corr_matrix = X.corr().abs()
    # Select upper triangle of correlation matrix
    # upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))
    # to_drop = [column for column in upper.columns if any(upper[column] > CORRELATION_THRESHOLD)]
    # Drop features
    # X.drop(to_drop, axis=1, inplace=True)

    rf_best = RandomForestClassifier(
        bootstrap=False,
        ccp_alpha=0.0,
        class_weight="balanced",
        max_depth=15,
        max_leaf_nodes=14,
        max_samples=None,
        min_impurity_decrease=0.0,
        min_samples_leaf=80,
        min_samples_split=175,
        min_weight_fraction_leaf=0.0,
        n_estimators=500,
        n_jobs=None,
        oob_score=False,
        random_state=None,
        verbose=0,
        warm_start=False,
    )

    # Split dataset
    # mettre 80% des sfid dans le train
    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y)
    X_train.drop(["accountid", "sfid"], axis=1, inplace=True)
    X_test.drop(["accountid", "sfid"], axis=1, inplace=True)

    rf_best.fit(X_train, y_train)

    y_pred_test = rf_best.predict(X_test)
    y_pred_train = rf_best.predict(X_train)

    print(f"auc train : {compute_auc(y_train, y_pred_train)}")
    print(f"auc test : {compute_auc(y_test, y_pred_test)}")

    args = {}
    args["bucket"] = "pretto-dataflow"
    args["model_trained"] = "model_v0.joblib"
    args["destination_model_name"] = "trustpilot/models/" + args["model_trained"]
    dump(rf_best, args["model_trained"])
    upload_blob(args["bucket"], args["model_trained"], args["destination_model_name"])

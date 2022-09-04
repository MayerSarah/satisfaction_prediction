# Data librairies
import pandas as pd
import warnings
import datetime as dt

from google.cloud import bigquery
from datetime import timedelta, date, timezone

# Google Cloud / Airflow libraries
from google.cloud import bigquery, storage
from tempfile import TemporaryFile

# Display
warnings.filterwarnings("ignore")
client = bigquery.Client(project="pretto-apis")
pd.options.display.float_format = "{:.3f}".format

from score_satisfaction_V2.sql_queries import (
    get_data_mandate_signed_train,
    get_data_mandate_signed_test,
    get_data_ringover_train,
    get_data_ringover_test,
    get_data_sf_train,
    get_data_sf_test,
)


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


# Train model
# Target for the train model


def get_NPS(bucket_name, NPS_path):
    NPS = load_CSV_to_dataframe_fom_GCS(bucket_name, NPS_path)
    NPS.drop(["Etape du parcours"], axis=1, inplace=True)

    return NPS


def get_Google_and_Facebook(bucket_name, Google_Facebook_path):
    Avis_Google_Facebook = load_CSV_to_dataframe_fom_GCS(bucket_name, Google_Facebook_path)

    return Avis_Google_Facebook


def get_Trustpilot(bucket_name, Trustpilot_path):
    Avis_Trustpilot = load_CSV_to_dataframe_fom_GCS(bucket_name, Trustpilot_path)

    return Avis_Trustpilot


def decup_Comments(NPS_Avis):
    Temp = [NPS_Avis]
    Days = [14]
    for day in Days:
        NPS_Avis_shifted = NPS_Avis.copy()
        NPS_Avis_shifted["Date_com"] = NPS_Avis_shifted["Date_com"] - dt.timedelta(day)
        Temp.append(NPS_Avis_shifted)
    NPS_Avis = pd.concat(Temp)

    return NPS_Avis


def target_definition_train(bucket_name, NPS_path, Trustpilot_path, Google_Facebook_path):
    df_mandat = get_data_mandate_signed_train()
    df_mandat.rename(
        columns={
            "received_at": "date_signature_mandat",
            "opportunity_sfid": "Id Salesforce",
        },
        inplace=True,
    )
    df_mandat.drop_duplicates(subset=None, keep="first", inplace=True)
    df_mandat["date_signature_mandat"] = pd.to_datetime(df_mandat["date_signature_mandat"], utc=None)
    df_mandat["date_signature_mandat"] = df_mandat["date_signature_mandat"].dt.round("1d")

    NPS = get_NPS(bucket_name, NPS_path)
    NPS = NPS.loc[NPS["Date_com"] > "2020-06-01"]
    NPS["Source"] = "NPS"

    Avis_Trustpilot = get_Trustpilot(bucket_name, Trustpilot_path)
    Avis_Trustpilot = Avis_Trustpilot.loc[Avis_Trustpilot["Date_com"] > "2020-06-01"]
    Avis_Trustpilot = Avis_Trustpilot[NPS.columns]

    Avis_Google_Facebook = get_Google_and_Facebook(bucket_name, Google_Facebook_path)
    Avis_Google_Facebook = Avis_Google_Facebook.loc[Avis_Google_Facebook["Date_com"] > "2020-06-01"]
    Avis_Google_Facebook = Avis_Google_Facebook[NPS.columns]

    Avis_Google_Facebook["Note"] = Avis_Google_Facebook["Note"].astype(float)
    Avis_Trustpilot["Note"] = Avis_Trustpilot["Note"].astype(float)
    NPS_Avis = pd.concat([NPS, Avis_Google_Facebook])
    NPS_Avis = pd.concat([NPS_Avis, Avis_Trustpilot])

    NPS_Avis["Date_com"] = pd.to_datetime(NPS_Avis["Date_com"], unit="ns", utc=None)
    NPS_Avis["Date_com"] = NPS_Avis["Date_com"].dt.round("1d")
    NPS_Avis.sort_values(by=["Id Salesforce", "Date_com"], inplace=True)
    NPS_Avis.reset_index(inplace=True, drop=True)

    NPS_Avis = NPS_Avis.merge(df_mandat, how="inner", on="Id Salesforce")
    NPS_Avis.sort_values(["Id Salesforce", "Date_com"], inplace=True)
    NPS_Avis.reset_index(inplace=True, drop=True)
    NPS_Avis["date_signature_mandat"] = NPS_Avis["date_signature_mandat"].dt.tz_localize(None)
    NPS_Avis["delta_date_comm_date_mandat"] = (NPS_Avis["date_signature_mandat"] - NPS_Avis["Date_com"]).dt.days
    NPS_Avis = NPS_Avis.loc[NPS_Avis["delta_date_comm_date_mandat"] < 0]
    NPS_Avis.sort_values(
        by=["Id Salesforce", "delta_date_comm_date_mandat"],
        inplace=True,
        ascending=False,
    )
    NPS_Avis.reset_index(inplace=True, drop=True)
    NPS_Avis.drop_duplicates(
        subset="Id Salesforce", keep="first", inplace=True
    )  # ne garder qu'une seule ligne pour une opp donnée (celle la plus proche de la date de signature de mandat)

    NPS_Avis = decup_Comments(NPS_Avis)

    return NPS_Avis


# Salesforce data train


def load_salesforce_df_train(Note):
    Salesforce = get_data_sf_train()
    features_Salesforce = pd.merge(
        Salesforce,
        Note,
        left_on=["sfid", "accountid"],
        right_on=["Id Salesforce", "accountid"],
        how="inner",
    )

    ListeEtat = [
        "createddate",
        "date_a_joindre__c",
        "date_r1__c",
        "date_rech_no_doc__c",
        "date_rech_docs__c",
        "date_montage__c",
        "send_date__c",
        "date_perdue__c",
        "date_offre_edit__c",
        "mandate_signed__c",
        "date_compromis",
        "date_condition_suspensive",
    ]

    for col in ListeEtat:
        features_Salesforce[col] = (
            features_Salesforce[col].apply(lambda x: pd.to_datetime(x, errors="coerce", utc=True)).dt.tz_localize(None)
        )

    for col in ListeEtat:
        features_Salesforce[col].loc[features_Salesforce[col] > features_Salesforce["Date_com"]] = None

    features_Salesforce = features_Salesforce.loc[features_Salesforce["Date_com"] > features_Salesforce["createddate"]]
    features_Salesforce.reset_index(drop=True, inplace=True)

    features_Salesforce["date_condition_suspensive"].loc[
        features_Salesforce["date_condition_suspensive"] < features_Salesforce["createddate"]
    ] = None

    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"] < features_Salesforce["date_montage__c"]) < 0
    ] = None

    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"] < features_Salesforce["send_date__c"]) < 0
    ] = None
    features_Salesforce["date_compromis"].loc[
        features_Salesforce["date_perdue__c"] < features_Salesforce["date_compromis"]
    ] = None
    features_Salesforce["date_compromis"].loc[
        ((features_Salesforce["date_condition_suspensive"] < features_Salesforce["date_compromis"]))
        & ((features_Salesforce["send_date__c"] < features_Salesforce["date_compromis"]))
    ] = None
    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"] - features_Salesforce["date_compromis"]).dt.days > 300
    ] = None
    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"].isnull()) & (features_Salesforce["date_compromis"].notnull())
    ] = features_Salesforce["date_compromis"] + timedelta(93)
    features_Salesforce["date_compromis"].loc[
        (features_Salesforce["date_compromis"].isnull()) & (features_Salesforce["date_condition_suspensive"].notnull())
    ] = features_Salesforce["date_condition_suspensive"] - timedelta(93)

    # On supprimme les dates perdues qui ont lieu avant des dates du process
    ListeEtat = [
        "date_a_joindre__c",
        "date_r1__c",
        "date_rech_no_doc__c",
        "date_rech_docs__c",
        "date_montage__c",
        "send_date__c",
    ]

    for col in ListeEtat:
        features_Salesforce["date_perdue__c"].loc[
            (features_Salesforce["date_perdue__c"] - features_Salesforce[col]).dt.days < -2
        ] = None

    return features_Salesforce


# Ringover for train model


def load_ringover_df_train(Note):
    call = get_data_ringover_train()
    ## On tronque les appels à e heure d'appel max
    call["incall_duration"].loc[call["incall_duration"] > 3600] = 3600
    call["start_time"] = call["start_time"].dt.tz_localize(None)

    AppelExpert = call.loc[call["UserRoleId"].isin(["00E1t000000DpBaEAK", "00E1t000000DpBVEA0"])]

    PremierAppelExpert = (
        AppelExpert[["pretto_user_name", "start_time"]].groupby(by="pretto_user_name", as_index=False).min()
    )
    PremierAppelExpert.columns = ["pretto_user_name", "Premier appel Expert"]
    call_2 = pd.merge(call, PremierAppelExpert, on=["pretto_user_name"], how="left")

    ringover_note = pd.merge(
        call_2,
        Note[
            [
                "accountid",
                "Id Salesforce",
                "Date_com",
                "Note",
                "date_signature_mandat",
                "delta_date_comm_date_mandat",
            ]
        ],
        on="accountid",
        how="inner",
    )

    # On ne conserve que les appels qui ont eu lieu avant le commentaire et 2 mois avant le commentaire

    ringover_note = ringover_note[(ringover_note["Date_com"] > ringover_note["start_time"])]
    # ringover_note = ringover_note[(ringover_note["date_signature_mandat"] > ringover_note["start_time"])]
    ringover_note = ringover_note[(ringover_note["Date_com"] - ringover_note["start_time"]).dt.days < 60]
    ringover_note["pretto_user_name_corrected"] = (
        ringover_note["pretto_user_name"].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")
    )
    ringover_note["pretto_user_name_corrected"] = ringover_note["pretto_user_name_corrected"].str.lower()

    # Correction des UserRoleId manquants

    ringover_note["Role"] = "autre"
    ringover_note["Role"].loc[ringover_note["UserRoleId"].isin(["00E1t000000DpBaEAK", "00E1t000000DpBVEA0"])] = "Expert"
    ringover_note["Role"].loc[ringover_note["UserRoleId"] == "00E1t000000DpBLEA0"] = "BuzDev"

    # ringover_note.drop(['direction', 'start_time', 'incall_duration'], axis=1, inplace=True)
    # ringover_note.sort_values(by=['sfid', 'start_time'], inplace=True)
    # ringover_note.drop_duplicates(keep='last', inplace=True)

    return ringover_note


#  Target for the test model


def target_definition_test():
    df_mandat = get_data_mandate_signed_test()
    df_mandat.rename(
        columns={"sfid": "Id Salesforce", "date_mandat": "date_signature_mandat"},
        inplace=True,
    )
    df_mandat["date_signature_mandat"] = pd.to_datetime(df_mandat["date_signature_mandat"], utc=None)
    df_mandat["date_signature_mandat"] = df_mandat["date_signature_mandat"].dt.round("1d")
    df_mandat.dropna(inplace=True)

    today = date.today()
    NPS_Avis = df_mandat.copy()
    NPS_Avis["Date_com"] = today.strftime("%Y-%m-%d")
    NPS_Avis["Date_com"] = pd.to_datetime(NPS_Avis["Date_com"], unit="ns", utc=None)
    NPS_Avis["Date_com"] = NPS_Avis["Date_com"].dt.round("1d")
    NPS_Avis.sort_values(by=["Id Salesforce", "Date_com"], inplace=True)
    NPS_Avis.reset_index(inplace=True, drop=True)
    NPS_Avis["date_signature_mandat"] = NPS_Avis["date_signature_mandat"].dt.tz_localize(None)
    NPS_Avis["delta_date_comm_date_mandat"] = (NPS_Avis["date_signature_mandat"] - NPS_Avis["Date_com"]).dt.days
    NPS_Avis = NPS_Avis.loc[NPS_Avis["delta_date_comm_date_mandat"] < 0]
    NPS_Avis.sort_values(
        by=["Id Salesforce", "delta_date_comm_date_mandat"],
        inplace=True,
        ascending=False,
    )
    NPS_Avis.reset_index(inplace=True, drop=True)
    NPS_Avis.drop_duplicates(subset="Id Salesforce", keep="first", inplace=True)
    NPS_Avis["Note"] = None

    return NPS_Avis


# Salesforce data for test


def load_salesforce_df_test(Note):
    Salesforce = get_data_sf_test()
    features_Salesforce = pd.merge(
        Salesforce, Note, left_on=["sfid", "accountid"], right_on=["Id Salesforce", "accountid"], how="inner"
    )

    ListeEtat = [
        "createddate",
        "date_a_joindre__c",
        "date_r1__c",
        "date_rech_no_doc__c",
        "date_rech_docs__c",
        "date_montage__c",
        "send_date__c",
        "date_offre_edit__c",
        "mandate_signed__c",
        "date_compromis",
        "date_perdue__c",
        "date_condition_suspensive",
    ]

    for col in ListeEtat:
        features_Salesforce[col] = (
            features_Salesforce[col].apply(lambda x: pd.to_datetime(x, errors="coerce")).dt.tz_localize(None)
        )

    for col in ListeEtat:
        features_Salesforce[col].loc[features_Salesforce[col] > features_Salesforce["Date_com"]] = None

    features_Salesforce = features_Salesforce.loc[features_Salesforce["Date_com"] > features_Salesforce["createddate"]]
    features_Salesforce.reset_index(drop=True, inplace=True)

    features_Salesforce["date_condition_suspensive"].loc[
        features_Salesforce["date_condition_suspensive"] < features_Salesforce["createddate"]
    ] = None

    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"] < features_Salesforce["date_montage__c"]) < 0
    ] = None

    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"] < features_Salesforce["send_date__c"]) < 0
    ] = None
    features_Salesforce["date_compromis"].loc[
        features_Salesforce["date_perdue__c"] < features_Salesforce["date_compromis"]
    ] = None
    features_Salesforce["date_compromis"].loc[
        ((features_Salesforce["date_condition_suspensive"] < features_Salesforce["date_compromis"]))
        & ((features_Salesforce["send_date__c"] < features_Salesforce["date_compromis"]))
    ] = None
    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"] - features_Salesforce["date_compromis"]).dt.days > 300
    ] = None
    features_Salesforce["date_condition_suspensive"].loc[
        (features_Salesforce["date_condition_suspensive"].isnull()) & (features_Salesforce["date_compromis"].notnull())
    ] = features_Salesforce["date_compromis"] + timedelta(93)
    features_Salesforce["date_compromis"].loc[
        (features_Salesforce["date_compromis"].isnull()) & (features_Salesforce["date_condition_suspensive"].notnull())
    ] = features_Salesforce["date_condition_suspensive"] - timedelta(93)

    return features_Salesforce


# Ringover data for test


def load_ringover_df_test(Note):
    call = get_data_ringover_test()
    ## On tronque les appels à e heure d'appel max
    call["incall_duration"].loc[call["incall_duration"] > 3600] = 3600
    call["start_time"] = call["start_time"].dt.tz_localize(None)

    AppelExpert = call.loc[call["UserRoleId"].isin(["00E1t000000DpBaEAK", "00E1t000000DpBVEA0"])]

    PremierAppelExpert = (
        AppelExpert[["pretto_user_name", "start_time"]].groupby(by="pretto_user_name", as_index=False).min()
    )
    PremierAppelExpert.columns = ["pretto_user_name", "Premier appel Expert"]
    call_2 = pd.merge(call, PremierAppelExpert, on=["pretto_user_name"], how="left")

    ringover_note = pd.merge(
        call_2,
        Note[
            [
                "accountid",
                "Id Salesforce",
                "Date_com",
                "date_signature_mandat",
                "delta_date_comm_date_mandat",
            ]
        ],
        on="accountid",
        how="inner",
    )

    # On ne conserve que les appels qui ont eu lieu avant le commentaire et 2 mois avant le commentaire

    ringover_note = ringover_note[(ringover_note["Date_com"] > ringover_note["start_time"])]
    # ringover_note = ringover_note[(ringover_note["date_signature_mandat"] > ringover_note["start_time"])]
    ringover_note = ringover_note[(ringover_note["Date_com"] - ringover_note["start_time"]).dt.days < 60]
    ringover_note["pretto_user_name_corrected"] = (
        ringover_note["pretto_user_name"].str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode("utf-8")
    )
    ringover_note["pretto_user_name_corrected"] = ringover_note["pretto_user_name_corrected"].str.lower()

    # Correction des UserRoleId manquants

    ringover_note["Role"] = "autre"
    ringover_note["Role"].loc[ringover_note["UserRoleId"].isin(["00E1t000000DpBaEAK", "00E1t000000DpBVEA0"])] = "Expert"
    ringover_note["Role"].loc[ringover_note["UserRoleId"] == "00E1t000000DpBLEA0"] = "BuzDev"

    # ringover_note.drop(['direction', 'start_time', 'incall_duration'], axis=1, inplace=True)
    # ringover_note.sort_values(by=['sfid', 'start_time'], inplace=True)
    # ringover_note.drop_duplicates(keep='last', inplace=True)

    return ringover_note

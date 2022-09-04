import numpy as np
import pandas as pd
import warnings
import datetime as dt

from datetime import timedelta, date, timezone

# Google Cloud / Airflow libraries
from google.cloud import bigquery, storage
from tempfile import TemporaryFile

# Display
warnings.filterwarnings("ignore")
client = bigquery.Client(project="pretto-apis")
pd.options.display.float_format = "{:.3f}".format


# RINGOVER


# Calcul du nombre de jours où il y a plusieurs appels le même jour
def compute_Nb_Jours_Plusieurs_appels(ringover_prep):
    ringover_note = ringover_prep.copy()
    features_Nb_Appels = []
    ringover_note_real_call = ringover_note.loc[ringover_note["incall_duration"] > 10]
    category = ["global", "7_derniers_jours", "14_derniers_jours"]
    for cat in category:
        if cat == "global":
            data = ringover_note_real_call
        elif cat == "7_derniers_jours":
            data = ringover_note_real_call.loc[
                (ringover_note_real_call["Date_com"] - ringover_note_real_call["start_time"]).dt.days < 7
            ]
        elif cat == "14_derniers_jours":
            data = ringover_note_real_call.loc[
                (ringover_note_real_call["Date_com"] - ringover_note_real_call["start_time"]).dt.days < 14
            ]
        else:
            print("Unrecognized key ! ", cat)
        data["start_time"] = data["start_time"].dt.floor("d")
        data = (
            data[["accountid", "sfid", "Date_com", "start_time", "incall_duration"]]
            .groupby(by=["accountid", "sfid", "Date_com", "start_time"], as_index=False)
            .count()
        )
        data.drop(["start_time"], axis=1, inplace=True)
        data.columns = ["accountid", "sfid", "Date_com", "Nb_Jours_Plusieurs_appels"]
        PlusQuUnAppelParJour = data.loc[data["Nb_Jours_Plusieurs_appels"] > 1]
        PlusQuUnAppelParJour = PlusQuUnAppelParJour.groupby(by=["accountid", "sfid", "Date_com"]).count()

        for current_feature in [PlusQuUnAppelParJour]:
            for columnName in current_feature.columns:
                current_feature = current_feature.rename(columns={columnName: columnName + "_" + cat})
            features_Nb_Appels.append(current_feature)
    Nb_Jours_Plusieurs_appels = pd.concat(features_Nb_Appels, axis=1)
    Nb_Jours_Plusieurs_appels = Nb_Jours_Plusieurs_appels.replace(np.nan, 0)

    return Nb_Jours_Plusieurs_appels


# Calcul de l'expérience de l'expert au moment du premier appel avec le client
def compute_Experience_expert(ringover_prep):
    ringover_note = ringover_prep.copy()
    ExperienceExpert = (
        ringover_note[
            [
                "accountid",
                "sfid",
                "Date_com",
                "start_time",
                "pretto_user_name",
                "Premier appel Expert",
            ]
        ]
        .groupby(by=["accountid", "sfid", "Date_com", "pretto_user_name"], as_index=False)
        .min()
    )
    ExperienceExpert["Experience_EC_jours"] = (
        ExperienceExpert["start_time"] - ExperienceExpert["Premier appel Expert"]
    ).dt.days
    ExperienceExpert["Experience_EC_jours"].loc[ExperienceExpert["Experience_EC_jours"] < 0] = 0
    ExperienceExpert = (
        ExperienceExpert[["accountid", "sfid", "Date_com", "Experience_EC_jours"]]
        .groupby(by=["accountid", "sfid", "Date_com"], as_index=False)
        .median()
    )

    return ExperienceExpert


# Création de la feature qui compte le nombre fois où un client n'a pas reçu d'appel de Pretto pendant 14 jours consécutifs
def compute_TempsEntreAppelPrettoSup14(ringover_prep):
    ringover_note = ringover_prep.copy()
    RingoverSorted = ringover_note.loc[ringover_note["incall_duration"] > 10].sort_values(
        by=["accountid", "sfid", "Date_com", "start_time"], ascending=True
    )

    # On ne prend que les appels sortant
    RingoverSortedOut = RingoverSorted.loc[RingoverSorted["direction"] == "out"]
    RingoverSortedOut["Temps entre chaque appel de Pretto"] = (
        RingoverSortedOut.groupby(["accountid", "sfid", "Date_com"])["start_time"].diff()
    ).dt.days
    TempsEntreAppelPretto = RingoverSortedOut.loc[RingoverSortedOut["Temps entre chaque appel de Pretto"] > 14]
    TempsEntreAppelPrettoSup14 = (
        TempsEntreAppelPretto[
            [
                "accountid",
                "sfid",
                "start_time",
                "Date_com",
                "Temps entre chaque appel de Pretto",
            ]
        ]
        .groupby(by=["accountid", "sfid", "Date_com"], as_index=False)["Temps entre chaque appel de Pretto"]
        .count()
    )
    TempsEntreAppelPrettoSup14.columns = [
        "accountid",
        "sfid",
        "Date_com",
        "Nb de fois sans appel depuis 14j par Pretto",
    ]

    return TempsEntreAppelPrettoSup14


# Création des features sur le nombre d'appels, la proportion d'appels entrants et sortant par experts, BuzDev et Customer Care


def compute_Appels_repartition(ringover_prep):
    ringover_note = ringover_prep.copy()
    all_features = []
    categoryToCreate = ["global", "expert", "buzdev", "customer_care"]
    for cat in categoryToCreate:
        if cat == "global":
            sub = ringover_note
            # print(sub)
        elif cat == "expert":
            sub = ringover_note.loc[ringover_note["UserRoleId"].isin(["00E1t000000DpBaEAK", "00E1t000000DpBVEA0"])]
        elif cat == "buzdev":
            sub = ringover_note.loc[ringover_note["UserRoleId"] == "00E1t000000DpBLEA0"]
        elif cat == "customer_care":
            sub = ringover_note.loc[
                (ringover_note["UserRoleId"] == "00E1t000000DpBLEA0")
                & (ringover_note["manager_name"] == "Baptiste Gardrat")
            ]
        else:
            print("Unrecognized key ! ", cat)

        nombreAppels = pd.pivot_table(
            data=sub, index=["accountid", "sfid", "Date_com"], values="start_time", aggfunc="count"
        )
        if len(nombreAppels) != 0:
            nombreAppels.columns = ["nombre appels"]

        AppelEntrantSortant = pd.pivot_table(
            data=sub, index=["accountid", "sfid", "Date_com"], columns="direction", values="start_time", aggfunc="count"
        )
        AppelEntrantSortant = AppelEntrantSortant.replace(np.nan, 0)
        if "in" in AppelEntrantSortant.columns:
            if "out" in AppelEntrantSortant.columns:
                AppelEntrantSortant["prct_in"] = (
                    100 * AppelEntrantSortant["in"] / (AppelEntrantSortant["in"] + AppelEntrantSortant["out"])
                )
                AppelEntrantSortant["prct_out"] = (
                    100 * AppelEntrantSortant["out"] / (AppelEntrantSortant["in"] + AppelEntrantSortant["out"])
                )

        for current_feature in [nombreAppels, AppelEntrantSortant]:
            for columnName in current_feature.columns:
                current_feature = current_feature.rename(columns={columnName: columnName + "_" + cat})
            all_features.append(current_feature)
    Appels_repartition = pd.concat(all_features, axis=1)
    Appels_repartition = Appels_repartition.replace(np.nan, 0)

    return Appels_repartition


# Calcul du nombre d'experts différents assignés au client
def compute_NbAppelDiffExper(ringover_prep):
    ringover_note = ringover_prep.copy()
    call_Plus_de_10_sec = ringover_note.loc[ringover_note["incall_duration"] > 10]
    NbAppelParExpertParAccount = (
        call_Plus_de_10_sec.loc[
            ~call_Plus_de_10_sec["manager_name"].isin(["Sirisack Sirimanotham", "Baptiste Gardrat"])
        ]
        .groupby(["accountid", "sfid", "Date_com", "pretto_user_name_corrected"])["start_time"]
        .count()
        .reset_index(drop=False)
    )
    NbAppelParExpertParAccount = NbAppelParExpertParAccount[NbAppelParExpertParAccount["start_time"] > 2]
    NbAppelDiffExpert = (
        NbAppelParExpertParAccount.groupby(["accountid", "sfid", "Date_com"])["pretto_user_name_corrected"]
        .nunique()
        .reset_index(drop=False)
    )
    NbAppelDiffExpert.columns = ["accountid", "sfid", "Date_com", "Nb experts différents"]

    return NbAppelDiffExpert


# Nombre d'experts différents que le client a eu au téléphone
def compute_NbAppelDiffInterlocuteurs(ringover_prep):
    ringover_note = ringover_prep.copy()
    call_Plus_de_10_sec = ringover_note.loc[ringover_note["incall_duration"] > 10]
    NbAppelDiffInterlocuteurs = (
        call_Plus_de_10_sec.loc[
            ~call_Plus_de_10_sec["manager_name"].isin(["Sirisack Sirimanotham", "Baptiste Gardrat"])
        ]
        .groupby(["accountid", "sfid", "Date_com"])["pretto_user_name_corrected"]
        .nunique()
        .reset_index(drop=False)
    )
    NbAppelDiffInterlocuteurs.columns = [
        "accountid",
        "sfid",
        "Date_com",
        "Nb interlocuteurs différents",
    ]

    return NbAppelDiffInterlocuteurs


# Date du premier appel Pretto - client
def compute_datePremierAppel(ringover_prep):
    ringover_note = ringover_prep.copy()
    call_Plus_de_10_sec = ringover_note.loc[ringover_note["incall_duration"] > 10]
    datePremierAppel = pd.pivot_table(
        data=call_Plus_de_10_sec,
        index=["accountid", "sfid", "Date_com"],
        values="start_time",
        aggfunc="min",
    )

    return datePremierAppel


# Date du dernier appel Pretto - client
def compute_dateDernierAppel(ringover_prep):
    ringover_note = ringover_prep.copy()
    call_Plus_de_10_sec = ringover_note.loc[ringover_note["incall_duration"] > 10]
    dateDernierAppel = pd.pivot_table(
        data=call_Plus_de_10_sec,
        index=["accountid", "sfid", "Date_com"],
        values="start_time",
        aggfunc="max",
    )

    return dateDernierAppel


# Nombre de messages répondeur laissés par l'EC à son client
def compute_Message_repondeur(ringover_prep):
    ringover_note = ringover_prep.copy()
    ringover_note = ringover_note.loc[
        (ringover_note["incall_duration"] >= 20)
        & (ringover_note["incall_duration"] < 60)
        & (ringover_note["direction"] == "out")
    ]

    nombreMessages = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)[
        "incall_duration"
    ].count()
    nombreMessages.columns = ["accountid", "sfid", "Date_com", "nombre messages repondeur"]
    nombreMessages.fillna(0, inplace=True)

    return nombreMessages


# Durée motenne des appels entre pretto et le client
def compute_dureeMoyenneAppel(ringover_prep):
    ringover_note = ringover_prep.copy()
    duréeMoyenneAppel = pd.pivot_table(
        data=ringover_note,
        index=["accountid", "sfid", "Date_com"],
        values="incall_duration",
        aggfunc="mean",
    )
    duréeMoyenneAppel.columns = ["Durée moyenne des appels"]

    return duréeMoyenneAppel


# Pourcentaage d'appels Sans réponse
def compute_NbAppelsansreponseIn(ringover_prep):
    ringover_note = ringover_prep.copy()
    NbAppelsansreponseIn = pd.pivot_table(
        data=ringover_note.loc[
            (ringover_note["direction"] == "in")
            & (ringover_note["incall_duration"].isnull())
            & (
                ringover_note["UserRoleId"].isin(["00E1t000000DpBaEAK", "00E1t000000DpBVEA0"])
            )  # uniquement les experts et les managers
        ],
        index=["accountid", "sfid", "Date_com"],
        values="direction",
        aggfunc="count",
    )
    NbAppelsansreponseIn.columns = ["Nb_appels_in_sans_reponse"]

    return NbAppelsansreponseIn


# Nombre d'appels sans réponse
def compute_NbAppelsansreponseOut(ringover_prep):
    ringover_note = ringover_prep.copy()
    NbAppelsansreponseOut = pd.pivot_table(
        data=ringover_note.loc[(ringover_note["direction"] == "out") & (ringover_note["incall_duration"].isnull())],
        index=["accountid", "sfid", "Date_com"],
        values="direction",
        aggfunc="count",
    )
    NbAppelsansreponseOut.columns = ["Nb appel out sans reponse"]

    return NbAppelsansreponseOut


# Durée totale des appels entrants (ie client vers EC)
def compute_duree_appels_entrants(ringover_prep):
    ringover_note = ringover_prep.copy()
    Duree_AppelsEntrants = pd.pivot_table(
        data=ringover_note.loc[(ringover_note["direction"] == "in")],
        index=["accountid", "sfid", "Date_com"],
        values="incall_duration",
        aggfunc="sum",
    )
    Duree_AppelsEntrants.columns = ["Duree_AppelsEntrants"]

    return Duree_AppelsEntrants


# Fréquence des appels entre Pretto et le client
def compute_frequenceAppel(ringover_prep):
    ringover_note = ringover_prep.copy()
    datePremierAppel = compute_datePremierAppel(ringover_note)
    dateDernierAppel = compute_dateDernierAppel(ringover_note)
    Appels_repartition = compute_Appels_repartition(ringover_note)
    frequenceAppel = dateDernierAppel - datePremierAppel
    frequenceAppel["start_time"] = frequenceAppel["start_time"].dt.days / Appels_repartition["nombre appels_global"]
    frequenceAppel.columns = ["attente moyenne entre deux appels"]

    return frequenceAppel


def compute_Nb_Appel_Sup_10_min(ringover_prep):
    Appel_Sup_10_min = ringover_prep.loc[ringover_prep["incall_duration"] > 600].copy()
    Nb_Appel_Sup_10_min = (
        Appel_Sup_10_min[["accountid", "sfid", "Date_com", "incall_duration"]]
        .groupby(["accountid", "sfid", "Date_com"])
        .count()
    )
    Nb_Appel_Sup_10_min.columns = ["Nb_Appel_Sup_10_min"]

    return Nb_Appel_Sup_10_min


def compute_Is_Dernier_Appel_Out(ringover_prep):
    ringover_sorted = ringover_prep.sort_values(
        by=["accountid", "sfid", "Date_com", "start_time"], ascending=False
    ).copy()
    Dernier_Appel = (
        ringover_sorted[["accountid", "sfid", "Date_com", "start_time", "direction"]]
        .groupby(["accountid", "sfid", "Date_com"])
        .first()
    )
    Dernier_Appel["Is_Dernier_Appel_Out"] = 1
    Dernier_Appel["Is_Dernier_Appel_Out"].loc[Dernier_Appel["direction"] == "in"] = 0
    Dernier_Appel.drop(["direction", "start_time"], axis=1, inplace=True)

    return Dernier_Appel


def compute_average_incall_duration(ringover_prep):
    ringover_note = ringover_prep.copy()
    average_incall_duration = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)[
        "incall_duration"
    ].mean()
    average_incall_duration.columns = [
        "accountid",
        "sfid",
        "Date_com",
        "average_incall_duation",
    ]

    return average_incall_duration


def compute_total_incall_duration(ringover_prep):
    ringover_note = ringover_prep.copy()
    total_incall_duration = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)[
        "incall_duration"
    ].sum()
    total_incall_duration.columns = ["accountid", "sfid", "Date_com", "duree_totale_appels"]

    return total_incall_duration


def compute_appel_3_minutes(ringover_prep):
    ringover_note = ringover_prep.copy()
    ringover_note = ringover_note.loc[ringover_note["incall_duration"] >= 180]
    appel_3 = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)["incall_duration"].count()
    appel_3.columns = ["accountid", "sfid", "Date_com", "nb_appel_sup_3_min"]

    return appel_3


import copy


def compute_nb_jours_appels(ringover_prep):
    ringover_note = copy.deepcopy(ringover_prep)
    nb_jours_appels = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)["start_time"].count()
    nb_jours_appels.columns = ["accountid", "sfid", "Date_com", "nb_jours_appels"]

    return nb_jours_appels


def compute_appels_manques(ringover_prep):
    ringover_note = ringover_prep.copy()
    data_ringo_appelmanque = pd.pivot_table(
        data=ringover_note,
        index=["accountid", "sfid"],
        columns=["Appel_manque"],
        values="start_time",
        aggfunc="count",
    )

    return data_ringo_appelmanque


def compute_nb_jours_appels_manques(ringover_prep):
    ringover_note = ringover_prep.copy()
    nb_jours_appels_manques = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)[
        "Appel_manque"
    ].count()
    nb_jours_appels_manques.columns = [
        "accountid",
        "sfid",
        "Date_com",
        "nb_jours_appels_manques",
    ]

    return nb_jours_appels_manques


def compute_nb_jours_appels_manques_sans_rappel(ringover_prep):
    ringover_note = ringover_prep.copy()
    ringover_note = ringover_note.loc[ringover_note["Appel_manque"] == "Pas de rappel"]
    nb_jours_appels_manques_sans_rappel = ringover_note.groupby(by=["accountid", "sfid", "Date_com"], as_index=False)[
        "Appel_manque"
    ].count()
    nb_jours_appels_manques_sans_rappel.columns = [
        "accountid",
        "sfid",
        "Date_com",
        "nb_jours_appels_manques_sans_rappel",
    ]

    return nb_jours_appels_manques_sans_rappel


def compute_qualite_ec(ringover_prep):
    ringover_note = ringover_prep.copy()
    retards_table = ringover_note[
        [
            "accountid",
            "sfid",
            "Date_com",
            "no_show",
            "on_time",
            "moins_5_min_retard",
            "entre_5_15_min_retard",
            "plus_15_min_retard",
        ]
    ]

    return retards_table


def compute_expert_en_vacances_depuis_dernier_appel(ringover_prep):
    ringover_sorted = ringover_prep.sort_values(
        by=["accountid", "sfid", "Date_com", "start_time"], ascending=False
    ).copy()

    Dernier_Appel = (
        ringover_sorted[["accountid", "sfid", "Date_com", "start_time", "Vacances"]]
        .groupby(["accountid", "sfid", "Date_com"])
        .first()
    )

    Expert_en_vacances = Dernier_Appel.copy()
    Expert_en_vacances["expert_en_vacs"] = 0
    Expert_en_vacances["expert_en_vacs"].loc[Expert_en_vacances["Vacances"] == "Vacances"] = 1

    Expert_en_vacances.drop(["Vacances", "start_time"], axis=1, inplace=True)

    return Expert_en_vacances


# Merge all features ringover
def create_all_features_ringover(ringover_note):
    dateDernierAppel = compute_dateDernierAppel(ringover_note)
    datePremierAppel = compute_datePremierAppel(ringover_note)
    dateDernierAppel.columns = ["Date dernier appel"]
    datePremierAppel.columns = ["Date premier appel"]
    duréeMoyenneAppel = compute_dureeMoyenneAppel(ringover_note)
    NbAppelsansreponseIn = compute_NbAppelsansreponseIn(ringover_note)
    NbAppelsansreponseOut = compute_NbAppelsansreponseOut(ringover_note)
    Nb_Jours_Plusieurs_appels = compute_Nb_Jours_Plusieurs_appels(ringover_note)
    NbAppelDiffExpert = compute_NbAppelDiffExper(ringover_note)
    NbAppelDiffInterlocuteurs = compute_NbAppelDiffInterlocuteurs(ringover_note)
    TempsEntreAppelPrettoSup14 = compute_TempsEntreAppelPrettoSup14(ringover_note)
    ExperienceExpert = compute_Experience_expert(ringover_note)
    Nb_Appel_Sup_10_min = compute_Nb_Appel_Sup_10_min(ringover_note)
    Appels_repartition = compute_Appels_repartition(ringover_note)
    frequenceAppel = compute_frequenceAppel(ringover_note)
    Dernier_Appel = compute_Is_Dernier_Appel_Out(ringover_note)
    average_incall_duration = compute_average_incall_duration(ringover_note)
    appel_3_minutes_plus = compute_appel_3_minutes(ringover_note)
    nb_jours_appels = compute_nb_jours_appels(ringover_note)
    AppelManque = compute_appels_manques(ringover_note)
    MessagesRepondeur = compute_Message_repondeur(ringover_note)
    Jour_AppelsManques = compute_nb_jours_appels_manques(ringover_note)
    Jour_AppelsManques_SansRappel = compute_nb_jours_appels_manques_sans_rappel(ringover_note)
    Duree_AppelsEntrants = compute_duree_appels_entrants(ringover_note)
    Total_incall_duration = compute_total_incall_duration(ringover_note)
    Retards = compute_qualite_ec(ringover_note)
    Vacances = compute_expert_en_vacances_depuis_dernier_appel(ringover_note)

    features_ringover = pd.concat(
        [
            Appels_repartition,
            frequenceAppel,
            dateDernierAppel,
            datePremierAppel,
            duréeMoyenneAppel,
            NbAppelsansreponseIn,
            NbAppelsansreponseOut,
            Nb_Jours_Plusieurs_appels,
            Nb_Appel_Sup_10_min,
            Dernier_Appel,
        ],
        axis=1,
    )

    features_ringover["Nb_appels_in_sans_reponse"].fillna(0, inplace=True)
    features_ringover["Nb appel out sans reponse"].fillna(0, inplace=True)
    features_ringover["Durée moyenne des appels"].fillna(0, inplace=True)

    features_ringover = pd.merge(features_ringover, NbAppelDiffExpert, on=["accountid", "sfid", "Date_com"], how="left")
    features_ringover = pd.merge(
        features_ringover,
        NbAppelDiffInterlocuteurs,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    features_ringover = pd.merge(
        features_ringover,
        average_incall_duration,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    features_ringover = pd.merge(
        features_ringover,
        appel_3_minutes_plus,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    features_ringover = pd.merge(features_ringover, nb_jours_appels, on=["accountid", "sfid", "Date_com"], how="left")
    features_ringover = pd.merge(features_ringover, AppelManque, on=["accountid", "sfid"], how="left")
    features_ringover = pd.merge(features_ringover, MessagesRepondeur, on=["accountid", "sfid", "Date_com"], how="left")
    features_ringover = pd.merge(
        features_ringover, Jour_AppelsManques, on=["accountid", "sfid", "Date_com"], how="left"
    )
    features_ringover = pd.merge(
        features_ringover,
        Jour_AppelsManques_SansRappel,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    features_ringover = pd.merge(
        features_ringover,
        Duree_AppelsEntrants,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    features_ringover = pd.merge(
        features_ringover,
        Total_incall_duration,
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )

    features_ringover = pd.merge(features_ringover, Retards, on=["accountid", "sfid", "Date_com"], how="left")
    features_ringover = pd.merge(features_ringover, Vacances, on=["accountid", "sfid", "Date_com"], how="left")

    features_ringover["Nb experts différents"].fillna(0, inplace=True)
    features_ringover["Pourcentage appels manqués in"] = (
        features_ringover["Nb_appels_in_sans_reponse"] / features_ringover["in_global"]
    ).fillna(0)
    features_ringover["Pourcentage appels manqués out"] = (
        features_ringover["Nb appel out sans reponse"] / features_ringover["out_global"]
    ).fillna(0)
    ringover_note_date = (
        ringover_note[["accountid", "sfid", "start_time", "date_signature_mandat", "Date_com"]]
        .groupby(by=["accountid", "sfid", "Date_com"], as_index=False)
        .max()
    )
    features_ringover = features_ringover.reset_index(drop=False)

    all_features_ringover = pd.merge(
        features_ringover, ringover_note_date, on=["accountid", "sfid", "Date_com"], how="inner"
    )
    all_features_ringover = pd.merge(
        all_features_ringover,
        TempsEntreAppelPrettoSup14[["accountid", "sfid", "Date_com", "Nb de fois sans appel depuis 14j par Pretto"]],
        on=["accountid", "sfid", "Date_com"],
        how="left",
    )
    all_features_ringover = pd.merge(
        all_features_ringover,
        ExperienceExpert[["accountid", "sfid", "Date_com", "Experience_EC_jours"]],
        on=["accountid", "sfid", "Date_com"],
        how="inner",
    )
    print(all_features_ringover.shape)

    all_features_ringover["Temps appel total"] = (
        all_features_ringover["nombre appels_global"] * all_features_ringover["Durée moyenne des appels"]
    )

    all_features_ringover["Attente_depuis_dernier_appel"] = (
        all_features_ringover["Date_com"] - all_features_ringover["Date dernier appel"]
    ).dt.days

    all_features_ringover["Attente depuis premier appel"] = (
        all_features_ringover["Date_com"] - all_features_ringover["Date premier appel"]
    ).dt.days

    all_features_ringover["Total_appels_manques"] = (
        all_features_ringover["Pas de rappel"] + all_features_ringover["Rappel le même jour"]
    )
    all_features_ringover["prt_appel_manque_sur_appels"] = (
        all_features_ringover["nb_jours_appels_manques"] / all_features_ringover["nb_jours_appels"]
    )
    all_features_ringover["ppt_appelmanque_sans_rappel"] = (
        all_features_ringover["nb_jours_appels_manques_sans_rappel"] / all_features_ringover["nb_jours_appels"]
    )
    all_features_ringover["Total_appels_manques"].fillna(0, inplace=True)

    all_features_ringover["duree_in_sur_duree_total"] = (
        all_features_ringover["Duree_AppelsEntrants"] / all_features_ringover["duree_totale_appels"]
    )

    nan_cols = [
        "Nb de fois sans appel depuis 14j par Pretto",
        "Nb_Jours_Plusieurs_appels_global",
        "Nb_Jours_Plusieurs_appels_7_derniers_jours",
        "Nb_Jours_Plusieurs_appels_14_derniers_jours",
        "Nb_Appel_Sup_10_min",
        "nb_appel_sup_3_min",
        "Rappel le même jour",
        "ppt_appelmanque_sans_rappel",
        "prt_appel_manque_sur_appels",
        "Pas de rappel",
        "duree_in_sur_duree_total",
        "nb_jours_appels_manques_sans_rappel",
        "Duree_AppelsEntrants",
        "attente moyenne entre deux appels",
        "Nb interlocuteurs différents",
        "average_incall_duation",
        "nombre messages repondeur",
        "Experience_EC_jours",
    ]

    for i in nan_cols:
        all_features_ringover[i].fillna(all_features_ringover[i].median(), inplace=True)

    all_features_ringover.dropna(inplace=True)
    all_features_ringover.reset_index(inplace=True, drop=True)

    zero_cols = ["Date_com", "no_show", "on_time", "moins_5_min_retard", "entre_5_15_min_retard", "plus_15_min_retard"]

    for i in zero_cols:
        all_features_ringover[i].fillna(0, inplace=True)

    all_features_ringover.dropna(inplace=True)
    all_features_ringover.reset_index(inplace=True, drop=True)

    return all_features_ringover


# SALESFORCE


# Features SF
def ajoutDelta(data):
    ListeEtat = ["date_r1__c", "date_montage__c", "send_date__c"]

    for col in range(len(ListeEtat) - 1):
        data[ListeEtat[col + 1] + "-" + ListeEtat[col]] = (data[ListeEtat[col + 1]] - data[ListeEtat[col]]).dt.days

    return data


# Dernière date changement d'Etat :
def compute_derniere_date_changement_etat(data):
    ListeEtat = [
        "date_a_joindre__c",
        "date_r1__c",
        "date_rech_no_doc__c",
        "date_rech_docs__c",
        "date_montage__c",
        "send_date__c",
    ]

    # Trouver la date de dernier changement d'état
    data["Dernier Etat avant avis"] = data[ListeEtat].max(axis=1, numeric_only=False)
    data["nom dernier etat"] = ""

    for etat in ListeEtat:
        data["nom dernier etat"].loc[data["Dernier Etat avant avis"] == data[etat]] = etat

    return data


# Dernière date changement d'Etat avant perdue:
def compute_derniere_date_changement_etat_avant_perdue(data):
    ListeEtat = [
        "date_a_joindre__c",
        "date_r1__c",
        "date_rech_no_doc__c",
        "date_rech_docs__c",
        "date_montage__c",
        "send_date__c",
    ]

    # Trouver la date de dernier changement d'état
    for col in ListeEtat:
        data[col] = data[col].apply(lambda x: pd.to_datetime(x, errors="coerce")).dt.tz_localize(None)

    data["Dernier Etat avant avis avant perdue"] = data[ListeEtat].max(axis=1, numeric_only=False)
    data["Date_com-dernier etat avant perdue et avis"] = (
        data["Date_com"] - data["Dernier Etat avant avis avant perdue"]
    ).dt.days

    return data


# Deltatime entre les différentes étapes du parcours
def compute_delta_time(data):
    data["dt_commentaire_suspensive"] = (data["Date_com"] - data["date_condition_suspensive"]).dt.days
    data["dt_creation_envoi"] = (data["send_date__c"] - data["createddate"]).dt.days
    data["dt_creation_mandat"] = (data["date_signature_mandat"] - data["createddate"]).dt.days
    data["dt_commentaire_mandat"] = (data["Date_com"] - data["date_signature_mandat"]).dt.days
    data["dt_envoi_commentaire"] = (data["Date_com"] - data["send_date__c"]).dt.days

    return data


def create_features_salesforce(features_Salesforce):
    features_Salesforce = ajoutDelta(features_Salesforce)
    features_Salesforce = compute_derniere_date_changement_etat(features_Salesforce)
    features_Salesforce = compute_derniere_date_changement_etat_avant_perdue(features_Salesforce)
    features_Salesforce = compute_delta_time(features_Salesforce)

    return features_Salesforce

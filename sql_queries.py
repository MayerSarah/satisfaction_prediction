# Data librairies
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import datetime as dt

# Google Cloud / Airflow libraries
from google.cloud import bigquery, storage
from tempfile import TemporaryFile

# Display
warnings.filterwarnings("ignore")
client = bigquery.Client(project="pretto-apis")
pd.options.display.float_format = "{:.3f}".format


# Train Data


def get_data_ringover_train():
    query = """
 WITH
registre_appels AS (
SELECT
o.accountid,
o.sfid,
DATE(start_time) AS date_appel,
incall_duration,
direction,
FROM
`pretto-apis.metabase.ringover_calls` c
INNER JOIN
`pretto-apis.metabase.opportunities` l
ON
c.project_id=l.project_id
INNER JOIN
`pretto-apis.salesforce.opportunity` o
ON
l.opportunity_sfid=o.sfid ),
appels_manques AS (
SELECT
accountid,
sfid,
DATE(date_appel) AS date_appel_manque
FROM
registre_appels
WHERE
(incall_duration < 60
  OR incall_duration IS NULL)
AND direction="in" ),
nb_appels_jour_appel_manque AS (
SELECT
registre_appels.accountid,
registre_appels.sfid,
date_appel,
CASE WHEN COUNTIF(registre_appels.incall_duration < 5) = 0 THEN "Pas de rappel"
ELSE "Rappel le même jour"
END as Appel_manque
FROM
appels_manques
LEFT JOIN
registre_appels
ON
registre_appels.sfid = appels_manques.sfid
AND registre_appels.accountid = appels_manques.accountid
AND registre_appels.date_appel = appels_manques.date_appel_manque
GROUP BY
date_appel,
accountid,
sfid),
  time_schedule AS (
  SELECT
    accountid,
    ownerid,
    DATE_TRUNC(startdatetime,day) AS jour_rdv,
    MIN(startdatetime) heure_rdv,
    user.name as expert_name
  FROM
    salesforce.event event
  LEFT JOIN
    salesforce.user user
    ON event.ownerid = user.sfid
  WHERE
    isdeleted =FALSE
    AND LOWER(status__c) LIKE '%confirmed%'
    AND type='Call'
    AND subject LIKE '%téléphonique%'
  GROUP BY
    accountid,
    ownerid,
    expert_name,
    jour_rdv),
  time_realised AS (
  SELECT
    opp.accountid,
    DATE_TRUNC(task.createddate,day) AS jour_appel,
    MIN(task.createddate) AS heure_premier_appel,
    user.name AS expert_name
  FROM
    salesforce.task task
  LEFT JOIN
    salesforce.opportunity opp
  ON
    task.whatid=opp.sfid
  LEFT JOIN
    salesforce.user user
  ON
    task.ownerid=user.sfid
  WHERE
    LOWER(subject) LIKE '%inbound%'
    OR LOWER(subject) LIKE '%outbound%' OR(LOWER(subject) LIKE '%appel%'
      AND LOWER(subject) LIKE '%démarr%')
    OR LOWER(subject) LIKE '%missed%'
    AND user.UserRoleId IN ("00E1t000000DpBaEAK",
      "00E1t000000DpBVEA0")
  GROUP BY
    accountid,
    jour_appel,
    expert_name),
    retards_and_noshow as (
SELECT
    COUNTIF(time_realised.heure_premier_appel IS NULL) as no_show,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)<1) as on_time,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)<5) as moins_5_min_retard,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)<15) as entre_5_15_min_retard,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)>=15) as plus_15_min_retard,
    time_schedule.expert_name
FROM
  time_schedule
LEFT JOIN
  time_realised
ON
  time_schedule.accountid=time_realised.accountid
  AND time_schedule.jour_rdv=time_realised.jour_appel
LEFT JOIN
  salesforce.user user
ON
  time_schedule.ownerid=user.sfid
GROUP BY
  expert_name
ORDER BY
  expert_name)

SELECT
o.sfid,
o.accountid,
c.direction,
start_time,
c.incall_duration,
c.pretto_user_name,
u.name as expert_name,
c.manager_name,
u.UserRoleId,
nbm.Appel_manque,
retards.no_show,
retards.on_time,
retards.moins_5_min_retard,
retards.entre_5_15_min_retard,
retards.plus_15_min_retard,
retards.expert_name,
IF (DATE(start_time) = DATE(vac.Date__c), "Vacances", "Work") as Vacances
--cast(c.start_time as time)

FROM
`pretto-apis.metabase.ringover_calls` c
INNER JOIN
`pretto-apis.metabase.opportunities` l
ON
c.project_id=l.project_id
INNER JOIN
`pretto-apis.salesforce.opportunity` o
ON
l.opportunity_sfid=o.sfid
LEFT JOIN
salesforce.user AS u
ON
u.email=users_email 
LEFT JOIN
nb_appels_jour_appel_manque nbm
ON nbm.sfid = o.sfid
AND nbm.accountid = o.accountid
AND DATE(start_time) = nbm.date_appel

LEFT JOIN 
retards_and_noshow as retards
ON 
u.name = retards.expert_name
LEFT JOIN 
  salesforce.absence vac
ON
  vac.User__c = u.sfid
  and DATE(start_time) = DATE(vac.Date__c)
WHERE
o.sfid IS NOT NULL AND o.accountid IS NOT NULL
AND o.recordtypeid = '0121t000000E3iEAAS'

ORDER BY
o.sfid
        """
    call = client.query(query).to_dataframe()

    return call


def get_data_sf_train():
    query = """
SELECT
  o.sfid,
  CASE
    WHEN DATE(date_compromis) > CURRENT_DATE() OR date_compromis < "2017-01-01" THEN NULL
  ELSE
  date_compromis
  END AS date_compromis,
  date_condition_suspensive,
  l.maturity_at_onboarding,
  l.date_R1_realised AS date_r1__c,
  send_date__c,
  date_accorde__c,
  date_offre_edit__c,
  date_offre_sign__c,
  date_a_joindre__c,
  date_rech_no_doc__c,
  date_rech_docs__c,
  date_montage__c,
  date_fonds__c,
  date_perdue__c,
  o.accountid,
  o.createddate,
  o.mandate_signed__c,
  o.stagename,
  l.canal,
  l.sub_canal,
  u.name,
  l.project_kind,
  lead_type,
  o.recordtypeid,
FROM
  metabase.opportunities AS l
INNER JOIN
  salesforce.opportunity AS o
ON
  o.sfid=l.opportunity_sfid
LEFT JOIN
  salesforce.user AS u
ON
  o.ownerid=u.sfid
WHERE
  --(sc.name = "Pretto.fr" or sc.name = "Parrainage")
  o.mandate_signed__c IS NOT NULL
  AND o.recordtypeid = '0121t000000E3iEAAS'

  """

    Salesforce = client.query(query).to_dataframe()

    return Salesforce


def get_data_mandate_signed_train():
    query = """
SELECT
  received_at,
  mb.opportunity_sfid

FROM
events.mandate_approved map

LEFT JOIN
  metabase.bank_requests mb
ON  
context_project_id = mb.project_id
  """

    Mandat = client.query(query).to_dataframe()

    return Mandat


# Test Data


def get_data_ringover_test():
    query = """
  WITH
  registre_appels AS (
  SELECT
    o.accountid,
    o.sfid,
    DATE(start_time) AS date_appel,
    incall_duration,
    direction,
  FROM
    `pretto-apis.metabase.ringover_calls` c
  INNER JOIN
    `pretto-apis.metabase.opportunities` l
  ON
    c.project_id=l.project_id
  INNER JOIN
    `pretto-apis.salesforce.opportunity` o
  ON
    l.opportunity_sfid=o.sfid ),
  appels_manques AS (
  SELECT
    accountid,
    sfid,
    DATE(date_appel) AS date_appel_manque
  FROM
    registre_appels
  WHERE
    (incall_duration < 60
      OR incall_duration IS NULL)
    AND direction="in" ),
  nb_appels_jour_appel_manque AS (
  SELECT
    registre_appels.accountid,
    registre_appels.sfid,
    date_appel,
    CASE WHEN COUNTIF(registre_appels.incall_duration < 5) = 0 THEN "Pas de rappel"
    ELSE "Rappel le même jour"
    END as Appel_manque
  FROM
    appels_manques
  LEFT JOIN
    registre_appels
  ON
    registre_appels.sfid = appels_manques.sfid
    AND registre_appels.accountid = appels_manques.accountid
    AND registre_appels.date_appel = appels_manques.date_appel_manque
  GROUP BY
    date_appel,
    accountid,
    sfid),  time_schedule AS (
  SELECT
    accountid,
    DATE_TRUNC(startdatetime,day) AS jour_rdv,
    MIN(startdatetime) heure_rdv
  FROM
    salesforce.event se
    LEFT JOIN salesforce.user user
    ON se.ownerid =user.sfid
  WHERE
    isdeleted =FALSE
    AND LOWER(status__c) LIKE '%confirmed%' and type='Call' and subject like '%téléphonique%'
        AND user.UserRoleId IN ("00E1t000000DpBaEAK",
      "00E1t000000DpBVEA0")
  GROUP BY
    accountid,
    jour_rdv ),
  time_realised AS (
  SELECT
    opp.accountid,
    DATE_TRUNC(task.createddate,day) AS jour_appel,
    MIN(task.createddate) AS heure_premier_appel
  FROM
    salesforce.task task
  LEFT JOIN
    salesforce.opportunity opp
  ON
    task.whatid=opp.sfid
  LEFT JOIN salesforce.user user
    ON task.ownerid=user.sfid
     
  WHERE
    LOWER(subject) LIKE '%inbound%'
    OR LOWER(subject) LIKE '%outbound%' OR(LOWER(subject) LIKE '%appel%'
      AND LOWER(subject) LIKE '%démarr%')
    OR LOWER(subject) LIKE '%missed%'
    AND user.UserRoleId IN ("00E1t000000DpBaEAK",
      "00E1t000000DpBVEA0")
      GROUP BY accountid,jour_appel),
      retards_and_noshow as (SELECT
time_schedule.accountid,
    COUNTIF(time_realised.heure_premier_appel IS NULL) as no_show,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)<1) as on_time,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)<5) as moins_5_min_retard,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)<15) as entre_5_15_min_retard,
    COUNTIF(DATE_DIFF(time_realised.heure_premier_appel,time_schedule.heure_rdv,minute)>=15) as plus_15_min_retard,


FROM
  time_schedule LEFT JOIN time_realised
  ON time_schedule.accountid=time_realised.accountid 
  AND time_schedule.jour_rdv=time_realised.jour_appel
GROUP BY accountid
ORDER BY accountid)

SELECT
o.sfid,
o.accountid,
c.direction,
start_time,
c.incall_duration,
c.pretto_user_name,
u.name as expert_name,
c.manager_name,
u.UserRoleId,
nbm.Appel_manque,
retards.no_show,
retards.on_time,
retards.moins_5_min_retard,
retards.entre_5_15_min_retard,
retards.plus_15_min_retard,
IF (DATE(start_time) = DATE(vac.Date__c), "Vacances", "Work") as Vacances

FROM
`pretto-apis.metabase.ringover_calls` c
INNER JOIN
`pretto-apis.metabase.opportunities` l
ON
c.project_id=l.project_id
INNER JOIN
`pretto-apis.salesforce.opportunity` o
ON
l.opportunity_sfid=o.sfid
LEFT JOIN
salesforce.user AS u
ON
u.email=users_email 
LEFT JOIN
nb_appels_jour_appel_manque nbm
ON nbm.sfid = o.sfid
AND nbm.accountid = o.accountid
AND DATE(start_time) = nbm.date_appel
LEFT JOIN 
  salesforce.absence vac
ON
  vac.User__c = u.sfid
  and DATE(start_time) = DATE(vac.Date__c)
LEFT JOIN
retards_and_noshow as retards
ON retards.accountid = o.accountid

WHERE
o.sfid IS NOT NULL AND o.accountid IS NOT NULL
AND o.recordtypeid = '0121t000000E3iEAAS'

ORDER BY
o.sfid
        """
    call = client.query(query).to_dataframe()

    return call


def get_data_sf_test():
    query = """
SELECT
  o.sfid,
  CASE
    WHEN DATE(date_compromis) > CURRENT_DATE() OR date_compromis < "2017-01-01" THEN NULL
  ELSE
  date_compromis
  END AS date_compromis,
  date_condition_suspensive,
  l.maturity_at_onboarding,
  l.date_R1_realised AS date_r1__c,
  send_date__c,
  date_accorde__c,
  date_offre_edit__c,
  date_offre_sign__c,
  date_a_joindre__c,
  date_rech_no_doc__c,
  date_rech_docs__c,
  date_montage__c,
  date_fonds__c,
  date_perdue__c,
  lost_date,
  o.accountid,
  o.createddate,
  o.mandate_signed__c,
  o.stagename,
  l.canal,
  l.sub_canal,
  u.name,
  l.project_kind,
  lead_type,
  o.recordtypeid,
  rpa.cnt as nb_reassignement_account,
FROM
  metabase.opportunities AS l
INNER JOIN
  salesforce.opportunity AS o
ON
  o.sfid=l.opportunity_sfid
LEFT JOIN
  salesforce.user AS u
ON
  o.ownerid=u.sfid
LEFT JOIN
  salesforce.reassignment_per_account rpa
ON
  rpa.accountid = o.accountid
WHERE
  --(sc.name = "Pretto.fr" or sc.name = "Parrainage")
  o.mandate_signed__c IS NOT NULL
  AND o.recordtypeid = '0121t000000E3iEAAS'
  AND isClosed = false
  AND stagename not in ("Offre éditée", "Offre signée", "Accordé", "Perdue", "Fonds débloqués")
  AND u.name not in ("BuzDev BuzDev", "Expert Pretto")
  AND l.visio=false

  """

    Salesforce = client.query(query).to_dataframe()

    return Salesforce


def get_data_mandate_signed_test():
    query = """
SELECT
  sfid,
  accountid,
  mandate_signed__c as date_mandat,

FROM
salesforce.opportunity o

INNER JOIN 
  metabase.opportunities l
ON
  o.sfid=l.opportunity_sfid

WHERE 
mandate_signed__c is not null
AND date_fonds__c is null
AND isClosed = False
AND o.stagename not in ("Offre éditée", "Offre signée", "Accordé", "Perdue", "Fonds débloqués")
AND lead_owner__C = 'LEAD_OWNER_PRETTO'
    """

    Mandat = client.query(query).to_dataframe()

    return Mandat

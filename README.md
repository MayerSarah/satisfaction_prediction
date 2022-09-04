# satisfaction_prediction

### Structure du code

**pretto/apps/airflow/dags** 

- dag_satisfaction_prediction:
    
    *Dump les prédictions du jour dans la table salesforce.scoring_satisfaction (toutes les opportunités ouvertes avant qu’elles soient en “Offre éditée”)*
    

- dag_satisfaction_prediction_train_model:
    
    *Réentraine le modèle tous les jours*
    

- scoring_satisfaction_V2/sql_queries:
    
    *6 requêtes SQL pour récupérer la donnée BQ*
    

- scoring_satisfaction_V2/load_data:
    
    *Récupération des features des données brutes pour tous les avis
    Correction d’incohérences dans les données brutes*
    

- scoring_satisfaction_V2/preprocessing:
    
    *Feature engineering à partir des données brutes merge des features 
    Gestion des valeurs manquantes 
    Selection métier des features* 
    
- scoring_satisfaction_V2/train:
    
    *1. Récupérer les NPS et avis qui sont sur GCS 
    2. Data augmentation ( on rajoute les points 14 jours avant)
    3. Création de toutes les features qui correspondent à tous ces commentaires (get_data_from_BQ → load_data → preprocessing → compute_all_features)
    4. Création de la target (good/ not good) 
    5. Suppression des avis moyen
    6. Suppression des features trop corrélées* 
    

<aside>
A noter

- quand on a plusieurs notes pour une même opportunité : on garde la note de NPS la plus récente. Si on a encore des doublons : on garde simplement la dernière note postée

- scoring_satisfaction_V2/predict:
    
    *1. Récupérer tous les dossiers ouverts, mandat signé, avant Offre éditée
    2. Création de toutes les features sur ces dossiers 
    3. Date de commentaire est ici égale à la date actuelle (date à laquelle on vuet effectivement prédire la satisfaction)* 
    

### Liste des features retenues dans le modèle

| Features | Signification |
  
| nombre_appels_global | nombre total d’appels entre Pretto et client
| prct_in_global | proportion des appels entrants sur le total des appels
| nombre appels_buzdev | nombre d’appels buzdev/ client
| prct_in_buzdev | proportion d’appels entrants sur l’ensemble des appels buzdev
| prct_out_buzdev | proportion d’appels sortants sur l’ensemble des appels buzdev
| attente moyenne entre deux appels | datedernierappel - datepremierappel / nombre appels global
| Durée moyenne des appels | Durée moyenne des appels
| Nb_appels_in_sans_reponse | Nombre d’appels entrants sans réponse (incall_duration=0)
| Nb appel out sans reponse | Nombre d’appels sortants sans réponse
| Nb_Jours_Plusieurs_appels_global | Nombre de jours différents où il y a eu plusieurs appels client/EC
| Nb_Jours_Plusieurs_appels_7_derniers_jours | Nombre de jours différents où il y a eu plusieurs appels client/EC sur les 7 derniers jours
| Nb_Jours_Plusieurs_appels_14_derniers_jours | Nombre de jours différents où il y a eu plusieurs appels client/EC sur les 14 derniers jours
| Nb_Appel_Sup_10_min | Nombre de calls d’une durée supérieure à 10 minutes
| Is_Dernier_Appel_Out | Retourne 1 si le dernier appel était sortant/ 0 sinon
| Nb experts différents | nombre d'experts différents assignés au client
| Nb interlocuteurs différents | nombre d’interlocuteurs différents (buzdev/EC/..)
| Pas de rappel | Nombre d’appels manques sans rappel
| nombre messages repondeur | Nombre de messages répondeur laissés par l’EC à son client
| Duree_AppelsEntrants | Somme de la durée de tous les appels entrants
| Pourcentage appels manqués in | Nombre d’appels manqués in sans réponse/ nombre total d’appels entrants
| Pourcentage appels manqués out | Nombre d’appels manqués out sans réponse/ nombre total d’appels sortants
| Nb de fois sans appel depuis 14j par Pretto | compte le nombre fois où un client n'a pas reçu d'appel de Pretto pendant 14 jours consécutifs
| Experience_EC_jours | Différence en jours entre la date du premier appel client et la date du premier appel jamais effectué par l’expert
| Attente_depuis_dernier_appel | Date actuelle - date du dernier appel
| Attente depuis premier appel | Date actuelle - date premier appel
| prt_appel_manque_sur_appels | nombre de jours avec appel manque sur nombre de jours avec appel
| ppt_appelmanque_sans_rappel | nombre de jours appels manques sans rappels / nb de jours appels
| duree_in_sur_duree_total | durée des appels entrants / somme totale de la durée des appels
| delta_date_comm_date_mandat | nombre de jours écoulés depuis la signature du mandat
| date_montage__c-date_r1__c | temps écoulé en jours entre le R1 et le montage
| dt_commentaire_suspensive | temps qu’il reste entre la date du jour (date de commentaire dans le train) et la date de fin des conditions suspensives
| dt_creation_envoi | temps écoulé en jours entre la création du dossier et l’envoi
| no_show | nombre de no show de l’EC avec cette opportunité
| on_time | nombre de fois où l’EC a été à l’heure à son appel (pas plus de 5minutes de retard)
| plus_15_min_retard | nombre de fois où l’EC a eu plus de 15 minutes de retard à son rdv avec ce client
| nb_appel_sup_3_min | nombre d’appels (entrants ou sortants) ayant une durée > 180sec

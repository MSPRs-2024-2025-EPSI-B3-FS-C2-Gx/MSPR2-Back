1. **Table "global_total_cumulative_cases"**  
   - **Contenu** : Une seule ligne contenant une colonne nommée par exemple **global_total_cumulative_cases**.  
   - **Valeur** : Le total mondial des cas cumulés, obtenu en additionnant pour chaque pays la dernière valeur disponible de **Cumulative_cases**.  


2. **Table "global_statistics"**  
   - **Contenu** : Une ligne par pays avec les indicateurs suivants :  
     - **Country** : Le nom du pays  
     - **total_cumulative_people_vaccinated** : La moyenne des valeurs cumulatives rapportées du nombre de personnes ayant reçu au moins une dose. Comme il s'agit de données cumulatives, cette "moyenne" reflète le niveau moyen des valeurs enregistrées sur l'ensemble de la période, et non le nombre de personnes vaccinées par jour.  
     - **total_cumulative_people_fully_vaccinated** : La moyenne des valeurs cumulatives rapportées du nombre de personnes considérées comme entièrement vaccinées, calculée sur toute la période des enregistrements.  
     - **avg_new_cases** : La moyenne des nouveaux cas rapportés par jour pour le pays, calculée sur la période totale couverte par les données.  
     - **avg_new_deaths** : La moyenne des nouveaux décès rapportés par jour pour le pays, calculée sur la période totale des enregistrements.
     - **total_cumulative_cases** : La somme des cas cumulés rapportés par pays, obtenue en additionnant pour chaque pays la dernière valeur disponible de **Cumulative_cases**.


3. **Table "covid_global_yearly_summary"**
   - **Contenu** : Cette table regroupe et agrège les données COVID-19 au niveau global par année.
     - **Year** : L'année d'enregistrement (extraite de la colonne Date_reported). 
     - **total_new_cases** : La somme des nouveaux cas rapportés durant l'année.
     - **total_new_deaths** : La somme des nouveaux décès rapportés durant l'année.
     - **total_cumulative_cases** :  La somme des cas cumulés (selon les enregistrements disponibles) durant l'année.
     - **total_cumulative_deaths** : La somme des décès cumulés durant l'année.
     - **CFR**: Le taux de létalité (Case Fatality Rate), calculé en pourcentage comme suit : \text{CFR} = \left(\frac{\text{total_cumulative_deaths}}{\text{total_cumulative_cases}}\right) \times 100
    - **Year_ts**: L'annee en timestamp


4. **Table "covid_region_yearly_summary"**
   - **Contenu** : Cette table présente des agrégations des données COVID-19 par région de l'OMS (WHO_region) et par année.
     - **WHO_region** : La région de l'Organisation mondiale de la santé à laquelle appartiennent les enregistrements.
     - **Year** : L'année d'enregistrement.
     - **total_new_cases** : La somme des nouveaux cas signalés durant l'année pour la région.
     - **total_new_deaths** :  La somme des nouveaux décès enregistrés durant l'année pour la région.
    - **Year_ts**: L'annee en timestamp
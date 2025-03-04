1. **Table "global_total_cumulative_cases"**  
   - **Contenu** : Une seule ligne contenant une colonne nommée par exemple **global_total_cumulative_cases**.  
   - **Valeur** : Le total mondial des cas cumulés, obtenu en additionnant pour chaque pays la dernière valeur disponible de **Cumulative_cases**.  
   - **Exemple** :

2. **Table "global_statistics"**  
   - **Contenu** : Une ligne par pays avec les indicateurs suivants :  
     - **Country** : Le nom du pays  
     - **total_cumulative_people_vaccinated** : La moyenne des valeurs cumulatives rapportées du nombre de personnes ayant reçu au moins une dose. Comme il s'agit de données cumulatives, cette "moyenne" reflète le niveau moyen des valeurs enregistrées sur l'ensemble de la période, et non le nombre de personnes vaccinées par jour.  
     - **total_cumulative_people_fully_vaccinated** : La moyenne des valeurs cumulatives rapportées du nombre de personnes considérées comme entièrement vaccinées, calculée sur toute la période des enregistrements.  
     - **avg_new_cases** : La moyenne des nouveaux cas rapportés par jour pour le pays, calculée sur la période totale couverte par les données.  
     - **avg_new_deaths** : La moyenne des nouveaux décès rapportés par jour pour le pays, calculée sur la période totale des enregistrements.
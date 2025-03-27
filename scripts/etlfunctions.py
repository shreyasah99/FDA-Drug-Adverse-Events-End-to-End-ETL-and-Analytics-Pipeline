import requests 
import pandas as pd
import json
import time 
import logging


def extract_data():
    base_url = 'https://api.fda.gov/drug/event.json'
    search_query = 'receivedate:[20200101 TO 20231231] AND occurcountry:"US" AND _missing_:companynumb'
    limit = 1000
    total_records=79250  # Maximum allowed by API

    all_results = []

    # Retry configuration for HTTP requests
    

    url = f'{base_url}?search={search_query}&limit={limit}'

    
    while len(all_results) < total_records:
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            if 'results' in data:
                results = data['results']
                all_results.extend(results)
            else:
                break

            if 'Link' in response.headers:
                link_header = response.headers['Link']
                next_url = None

                for link in link_header.split(','):
                    if 'rel="next"' in link:
                        next_url = link.split(';')[0].strip('<>')
                        break

                if next_url:
                    url = next_url
                else:
                    break
            else:
                break

            if len(all_results) >= total_records:
                break

        # Ensure we trim the results to exactly `total_records`
            all_results = all_results[:total_records]

        # Convert results to DataFrame
    df = pd.DataFrame(all_results)
    return df


def transform_data(df):

#Report, Country, received date data
    
    ## extracting required columns
    df_date_country = df.loc[:,['safetyreportversion', 'safetyreportid','occurcountry','receiptdate', 'serious','seriousnessother','seriousnessdeath']].copy()

    ## replacing values inside cells with the data provided in the website
    df_date_country['serious'] = df_date_country['serious'].astype(str).str.strip().replace({
        "1": "The adverse event resulted in a life threatening condition",
        "2": "The adverse event did not result in any serious condition"})
    df_date_country['seriousnessother'] = df_date_country['seriousnessother'].astype(str).str.strip().replace({"1.0": "YES",
                                                                                                              "2.0" : "NO"})
    df_date_country['seriousnessdeath'] = df_date_country['seriousnessdeath'].astype(str).str.strip().replace({"1.0": "YES",
                                                                                                              "2.0": "NO"})

    ## renaming columns
    df_date_country.rename(columns = {'occurcountry':'reportcountry',
                                        'receiptdate': 'reportdate',
                                        'serious':'eventoutcome',
                                        }, inplace= True)

    ## final country, date data
    reports = df_date_country
    reports.fillna(0, inplace =True)

    reports['safetyreportversion'] = reports['safetyreportversion'].astype(int)
    reports['safetyreportid'] = reports['safetyreportid'].astype(int)
    reports['reportcountry'] = reports['reportcountry'].astype(str)
    reports['reportdate'] = pd.to_datetime(reports['reportdate'], format='%Y%m%d')
    reports['eventoutcome'] = reports['eventoutcome'].astype(str)
    reports['seriousnessother'] = reports['seriousnessother'].astype(str).str.strip().replace("nan", "NOT MENTIONED")
    reports['seriousnessdeath'] = reports['seriousnessdeath'].astype(str).str.strip().replace("nan","NOT MENTIONED")



#Patient Data

    ## cretaing unique columns
    df_unique= df.loc[:,('safetyreportversion','safetyreportid')]##cretaing unique columns
    df_unique.rename(columns={"safetyreportversion":'version_id', "safetyreportid":'id'}, inplace = True)

    ##normalizing patient data
    df_patient_sample = df['patient']
    df_patient = pd.json_normalize(df_patient_sample.apply(eval)) ##copy1
    print(df_patient.columns)
    logging.info(f"Initial DataFrame columns: {df_patient.columns}")
    df_patient_1 =pd.concat([df_unique,df_patient], axis=1)
    logging.info(f"Initial DataFrame columns: {df_patient_1.columns}")
    print(df_patient_1.columns)

    ## extracting required columns
    df_final_patient = df_patient_1.loc[:,['id','version_id','patientonsetage', 'patientweight', 'patientsex']]
    ##final patient data
    
    ## renaming columns
    df_final_patient.rename(columns={'id': 'patientid','patientonsetage': 'patientage'}, inplace = True)

    ## final data
    patients = df_final_patient
    patients.fillna(0, inplace = True)

    patients['patientid'] = patients['patientid'].astype(int)
    patients['version_id'] = patients['version_id'].astype(int)
    patients['patientage'] = patients['patientage'].replace(0,"NOT MENTIONED" )
    patients['patientweight'] = patients['patientweight'].astype(float).round().astype(int)
    patients['patientweight'] = patients['patientweight'].replace(0, "NOT MENTIONED")
    patients['patientsex'] = patients['patientsex'].astype(str).str.strip().replace({"1": "Male",
                                                                         "2": "Female"
                                                                        })

#Patient reaction-to-drug data
    
    ## normlaizing  data related to patient's reaction to drug 
    df_reaction = df_patient_1.explode('reaction')
    df_reaction.reset_index(drop=True, inplace =True)
    final_reaction = pd.json_normalize(df_reaction['reaction'])
    final_reaction = pd.concat([final_reaction,df_reaction['id']], axis = 1) 

    ## extracting required columns
    final_reaction = final_reaction[['id','reactionmeddrapt','reactionoutcome']]

    ## renaming columns 
    final_reaction.rename(columns = {'id':'symptomid',
                                    'reactionmeddrapt': 'symptomname',
                                    'reactionoutcome': 'symptomoutcome' }, inplace = True)

    ## final patient reaction-to-drug data
    symptoms = final_reaction
    symptoms.fillna(0, inplace=True)
    
    symptoms['symptomid'] = symptoms['symptomid'].astype(int)
    symptoms['symptomname'] = symptoms['symptomname'].astype(str)
    symptoms['symptomoutcome'] = symptoms['symptomoutcome'].astype(str).str.strip().replace({"1" : "Recovered",
                                                                                 "2" : "Recovering",
                                                                                 "3" : "Not Recovered",
                                                                                 "4" : "Recovered with consequent health issues",
                                                                                 "5" : "Fatal",
                                                                                 "6" : "UNKNOWN",
                                                                                 "0": "UNKNOWN"})
    
#Patient reaction-to-drug data

    ## extracting required columns and normalizing drug related data
    df_sample = df_patient_1[['id','version_id','drug']]
    df_drug = df_sample.explode('drug')
    df_drug.reset_index(drop = True, inplace = True)
    df_drug_final = pd.json_normalize(df_drug['drug'])
    df_drug_final['id'] = df_drug['id'] 

    ## filtering only the drug which are considered by the reporter to be the cause
    df_drug_final= df_drug_final[df_drug_final['drugcharacterization'] == "1"][['id', 'drugcharacterization','openfda.product_type','medicinalproduct','drugindication', 'activesubstance.activesubstancename']]

    ## renaming columns
    df_drug_final.rename(columns = {'id':'drugid',
                                    'openfda.product_type':'drugtype',
                                    'drugindication':'drugactualpurpose',
                                    'medicinalproduct':'drugtradename',
                                    'activesubstance.activesubstancename':'drugactivechemical'}, inplace = True)

    ## final drug data
    drugs = df_drug_final
    drugs.fillna(0, inplace = True)


    drugs['drugid'] = drugs['drugid'].astype(int)
    drugs['drugcharacterization'] = drugs['drugcharacterization'].astype(int)
    drugs['drugtype'] = drugs['drugtype'].astype(str)
    drugs['drugtype'] = drugs['drugtype'].str.replace("['", '').str.replace("']", '')
    drugs['drugtype'] = drugs['drugtype'].str.replace("0", "NOT MENTIONED")
    drugs['drugtradename'] = drugs['drugtradename'].astype(str).str.strip().replace("0", "NOT MENTIONED")
    drugs['drugactualpurpose'] = drugs['drugactualpurpose'].astype(str).str.strip().replace("0", "NOT MENTIONED")
    drugs['drugactivechemical'] = drugs['drugactivechemical'].astype(str).str.strip().replace("0", "NOT MENTIONED")

    return reports, patients, symptoms, drugs





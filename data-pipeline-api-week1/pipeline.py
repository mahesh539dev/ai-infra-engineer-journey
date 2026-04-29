import pandas as pd
import numpy as np
import json

csv_data = pd.read_csv('Titanic-Dataset.csv')

def generate_clean_data():

    age_fill = csv_data["Age"].median()

    csv_data.fillna({'Age':age_fill}, inplace=True)

    csv_data.dropna(subset=['Embarked'], inplace=True)

    csv_data.drop(columns=['Cabin'], inplace=True)
    
    survival_df = csv_data[csv_data['Survived']==1]
    non_survival_df = csv_data[csv_data['Survived']==0]
    
    survival_rate = len(survival_df)/len(csv_data) * 100
    
    avg_survival_age = np.mean(survival_df['Age'])
    avg_nonsurvival_age = np.mean(non_survival_df['Age'])
    survival_count_by_gender = survival_df.groupby('Sex').size()
    survival_count_by_passenger_class = survival_df.groupby('Pclass').size()

    data_to_save = {
        "survival_rate_percentage": round(survival_rate, 2),
        "total_passengers": len(csv_data),
        "avg_survival_age": round(avg_survival_age, 2),
        "avg_nonsurvival_age": round(avg_nonsurvival_age, 2),
        "survival_count_by_gender": {str(k): int(v) for k, v in survival_count_by_gender.items()},
        "survival_count_by_passenger_class": {str(k): int(v) for k, v in survival_count_by_passenger_class.items()}
    }

    with open('stats.json', 'w') as f:
        json.dump(data_to_save, f, indent=4)

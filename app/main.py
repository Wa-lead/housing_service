import mysql.connector
from flask import Flask, request
import numpy as np
import pandas as pd
import sklearn.metrics

apartment_columns = ['id',
 'coords',
 'size',
 'price',
 'bedrooms',
 'bathrooms',
 'availability',
 'capacity',
 'distance_from_central']

cdt_columns = ['name', 'id', 'degree', 'field_of_study', 'university', 'graduation_year', 'courses',
           'health_history', 'intersts', 'car', 'house', 'job', 'city', 'country', 'date_of_birth', 
           'family_members', 'company_coords']

app = Flask(__name__)


@app.route('/', methods=['GET'])
def return_data():
    id = request.args.get('id')
    cnx = mysql.connector.connect(
                host="rm-l4vtsmuu203976dh4qo.mysql.me-central-1.rds.aliyuncs.com",
                user="admin_account",
                password="Admin@2023",
                database="mysql"
        )
    cursor = cnx.cursor()

    query = f"SELECT * FROM new_schema.apartment_data"
    cursor.execute(query)
    apartments = cursor.fetchall()
    apartments = dict(zip(apartment_columns, np.array(apartments).T))
    apartments = pd.DataFrame(data=apartments)
    apartments = apartments[apartments['availability'] == 'available']

    query = f"SELECT * FROM new_schema.data WHERE id = \"{id}\""
    cursor.execute(query)
    cdt = cursor.fetchall()
    cdt = dict(zip(cdt_columns, np.array(cdt).T))
    cdt = pd.DataFrame(data=cdt).iloc[0]
    
    # split the coords into two
    cdt['latitute'] = float(cdt['company_coords'].split(',')[0].replace('[', ''))
    cdt['longitude'] = float(cdt['company_coords'].split(',')[1].replace(']', ''))
    cdt.drop('company_coords', inplace=True)

    apartments['latitute'] = apartments['coords'].apply(lambda x: x.split(',')[0].replace('[', '')).astype(float)
    apartments['longitude'] = apartments['coords'].apply(lambda x: x.split(',')[1].replace(']', '')).astype(float)
    apartments.drop(columns='coords', inplace=True)
    
    apartments = apartments[apartments['capacity'] >= cdt['family_members']]
    distances = apartments.apply(lambda x: sklearn.metrics.pairwise.euclidean_distances([[x['latitute'], x['longitude'], x['capacity']]], [[cdt['latitute'], cdt['longitude'], cdt['family_members']]])[0][0], axis=1)
    # select the apartment with the minimum distance
    min_distance = np.argsort(distances)[::-1][:5]
    apartments = apartments.iloc[min_distance]
    
    return apartments.to_dict(orient='records')


if __name__ == '__main__':
        app.run(host='127.0.0.1', port=80, debug=True)
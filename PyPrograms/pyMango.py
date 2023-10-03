import pandas as pd
from pymongo import MongoClient

myclient = MongoClient('mongodb://%s:%s@localhost:27017/admin' % ('root', 'root'))    ## login to mongodb with admin user
patientdb = myclient['Patient']
medical_col = patientdb['medical_data']
data = pd.read_csv('../Data/heart.csv')
for  i in data.index :
 	X= data.iloc [i,: ]
 	medical_col.insert_one(X.to_dict()) 


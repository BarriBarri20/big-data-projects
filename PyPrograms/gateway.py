
from kafka import KafkaProducer
import json
import datetime
import pandas as pd
from time import sleep



def data_to_kafka(nb_patient=10,n=2): 
                                                       

        #create kafka producer
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092',
                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
  
   
    data= pd.read_csv('../Data/heart.csv').iloc[:nb_patient,:]
        
    for  i in data.index :
        X= data.iloc[[i],:]
        
        if 'target' in X.columns:              
            type_record= X.loc[i,'target']
            X= X.drop('target',axis=1)
                                ## type_record is a variable that identify the label of each record read.
        
        
        if type_record==1:
            
                topic_name = "urgent_data"  
        else:                              ## The kafka Topic is specified based on the type_record value(1 or 0)

                topic_name= "normal_data"
            
        
        doc= X.drop(['sex','age'],axis=1).to_dict('records')[0]
        doc['date']=str(datetime.datetime.now().date())
        doc['time']=str(datetime.datetime.now().time())[:5]
        doc['Patient_ID'] = i+1
        producer.send(topic_name,doc)
        print("this data is urgent !!!" if type_record==1 else "this data is normal")
        print(doc)
        sleep(n)
    producer.flush()

data_to_kafka(nb_patient=6) 
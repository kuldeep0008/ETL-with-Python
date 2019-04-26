import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
#import numpy as np
#-----------------ETL part-----------------------------------------
class ConnectDatabase():
    def input_database(self, user, password, host, database):
        
            source_conx=mysql.connector.connect(user=user, password=password, host=host, database=database)
            return source_conx
        
        
    def query_input(self, query1, query2, sourc_conn):
            #self.query1= "select * from products_expenditure"
            productExp=pd.read_sql(query1,source_conx)
            #self.query2='select * from products_sale'
        
            productSale=pd.read_sql(query2, source_conx)
            
            return productExp, productSale
            
    def merge_db(self, db_one, db_two):
        
            finaldf=pd.merge(db_one, db_two, on='product_id', how='inner')
            return finaldf
        
class update_val():
    
        def rate(val):
            if int(val) >6 :
                return 'good'
            elif int(val) <=8 and int(val) >=5:
                return 'average'
            elif int(val)< 5: 
                return 'bad'

#finaldf.groupby(['product_Rating','product_id']).sum()

#--------------------VISUALIZATION OF DATA----------------------------------

class data_visulaisation():
    def count_plot(self,x_bar,dataFrame):
        sns.countplot(x=x_bar,data = dataFrame)
        
    def bar_plot(self, dataframe, colname1, colname2, label):
        Y=dataframe[colname1].values
        X=dataframe[colname2].values
        plt.bar(X, height= Y,label=label, width= 0.8, color= 'green')
        for a in range(len(X)):
            plt.annotate(xy= [X[a],Y[a]+500], s=str(Y[a]), color= 'red')
        plt.ylim(top=30000)
        
#VISUALIZATION OF DATA


#finaldf.to_csv(r'C:\Users\kpanwar\Desktop\productRating.csv')
#x=finaldf['product_Rating']
#y=finaldf['SalesRating']

#____________________________________Calling Functions_________________________

conn_db=ConnectDatabase()
source_conx = conn_db.input_database(user='root', password='kuldeep', host='localhost',database='foods_industry')
two_tables=conn_db.query_input('select * from products_expenditure', 'select * from Products_sale',sourc_conn=source_conx)

#Merging two Tables
final_df=conn_db.merge_db(two_tables[0], two_tables[1])
final_df['product_Rating']=final_df['Sales_Rating'].apply(update_val.rate)


#---Data visulization---
    #CountPlot
data_v=data_visulaisation()
data_v.count_plot('product_Rating', final_df)

    #Barplot
data_v.bar_plot(dataframe=final_df, colname1='Expenditure', colname2='product_name', label='Expenditure')














print()
#sns.countplot(x='product_name',hue= 'Sales_Rating', data= finaldf)
#plt.bar(x,height=[np.arange(11)],label='bar1', color= 'red')
#plt.xlabel('x')
#plt.ylabel('y')
#plt.show()
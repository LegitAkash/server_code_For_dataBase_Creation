import pandas as pd
import numpy as np
import mysql.connector 
import pymysql
from sqlalchemy import create_engine
# loading the File to the DataFrame
df=pd.read_csv("FR-All StockItems 20230203_2{CSV}.csv",encoding= 'unicode_escape',low_memory=False)

# fetching the Table to a DataFrame from that DataBase
try:
    mydb= mysql.connector.connect(host = "localhost",user = "root",password = "",database = "frontier_2_site")
    query = "Select * from items;"
    result_dataFrame = pd.read_sql(query,mydb)
    mydb.close() #close the connection
except Exception as e:
    mydb.close()
    print(str(e))
temp_df=pd.merge(result_dataFrame,df,left_on=["InventoryID","cross_ref_inventory_id"],right_on=["Inventory ID","Foc100_CrossRef"])
#Change of the Values in th DataFrame
temp_df["height"]=temp_df["Item Height (CM)"]
temp_df["width"]=temp_df["Item Width (CM)"]
temp_df["length	"]=temp_df["Item Length (CM)"]
temp_df["DimensionWeight"]=temp_df["Item Dimensional Weight (Domestic)"]
temp_df["DefaultPrice"]=temp_df["Default Price (DefaultPrice)"]
temp_df["MSRP_x"]=temp_df["MSRP_y"]
temp_df["web_display_id"]=temp_df["WebDisplayID"]
temp_df["clearance_Item"]=temp_df["Clearance Item"]
temp_df["has_360_image"]=temp_df["Has 360 Image"]
temp_df["has_image"]=temp_df["Has Image"]
temp_df["item_rank"]=temp_df["Item Rank"]
temp_df["tax_category_id"]=temp_df["Tax Category"]
#renaming the Columns Name
temp_df.rename(columns={"MSRP_x":"MSRP"},inplace=True)
temp_df.rename(columns={"DB_ID_x":"DB_ID"},inplace=True)
changed_df=temp_df[result_dataFrame.columns.tolist()]
unchanged_df=result_dataFrame[~result_dataFrame["id"].isin(changed_df["id"].to_list())]
final_df=pd.concat([changed_df,unchanged_df])


mydb = mydb = mysql.connector.connect(host = "localhost",user = "root",password = "",database = "frontier_2_site")
cursor = mydb.cursor()
# CREATE TABLE new_table_name LIKE old_table_name;
                                    
cursor.execute("CREATE OR REPLACE TABLE items_temp8 LIKE items;")
cursor.execute("TRUNCATE TABLE items_temp8;")
mydb.close()

cnx = create_engine('mysql+pymysql://root:@localhost/frontier_2_site')  
final_df.to_sql('items_temp8', con=cnx, if_exists='replace')
import pandas as pd
import numpy as np
import mysql.connector 
import pymysql
from sqlalchemy import create_engine
df1= pd.read_excel('FG_Stock_Item_20230303.xls')
df1.drop(columns=["Selected","Item Weight"],inplace=True)

try:
    mydb = mysql.connector.connect(host = "localhost",user = "root",password = "",database = "testing_1")
    query = "Select * from items;"
    result_dataFrame = pd.read_sql(query,mydb)
    mydb.close() #close the connection
except Exception as e:
    mydb.close()
    print(str(e))
changed_df=pd.merge(result_dataFrame,df1,left_on=["cross_ref_inventory_id","InventoryID"],right_on=["Inventory ID","Frontier ID"])
changed_df["focus_db_id"]=changed_df["DB ID"]
changed_df["focus_default_price"]=changed_df["Default Price"]
changed_df["focus_msrp"]=changed_df["MSRP_y"]
changed_df.rename(columns={"MSRP_x":"MSRP"},inplace=True)
temp_1=changed_df[['id', 'item_slug', 'acumatica_id', 'brand', 'brand_id', 'height',
       'width', 'length', 'dimensional_weight', 'item_location', 'AverageCost',
       'BaseUOM', 'COGSAccount', 'CurrentStdCost', 'DefaultPrice',
       'DimensionVolume', 'DimensionWeight', 'ImageUrl', 'InventoryAccount',
       'InventoryID', 'ItemClass', 'ItemType', 'LandedCostVarianceAccount',
       'LastCost', 'LastStdCost', 'LotSerialClass', 'Markup', 'MaxCost',
       'MinCost', 'MinMarkup', 'MSRP', 'PackagingOption', 'PackSeparately',
       'PendingStdCost', 'POAccrualAccount', 'PriceClass',
       'PurchasePriceVarianceAccount', 'TaxCategory', 'is_variable_product',
       'DB_ID', 'created_at', 'updated_at', 'cross_ref_inventory_id',
       'pack_weight', 'weight_uom', 'pack_volume', 'volume_uom',
       'is_cross_ref_inventory_synced', 'ITEMNONRTN', 'is_returnable',
       'item_status', 'attribute_value_id', 'item_rank', 'has_image',
       'has_360_image', 'inventory_stock', 'item_dentsp', 'hca_status',
       'schein_code', 'patterson_code', 'excluded_item', 'clearance_Item',
       'is_special_price', 'manufacturer_code', 'animation_type',
       'alternate_animation_type', 'parent_code', 'web_display_id', 'dc_id',
       'is_request_del', 'del_request_date', 'tax_category_id', 'REQDENLICN',
       'focus_db_id', 'focus_default_price', 'focus_msrp']]
unchaged_df=result_dataFrame[~result_dataFrame["id"].isin(changed_df["id"].to_list())]
final_df=pd.concat([temp_1,unchaged_df])

mydb = mydb = mysql.connector.connect(host = "localhost",user = "root",password = "",database = "testing_1")
cursor = mydb.cursor()
                                    
cursor.execute("CREATE OR REPLACE TABLE items_temp6 LIKE items;")
cursor.execute("TRUNCATE TABLE items_temp6 ;")
mydb.close()

cnx = create_engine('mysql+pymysql://root:@localhost/testing_1')  
final_df.to_sql('items_temp6', con=cnx, if_exists='replace')
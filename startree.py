import pandas as pd
from treelib import Node,Tree
from pprint import pprint
from collections import namedtuple  
import warnings
warnings.filterwarnings("ignore")
df = pd.read_csv('./ecommerce.csv')

split_value = 1000

agg_dict = {}

# true_dimensions = ['Country', 'Browser', 'Locale']
# aggregate = ['Impressions']
# aggregations = ['sum', 'min', 'max', 'mean']

true_dimensions = ['StockCode', 'InvoiceDate', 'Country']
aggregate = ['StockCode', 'CustomerID', 'Quantity', 'Country']
aggregations = ['count']

agg_dict = {'StockCode':['count'], 'Quantity':['sum'], 'Country':pd.Series.nunique, 'UnitPrice':['sum']}

# true_dimensions = ['xyz_campaign_id', 'fb_campaign_id', 'age', 'gender', 'interest']
# aggregate = ['Impressions', 'Clicks', 'Spent', 'Total_Conversion', 'Approved_Conversion']
# aggregations = ['sum', 'min', 'max', 'mean']

temp_dict = {}
for d in true_dimensions : 
    unique_counts = len(df[d].unique())
    temp_dict[d] = unique_counts
true_dimensions = sorted(temp_dict, key=temp_dict.get)
# for agg in aggregate:
#     agg_dict[agg] = aggregations
# print(agg_dict)
print(true_dimensions)

global star_tree

def index(dimensions, df) : 
    global star_tree
    for d in dimensions:
        unique_values = df[d].value_counts().to_dict()
        for k,v in unique_values.items() : 
            if(v>split_value) :
                temp_df = df[df[d] == k]
                temp_df2 = temp_df.copy()
                filtered_d = list(filter(lambda x: x!= d, dimensions))
                for dimension in filtered_d : 
                    temp_df2[dimension] = '*'
                index(filtered_d, df[df[d]==k])
                temp_df2 = temp_df2.groupby(true_dimensions).agg(agg_dict).reset_index()
                temp_df2['row_count'] = v
                temp_df2['df'] = temp_df.to_json(orient="records")
                star_tree = star_tree.append(temp_df2, ignore_index=True)
            else:
                continue

star_tree = pd.DataFrame([])
index(true_dimensions, df)
star_tree.columns = ["_".join(x) for x in star_tree.columns.ravel()]
star_tree = star_tree.drop_duplicates(subset=[x+"_" for x in true_dimensions], keep='last')
print("Finished building index!")
print(star_tree)
star_tree.to_csv('./star_tree.csv')
print("Finished writing to csv")


#TODO : Add more star nodes based on frequency of data being queried
#TODO : Range query support for continous values
#TODO : Maintain separate cardinality table
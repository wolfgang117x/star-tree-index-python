import pandas as pd
from treelib import Node,Tree
from pprint import pprint
from collections import namedtuple  
import warnings
warnings.filterwarnings("ignore")
df = pd.read_csv('./data.csv')

split_value = 50

agg_dict = {}

true_dimensions = ['Country', 'Browser', 'Locale']
aggregate = ['Impressions']
aggregations = ['sum', 'min', 'max', 'mean']

# true_dimensions = ['xyz_campaign_id', 'fb_campaign_id', 'age', 'gender', 'interest']
# aggregate = ['Impressions', 'Clicks', 'Spent', 'Total_Conversion', 'Approved_Conversion']
# aggregations = ['sum', 'min', 'max', 'mean']

temp_dict = {}
for d in true_dimensions : 
    unique_counts = len(df[d].unique())
    temp_dict[d] = unique_counts
true_dimensions = sorted(temp_dict, key=temp_dict.get)
for agg in aggregate:
    agg_dict[agg] = aggregations


global star_tree

def index(dimensions, df) : 
    global star_tree
    for d in dimensions:
        unique_values = df[d].value_counts().to_dict()
        # print("UNIQUE VALUES  : ", unique_values)
        for k,v in unique_values.items() : 
            if(v>split_value) :
                temp_df = df[df[d] == k]
                temp_df2 = temp_df.copy()
                # print("TDF : ",temp_df)
                filtered_d = list(filter(lambda x: x!= d and x not in aggregate, dimensions))
                for dimension in filtered_d : 
                    # print("DIMENSION : ",dimension)
                    temp_df2[dimension] = '*'
                index(filtered_d, df[df[d]==k])
                # temp_df = temp_df.reset_index()
                # print(temp_df)
                # temp_df = temp_df.groupby(true_dimensions).agg(imp_sum=('Impressions', 'sum'), imp_min = ('Impressions', 'min'), imp_max = ('Impressions', 'max'), imp_avg = ('Impressions', 'mean')).reset_index()
                temp_df2 = temp_df2.groupby(true_dimensions).agg(agg_dict).reset_index()
                temp_df2['value_count'] = v
                temp_df2['df'] = temp_df.to_json(orient="records")
                # print("TDF POST : ",temp_df)
                # print(type(temp_df))
                star_tree = star_tree.append(temp_df2, ignore_index=True)

star_tree = pd.DataFrame([])
index(true_dimensions, df)
star_tree = star_tree.drop_duplicates()
star_tree.to_csv('./star_tree.csv')
# print(df)
# print(star_list)
pprint(star_tree)

# def generate_tree(df, dimensions, star_dict) : 
#     if(df.shape[0]>split_value) : 
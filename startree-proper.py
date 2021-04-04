import pandas as pd
from anytree import Node, RenderTree
from pprint import pprint
from collections import namedtuple  
import warnings

warnings.filterwarnings("ignore")

# df = pd.read_csv('./data.csv')
df = pd.read_csv('./ecommerce.csv')

split_value = 1000

# dimensions = ['Country', 'Browser', 'Locale', 'Impressions']
# aggregations = {'Country':pd.Series.nunique, 'Browser':pd.Series.nunique, 'Locale':pd.Series.nunique, 'Impressions' : 'sum'}

dimensions = ['Country', 'StockCode', 'InvoiceDate']
aggregations = {'StockCode':['count'], 'Quantity':['sum'], 'Country':pd.Series.nunique, 'UnitPrice':['sum']}

def star_node(name, record_count, records, aggregations, parent=None, children=None) :
    return Node(name, record_count = record_count, records = records, aggregations = aggregations, parent = parent)

root_node = star_node('root', len(df.index), df, df.groupby(dimensions).agg(aggregations))

def drop_return(df, index):
    row = df.loc[index]
    df.drop(index, inplace=True)
    print(df)
    return row

def recursive(root_node, i) :
    if(root_node.record_count > split_value and i<len(dimensions)) : 
        unique_values = root_node.records[dimensions[i]].value_counts().to_dict()
        # print(unique_values)
        for k,v in unique_values.items() : 
            if(v > split_value) : 
                index =root_node.records[root_node.records[dimensions[i]]==k].index
                sub_records = root_node.records.loc[index]
                # new_root_node_records = root_node.records.drop(index)
                # root_node = star_node(root_node.name, len(new_root_node_records.index), new_root_node_records, new_root_node_records.groupby(dimensions).agg(aggregations))
                new_node = star_node(dimensions[i] + "_" + str(k),  len(sub_records.index), sub_records, sub_records.groupby(dimensions[i]).agg(aggregations), parent=root_node)
                recursive(new_node, i+1)

recursive(root_node, 0)

for pre, fill, node in RenderTree(root_node):
    treestr = u"%s%s" % (pre, node.name)
    print(treestr, "Records:", node.record_count)

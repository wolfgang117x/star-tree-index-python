import pandas as pd
from anytree import Node, RenderTree
from pprint import pprint
from collections import namedtuple  
import warnings

warnings.filterwarnings("ignore")

# df = pd.read_csv('./data.csv')
df = pd.read_csv('./ecommerce.csv')

t_total = df.shape[0]

split_value = 1000

# dimensions = ['Country', 'Browser', 'Locale']
# aggregations = {'Country':pd.Series.nunique, 'Browser':pd.Series.nunique, 'Locale':pd.Series.nunique, 'Impressions' : 'sum'}

dimensions = ['Country', 'StockCode', 'InvoiceDate']
aggregations = {'StockCode':['count'], 'Quantity':['sum'], 'Country':pd.Series.nunique, 'UnitPrice':['sum']}

def star_node(name, record_count, records, aggregations, parent=None, children=None) :
    return Node(name, record_count = record_count, records = records, aggregations = aggregations, parent = parent)

root = star_node('root', df.shape[0], df, df.groupby(dimensions).agg(aggregations))

def drop_return(df, index):
    row = df.loc[index]
    df.drop(index, inplace=True)
    print(df)
    return row

def recursive(root_node, i) :
    if(root_node.record_count > split_value and i<len(dimensions)) : 
        unique_values = root_node.records[dimensions[i]].value_counts().to_dict()
        for k,v in unique_values.items() : 
            if(v > split_value) : 
                index = root_node.records[root_node.records[dimensions[i]]==k].index
                sub_records = root_node.records.loc[index]
                new_node = star_node(dimensions[i] + "_" + str(k),  sub_records.shape[0], sub_records, sub_records.groupby(dimensions[i]).agg(aggregations), parent=root_node)
                print('Created New Node : ', root_node.name, new_node.name, new_node.record_count, "Dimension : ", dimensions[i])
                recursive(new_node, i+1)
                temp_record_count = root_node.records.shape[0]
                root_node.records = root_node.records.drop(index)
                root_node.record_count = root_node.records.shape[0]
        if(i+1<len(dimensions)):
            recursive(root_node, i+1)

recursive(root, 0)
root.record_count = root.records.shape[0]
root.records = root.records
root.records.aggregations = root.records.groupby(dimensions).agg(aggregations)
total = 0
for pre, fill, node in RenderTree(root):
    treestr = u"%s%s" % (pre, node.name)
    total += node.record_count
    print(treestr, "Records:", node.record_count, "Total : ", total)
    
print("TOTAL : ", t_total)
print("ACTUAL : ", total)
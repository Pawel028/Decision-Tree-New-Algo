from Decision_Tree import Data_Tree
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

new = pd.read_excel("./pivot1.xlsx")
split = new[new.columns[0]]
for i in range(len(split)):
    split.iloc[i] = ""

data=Data_Tree(filename="./pivot1.xlsx",split = split, nrows_orig=[])
data.matrix = data.assign_data()
data.nrows_orig = data.matrix[data.depvar2].sum()
data.update_const()

data, split_final = data.create_nodes()
pd.DataFrame(split_final).to_excel("split.xlsx")

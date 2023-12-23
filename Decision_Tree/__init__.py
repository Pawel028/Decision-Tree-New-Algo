from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

def metric(d,varname, depvar1, depvar2, avg_metric):
    dgb = pd.DataFrame(d).groupby(by = varname).sum() 
    met=dgb[depvar1]/dgb[depvar2]
    met1=pd.DataFrame(met)
    met1.columns = {"Fraction"}
    met1["Avg_metric"] = avg_metric          
    return met1

def find_string(string, array):
    for i in array:
        if i == string:
            return True
        
    return False

def exec_func(command):
    exec(command, globals())


class Data_Tree:
    matrix = []

    def __init__(self, filename,split,nrows_orig, varlist=[],depvar1="Duplicates",depvar2="Claims",nrows=0, nvars=0, avg_metric=0, split_var=[], 
                 neg_split_var_cat=[], pos_split_var_cat=[], split_ratio=0.2):
        self.filename = filename
        self.varlist = varlist
        self.nrows = nrows
        self.nvars = nvars
        self.depvar1 = depvar1
        self.depvar2 = depvar2
        self.avg_metric = avg_metric
        self.split_var = split_var
        self.neg_split_var_cat = neg_split_var_cat
        self.pos_split_var_cat = pos_split_var_cat
        self.split = split
        self.nrows_orig = nrows_orig
        self.split_ratio=split_ratio
        # self.data = self.assign_data(filename)        

    def assign_data(self):
        return pd.read_excel(self.filename)
    
    def Best_Split(self):
        diff = 0
        split_var=np.array([])
        neg_split_var_cat=np.array([])
        pos_split_var_cat=np.array([])

        for var in self.varlist:
            length = len(self.matrix[var].unique())
            # print(length)
            cat_array = self.matrix[var].unique()
            neg_split = np.array([])
            pos_split = np.array([])

            if (length >1 and var != self.depvar1 and var != self.depvar2):
                for cat in self.matrix[var].unique():
                    mat = self.matrix[self.matrix[var]!=cat]
                    mtr = (mat[self.depvar1].sum()/mat[self.depvar2].sum())
                    if mtr>=self.avg_metric:
                        neg_split = np.append(neg_split,cat)
                    else:
                        pos_split = np.append(pos_split,cat)
                # print(pos_split,neg_split)
                mat_neg = self.matrix[self.matrix[var].isin(neg_split)]
                mat_pos = self.matrix[self.matrix[var].isin(pos_split)]
                neg_metric = mat_neg[self.depvar1].sum()/mat_neg[self.depvar2].sum()
                pos_metric = mat_pos[self.depvar1].sum()/mat_pos[self.depvar2].sum()
                diff1=abs(neg_metric-pos_metric)
                if diff1>diff:
                    split_var=var
                    neg_split_var_cat = neg_split
                    pos_split_var_cat = pos_split


        return split_var, neg_split_var_cat, pos_split_var_cat
    

    def update_const(self):
        self.nrows = len(self.matrix)
        self.varlist = self.matrix.columns
        self.nvars = len(self.matrix.columns)
        self.avg_metric = self.matrix[self.depvar1].sum()/self.matrix[self.depvar2].sum()

        self.split_var, self.neg_split_var_cat, self.pos_split_var_cat = self.Best_Split()
        # print(self.split_var, self.neg_split_var_cat, self.pos_split_var_cat)

        for i in range(len(self.matrix)):
            if find_string(self.matrix[self.split_var].iloc[i],self.neg_split_var_cat):        
                self.split.iloc[i] = str(self.split.iloc[i])+"Split_Neg:"+str(self.split_var)+"->"+str(self.neg_split_var_cat)+":: "
            else:
                self.split.iloc[i] = str(self.split.iloc[i])+"Split_Pos:"+str(self.split_var)+"->"+str(self.pos_split_var_cat)+":: "
        # print(self.split)
                
    def split_data(self):
        split = self.split[self.matrix[self.split_var].isin(self.neg_split_var_cat)]
        neg_data = Data_Tree(filename="./pivot1.xlsx", split=split, nrows_orig=self.nrows_orig)
        neg_data.matrix = self.matrix[self.matrix[self.split_var].isin(self.neg_split_var_cat)]
        neg_data.update_const()
        if neg_data.split_var != [] and (neg_data.matrix[self.depvar2].sum())>neg_data.split_ratio*(neg_data.nrows_orig):
            neg_data = neg_data.split_data()


        split = self.split[self.matrix[self.split_var].isin(self.pos_split_var_cat)]
        pos_data = Data_Tree(filename="./pivot1.xlsx", split=split, nrows_orig = self.nrows_orig)
        pos_data.matrix = self.matrix[self.matrix[self.split_var].isin(self.pos_split_var_cat)]
        pos_data.update_const()
        if pos_data.split_var != [] and (pos_data.matrix[self.depvar2].sum())>pos_data.split_ratio*(pos_data.nrows_orig):
            pos_data = pos_data.split_data()

        self.matrix = pd.concat([neg_data.matrix, pos_data.matrix])
        self.split = pd.concat([neg_data.split, pos_data.split])
        return self
    
    def create_nodes(self):
        self.split_data()
        self.matrix = self.matrix.sort_index()
        self.split = self.split.sort_index()
        split_fin = self.split.unique()
        self.matrix.Nodes = self.split
        self.matrix.Nodes.rename("Nodes")
        num=1
        ind = []
        for j in split_fin:
            global data_mat
            data_mat = pd.DataFrame(self.matrix)
            rule=""
            for i in j.split("::"):        
                if i != " ": 
                    if i.split(":")[1][0:2]!="[]":                
                        cat_array = i.split(":")[1].split("->")[1][1:-1].replace("'","").split(" ")
                        var = i.split(":")[1].split("->")[0]
                        str2 = "data_mat = data_mat[data_mat['"+str(var)+"'].isin("+str(cat_array)+")]"
                        
                        exec_func(str2)                
            ind = data_mat.index
            str3 = "self.matrix.Nodes.iloc[ind]='Node_"+str(num)+"'"
            exec(str3)
            # print(str3)
            num=num+1
        # print(self.matrix.Nodes.columns)
        self.matrix = pd.concat([self.matrix,self.matrix.Nodes], axis=1)
        list = np.array(self.varlist)
        self.matrix.columns = (np.append(list,'Nodes'))
        # print(self.matrix.Nodes.columns)
            # print(self.matrix.Nodes.unique())        
        return self, split_fin



            






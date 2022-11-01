import pandas as pd
one = pd.DataFrame
two = pd.DataFrame

one=pd.read_csv("C:/Py/DF/one.txt", sep=',', usecols=['CustomerId','Transaction_Amount','Transaction_Date'], dtype={'CustomerId':str,'Transaction_Amount':float,'Transaction_Date':str})
two=pd.read_csv("C:/Py/DF/two.txt", sep=',', usecols=['CustomerId','Transaction_Amount','Transaction_Date'], dtype={'CustomerId':str,'Transaction_Amount':float,'Transaction_Date':str})
one=one.round({'Transaction_Amount':2})
two=two.round({'Transaction_Amount':2})
one['Transaction_Date'] = one['Transaction_Date'].str.replace('-','/').str.replace(' 00:00:00','')

# print('\n--> 1st dataframe',one.to_string())
print('\n--> Columns of 1st dataframe',one.columns)
print('\n--> Index of 1st dataframe',one.index)
# print('\n--> 2nd dataframe',two.to_string())
print('\n--> Columns of 2nd dataframe',two.columns)
print('\n--> Index of 2nd dataframe',two.index)

union=pd.merge(one,two,on='CustomerId',how='outer')
print('\n--> Combined dataset (outer-join on one key):\n',union.to_string())

merged=pd.merge(one,two,on=['CustomerId','Transaction_Amount','Transaction_Date'],how='inner')
print('\n--> Fully matching dataset (inner-join on multiple keys):\n',merged.to_string())

missinga=one.merge(two,on='CustomerId',how='outer',indicator=True).loc[lambda x : x['_merge']=='left_only']
print('\n--> Keys extra in dataset one (outer-join on one key, display left_only):\n',missinga.iloc[:,[0,1,2]].to_string())

missingb=one.merge(two,on='CustomerId',how='outer',indicator=True).loc[lambda x : x['_merge']=='right_only']
print('\n--> Keys extra in dataset two (outer-join on one key, display right_only):\n',missingb.iloc[:,[0,1,2]].to_string())

mismatches=pd.merge(one,two,on='CustomerId',how='outer',indicator=True).loc[lambda x : (x['Transaction_Amount_x']!=x['Transaction_Amount_y']) & (x['_merge']=='both')]
print('\n--> Value Mismatches (outer-join on one key, display both):\n',mismatches.iloc[:,[0,1,2]].to_string())

print('\n--> Checking Totals:',union.shape[0],'=',merged.shape[0]+missinga.shape[0]+missingb.shape[0]+mismatches.shape[0],'=>',union.shape[0]==merged.shape[0]+missinga.shape[0]+missingb.shape[0]+mismatches.shape[0])

print('\n--> Rows which are not common between two dataframes:')
print(pd.concat([one,two]).drop_duplicates(keep=False))

print('\n--> Transaction_Amounts that are not common between the two dataframes:')
print(set(one.Transaction_Amount).symmetric_difference(two.Transaction_Amount))

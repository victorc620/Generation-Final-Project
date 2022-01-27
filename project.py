import ast
import pandas as pd

df = pd.read_csv('Group project/chesterfield_25-08-2021_09-00-00.csv', names = ["datetime","Location","fullname", "products+price", "total_price","payment_type","card_number"])
df = pd.read_csv('Group project/chesterfield_25-08-2021_09-00-00.csv', converters={'products+price': ast.literal_eval})
df.insert(0, "Main Key", range(1, 1+ len(df)))

#df.drop(['card_number', 'fullname'], inplace=True, axis=1)

print(df.explode("products+price"))
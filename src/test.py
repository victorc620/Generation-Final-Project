import pandas as pd
df = pd.read_csv('/home/jean/Desktop/Cafe_Intel/team-4-project/src/chesterfield_25-08-2021_09-00-00.csv', names = ["datetime","Location","fullname", "products+price", "total_price","payment_type","card_number"])
print(df.drop("card_number", inplace=True, axis=1))

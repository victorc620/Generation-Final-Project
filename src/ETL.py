from sys import argv
import csv 

script, filename = argv

def column_headers():
    with open(filename, newline='') as data:
        reader = csv.reader(data,dialect='excel')
        data= []
        for line in reader:
            data.append(line)
    return data
      

data = column_headers()

print(type(data))
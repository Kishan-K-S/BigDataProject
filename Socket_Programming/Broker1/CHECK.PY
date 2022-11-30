import os 
import json 

filename = './partition1.json'
entry = {'carl':33}
entry2 = {'carl2':34}
entry3 = []


with open(filename,'w') as file:
    json.dump(entry3,file)

with open(filename,'r') as file:
    data = json.load(file)
   
data.append(entry)

with open(filename,"w") as file:
    json.dump(data,file)
with open(filename,'r') as file:
    data = json.load(file)
data.append(entry2)

with open(filename,"w") as file:
    json.dump(data,file)
import json 

filename = ".\consumer.json"
final =[]
with open(filename,'r') as file:
    data = json.load(file)
    
    for i in range(0,len(data)):
        for j in range(0,3):
            for k in data[i][j]:
                for m in k:
                    if(int(m)>22000):
                        final.append(k)
                
    
    print(final)
        
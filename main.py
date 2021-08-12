
import json
import pandas

fileName='C:/Temp/Transaction.csv'
file_df=pandas.read_csv(fileName)
output_file_batch_size=1000
file_df = file_df[['Account_ID ','CODE ','Implemented Date ','Active Indicator ','Account Type ','Service ','BU','Request Date ','Account status ','Status Code ','$ Amount ','Version ','Agent ID ','FIBRE ','last Updated Date ','Property TYPE ','Post Code ']]

#creating hash based on account_id, property_type,account_id
print("*creating hash")
a = (file_df["Account_ID "].astype(str) +"_"+ file_df["Property TYPE "].astype(str) ).to_list()
file_df["hash_value"]=list(map(lambda x: hash(x),a ))
del a
print(file_df.head() )


#Filter out the questionable data
print("#Filter out the questionable data")
file_df=file_df.dropna()
print(file_df)


#Write the data in a JSON file one record at a time
#When the number of events reach 1000, output the events to a JSON file
print("#Write the data in a JSON file one record at a time")
counter=0
len= len(file_df[['Account_ID ', 'Property TYPE ']].to_dict(orient='index'))
for key,value in file_df[['Account_ID ','Property TYPE ']].to_dict(orient='index').items():
    print(value)
    if (counter  ==0):
        jsonFile = open("C:/Temp/transaction."+str(int(counter/output_file_batch_size)+1)+"json", "w")
        jsonFile.write("[")
        jsonFile.write( json.dumps(value))
        if (counter<len-1 and counter %output_file_batch_size !=(output_file_batch_size-1)):
            jsonFile.write(',')
    elif (counter %output_file_batch_size ==0):
        jsonFile.write("]")
        if ( jsonFile.closed != True):
            jsonFile.close()
        jsonFile = open("C:/Temp/transaction."+str(int(counter/output_file_batch_size)+1)+"json", "w")
        jsonFile.write("[")
        jsonFile.write( json.dumps(value))
        if (counter<len-1 and counter %output_file_batch_size !=(output_file_batch_size-1)):
            jsonFile.write(',')
    else:
        jsonFile.write( json.dumps(value))
        if (counter<len-1 and counter %output_file_batch_size !=(output_file_batch_size-1)):
            jsonFile.write(',')

    counter=counter+1
#    print(counter)
jsonFile = open("C:/Temp/transaction." + str(  int(counter / output_file_batch_size) + 1) + "json", "w")
jsonFile.write(']')
jsonFile.close()


#List post codes based on fastest response . Hint ( Refer columns Request date and implementation Date )
print("#List post codes based on fastest response")
file_df['Response_Time']=pandas.to_datetime(file_df['Implemented Date '] , format="%d/%m/%Y %H:%M")-pandas.to_datetime(file_df['Request Date ']  ,format="%d/%m/%Y %H:%M")
#print(file_df['Response_Time'])
print(file_df.nsmallest(5,columns=['Response_Time']))

#Top Agents based on postcode and amount
print("#Top Agents based on postcode and amount")
top_agent=file_df.groupby(by=['Post Code ','Agent ID '] ,axis=0)['Post Code ','Agent ID ','$ Amount '].agg({'$ Amount ':'sum'}).reset_index()
print(top_agent.groupby(by=['Post Code '],axis=0)['Post Code ','Agent ID ','$ Amount '].agg({'$ Amount ':'max'}).reset_index())
del top_agent
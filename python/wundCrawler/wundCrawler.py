import requests
import pandas as pd

print("Crawler test")

parameters = {'ID':'IYUCATNT2','month':1,'day':1,'year':2016,'format':1}

# Other tries:
#parameters = {'ID':'IYUCATNT2','month':1,'day':1,'year':2017}
#parameters = {'ID':'IYUCATNT2','month':'1','day':'1','year':'2017','format':'1'}

# pd.date_range(start='2016-01-01',end='2016-12-31',freq='D')

#Bad date:
#2016-07-26
#parameters = {'ID':'IYUCATNT2','month':7,'day':26,'year':2016,'format':1}
#2017-06-11
#parameters = {'ID':'IYUCATNT2','month':6,'day':11,'year':2017,'format':1}

resp = requests.get('https://www.wunderground.com/weatherstation/WXDailyHistory.asp',params=parameters)

print(resp.content.decode('utf-8'))
#?ID=IYUCATNT2&month=1&day=1&year=2017&format=5')

print(resp.content.decode('utf-8').split('\n')[1])
f= open("date.txt","wb")
#f.write(resp.content.decode('utf-8'))
f.write(resp.content)
f.close()

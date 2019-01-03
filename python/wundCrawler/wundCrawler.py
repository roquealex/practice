import requests
import pandas as pd
import re
import os

def setupPath(dirpath):
    if dirpath not in ['','.','./'] and not os.path.isdir(dirpath) :
        print("Path do not exist",dirpath)
        print("Creating it...")
        os.makedirs(dirpath)
    


print("Crawler test")

#specify the file as a pandas.Timestamp 

path = '.'
hier = False
#hier = True
hier_patt = '%Y/%m/'
format = 'Csv'
pwsID = 'IYUCATNT2'
ts = pd.Timestamp(year=2017, month=1, day=1)
skip=True


parameters = {'ID':pwsID,'month':ts.month,'day':ts.day,'year':ts.year,'format':1}

#dateStr = ts.strftime('yyyy-MM-dd')
dateStr = ts.strftime('%Y-%m-%d')

# Other tries:
#parameters = {'ID':'IYUCATNT2','month':1,'day':1,'year':2017}
#parameters = {'ID':'IYUCATNT2','month':'1','day':'1','year':'2017','format':'1'}

# pd.date_range(start='2016-01-01',end='2016-12-31',freq='D')

#Bad date:
#2016-07-26
#parameters = {'ID':'IYUCATNT2','month':7,'day':26,'year':2016,'format':1}
#2017-06-11
#parameters = {'ID':'IYUCATNT2','month':6,'day':11,'year':2017,'format':1}

# From spark:
#Format("yyyy-MM-dd HH:mm:ss")

resp = requests.get('https://www.wunderground.com/weatherstation/WXDailyHistory.asp',
                    params=parameters,timeout=10)
resp.raise_for_status()
#print(resp.content.decode('utf-8'))
# Binary:
#print(resp.content)
# text:
print(resp.text)
#?ID=IYUCATNT2&month=1&day=1&year=2017&format=5')

#print(resp.content.decode('utf-8').split('\n')[1])
lines = resp.text.split('\n')
print(lines[1])

#    '^Time,TemperatureF,.*,WindDirectionDegrees,WindSpeedMPH,.*0,DateUTC\S$',
#    '^Time,TemperatureF,.*,WindDirectionDegrees,WindSpeedMPH,.*0,DateUTC\S*$',
valid = re.match(
    r'^Time,TemperatureF,.*,WindDirectionDegrees,WindSpeedMPH,.*,DateUTC\S*$',
    resp.text.split('\n')[1])

assert valid, 'Invalid format for date '+dateStr+', header not found'

print(dateStr)

fileBaseName = pwsID+'-'+dateStr

hierPath = ''
if hier :
    hierPath = ts.strftime(hier_patt)
    

basePath = path+'/'+hierPath
setupPath(basePath)

fileBasePath = basePath+fileBaseName

format = ''
if format.lower() == 'csv' :
    print('Saving CSV file')
    fileName = fileBasePath+'.csv'
    #noBrLines lines.map(lambda x: re.sub(r'<br>|,$',r'',x),lines)
    # The response from wunderground is almost a csv but it has <br> and a coma at the end
    noBrLines = map(lambda x: re.sub(r'<br>|,$',r'',x),lines)
    noBlankLines = filter(lambda x : x!='',noBrLines)
    #print(list(noBlankLines))
    f= open(fileName,"w")
    f.write('\n'.join(noBlankLines))
    f.close()
else :
    fileName = fileBasePath+'.txt'
    print('Saving raw file',)
    # Raw file from the binary response
    f= open(fileName,"wb")
    f.write(resp.content)
    f.close()
    
#f= open("date.txt","wb")
#f.write(resp.content)
#f.close()

# some nice info:
#print(resp.url)
#print(resp.encoding)
#print(requests.codes.ok)





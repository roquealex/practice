import requests
import pandas as pd
import re
import os
import time

def setupPath(dirpath):
    if dirpath not in ['','.','./'] and not os.path.isdir(dirpath) :
        print("Path do not exist",dirpath)
        print("Creating it...")
        os.makedirs(dirpath)
    
def requestDaily(pwsID, ts, path='.', hier=True, hier_patt='%Y/%m/',
                 format = 'csv',skip=True,sleep=3) :

    # This string version of the date will be useful in the whole function
    dateStr = ts.strftime('%Y-%m-%d')

    # First calculate filenames to see if the file exist and we need to skip
    fileBaseName = pwsID+'-'+dateStr

    hierPath = ''
    if hier :
        hierPath = ts.strftime(hier_patt)

    basePath = path+'/'+hierPath

    fileBasePath = basePath+fileBaseName

    fileName = fileBasePath+('.csv' if format.lower() == 'csv' else '.txt')
    if skip and os.path.exists(fileName) :
        print(fileName,"Already exists skiping it...")
        # TODO change for return when this is a function
        return

    '''
    if format.lower() == 'csv' :
        fileName = fileBasePath+'.csv'
    else :
        fileName = fileBasePath+'.txt'
    '''

    parameters = {'ID':pwsID,'month':ts.month,'day':ts.day,'year':ts.year,'format':1}

    resp = requests.get('https://www.wunderground.com/weatherstation/WXDailyHistory.asp',
                        params=parameters,timeout=10)
    resp.raise_for_status()
    #print(resp.content.decode('utf-8'))
    # Binary:
    #print(resp.content)
    # text:
    #print(resp.text)
    #?ID=IYUCATNT2&month=1&day=1&year=2017&format=5')

    #print(resp.content.decode('utf-8').split('\n')[1])

    lines = resp.text.split('\n')

    #print(lines[1])

    #    '^Time,TemperatureF,.*,WindDirectionDegrees,WindSpeedMPH,.*0,DateUTC\S$',
    #    '^Time,TemperatureF,.*,WindDirectionDegrees,WindSpeedMPH,.*0,DateUTC\S*$',
    valid = re.match(
        r'^Time,TemperatureF,.*,WindDirectionDegrees,WindSpeedMPH,.*,DateUTC\S*$',
        resp.text.split('\n')[1])

    assert valid, 'Invalid format for date '+dateStr+', header not found'


    setupPath(basePath)


    if format.lower() == 'csv' :
        print('Writing CSV file',fileName)
        #fileName = fileBasePath+'.csv'
        #noBrLines lines.map(lambda x: re.sub(r'<br>|,$',r'',x),lines)
        # The response from wunderground is almost a csv but it has <br> and a coma at the end
        noBrLines = map(lambda x: re.sub(r'<br>|,$',r'',x),lines)
        noBlankLines = filter(lambda x : x!='',noBrLines)
        #print(list(noBlankLines))
        f= open(fileName,"w")
        f.write('\n'.join(noBlankLines))
        f.close()
    else :
        #fileName = fileBasePath+'.txt'
        print('Writing raw file',fileName)
        # Raw file from the binary response
        f= open(fileName,"wb")
        f.write(resp.content)
        f.close()
        
    time.sleep(sleep)


print("Crawler test")

#specify the file as a pandas.Timestamp 

#pwsID = 'IYUCATNT2'
#ts = pd.Timestamp(year=2017, month=1, day=1)
#requestDaily(pwsID, ts, path='.', hier=True, hier_patt='%Y/%m/',format = 'csv',skip=True) :

pwsID = 'IYUCATNT2'

#dates = pd.date_range(start='2016-01-01',end='2016-01-31',freq='D',tz='America/Merida')
#
dates = pd.date_range(start='2012-01-01',end='2016-12-31',freq='D')

basePath = '.'
cachePath = basePath+'/dailyHistory/'+pwsID
for date in dates :
    requestDaily(pwsID,date,path=cachePath)
    # Miving the sleep inside the method
    #time.sleep(3)

#print("one")
#time.sleep(3)
#print("two")
#time.sleep(3)
#print("three")
#time.sleep(3)




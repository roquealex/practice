import pandas as pd
import os
from pandas.api.types import is_numeric_dtype

def purgeFile(path,remove=False) :
    if remove :
        os.remove(path)
    else :
        print(f'+ File {path} skipped removal')
    

def testAndPurgeFile(fileUnderTest, threshold = 1, numcols=16):
    # Test if file exists:
    if not os.path.exists(fileUnderTest) :
        print(f'WARNING: File {fileUnderTest} is not available')
        return

    # Read it from pandas and see how many entries and schema, etc
    df = pd.read_csv(fileUnderTest)

    # Number of rows
    dfShape = df.shape
    if dfShape[0] < threshold :
        print(f'Purging file {fileUnderTest} number of rows less than {threshold}, actual {dfShape[0]}')
        purgeFile(fileUnderTest)
        return
        exit(0)

    # Not all the required cols
    requiredCols = ['Time','WindDirection','WindDirectionDegrees', 'WindSpeedMPH', 'WindSpeedGustMPH']
    if not all([x in df.columns.values for x in requiredCols]) :
        print(f'Purging file {fileUnderTest} one of the required columns is missing: {", ".join(requiredCols)}')
        purgeFile(fileUnderTest)
        return
        exit(0)

    # Not numeric
    numericCols = ['WindDirectionDegrees', 'WindSpeedMPH', 'WindSpeedGustMPH']
    if not all([is_numeric_dtype(df[x]) for x in numericCols]) :
        print(f'Purging file {fileUnderTest} one of the following columns is not numeric: {", ".join(requiredCols)}')
        purgeFile(fileUnderTest)
        return
        exit(0)

    # Number of cols
    if dfShape[1] != numcols :
        print(f'Purging file {fileUnderTest} number of columns is not {numcols}, actual {dfShape[1]}')
        purgeFile(fileUnderTest)
        return
        exit(0)


def testDaily(pwsID, ts, path='.', hier=True, hier_patt='%Y/%m/',
                 format = 'csv') :

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
    #print(f'testing file {fileName}')
    testAndPurgeFile(fileName)

print('Purge Empty')

#fileUnderTest = './dailyHistory/IYUCATNT2/2016/01/IYUCATNT2-2016-01-02.csv'
#threshold = 90
#numcols = 16

#testAndPurgeFile('./dailyHistory/IYUCATNT2/2016/01/IYUCATNT2-2016-01-02.csv')

pwsID = 'IYUCATNT2'
#date = pd.Timestamp(year=2016, month=1, day=2)

# This range has to match exactly the crawler script
dates = pd.date_range(start='2012-01-01',end='2016-12-31',freq='D')

basePath = '.'
cachePath = basePath+'/dailyHistory/'+pwsID

for date in dates :
    testDaily(pwsID, date, path=cachePath)

#python purgeEmpty.py | grep -v  "^+"  > problems.txt 


import pandas as pd
import os
from pandas.api.types import is_numeric_dtype

def purgeFile(path,remove=False) :
    if remove :
        os.remove(path)
    else :
        print(f'+ File {path} skipped removal')
    


def testAndPurgeFile(fileUnderTest, threshold = 90, numcols=16):
    # Test if file exists:
    if not os.path.exists(fileUnderTest) :
        print(f'WARNING: File {fileUnderTest} is not available')
        exit(0)

    # Read it from pandas and see how many entries and schema, etc
    df = pd.read_csv(fileUnderTest)

    # Number of rows
    dfShape = df.shape
    if dfShape[0] < threshold :
        print(f'Purging file {fileUnderTest} number of rows less than {threshold}')
        purgeFile(fileUnderTest)
        exit(0)

    # Number of cols
    if dfShape[1] != numcols :
        print(f'Purging file {fileUnderTest} number of columns is not {numcols}')
        purgeFile(fileUnderTest)
        exit(0)

    # Not all the rows
    requiredCols = ['Time','WindDirection','WindDirectionDegrees', 'WindSpeedMPH', 'WindSpeedGustMPH']
    if not all([x in df.columns.values for x in requiredCols]) :
        print(f'Purging file {fileUnderTest} one of the required columns is missing: {", ".join(requiredCols)}')
        purgeFile(fileUnderTest)
        exit(0)

    # Not numeric
    numericCols = ['WindDirectionDegrees', 'WindSpeedMPH', 'WindSpeedGustMPH']
    if not all([is_numeric_dtype(df[x]) for x in numericCols]) :
        print(f'Purging file {fileUnderTest} one of the following columns is not numeric: {", ".join(requiredCols)}')
        purgeFile(fileUnderTest)
        exit(0)

print('Purge Empty')

#fileUnderTest = './dailyHistory/IYUCATNT2/2016/01/IYUCATNT2-2016-01-02.csv'
#threshold = 90
#numcols = 16

testAndPurgeFile('./dailyHistory/IYUCATNT2/2016/01/IYUCATNT2-2016-01-02.csv')



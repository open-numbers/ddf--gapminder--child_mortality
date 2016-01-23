# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 10:48:56 2016

@author: FinArb
"""

#declare libraries
import os
import urllib2
import requests
import time
import sys
import xlrd
import pandas as pd
#take in command line arguments
root=str(sys.argv[1])
pathspt=os.path.sep
eufinm="http://www.childmortality.org/files_v20/download/RatesDeaths_AllIndicators.xlsx"
sseufinm="http://www.childmortality.org/files_v20/download/SexSpecificU5MRandIMR.xlsx"
arrufm="http://www.childmortality.org/files_v20/download/U5MR_ARR_1990-2015.xlsx"
udata="http://www.childmortality.org/files_v20/download/UN%20IGME%20Total%20U5MR,%20IMR%20and%20NMR%20Database%202015.xlsx"
GRMDG="http://www.childmortality.org/files_v20/download/MDGRegion%20Rates%20&%20Deaths%201970-2015%20(Coverage%2080%20plus).xlsx"
GRU="http://www.childmortality.org/files_v20/download/UNICEFRegion%20Rates%20&%20Deaths%201970-2015%20(Coverage%2080%20plus).xlsx"
#make directories
if not os.path.isdir(root+pathspt+'downloaded'):
    os.makedirs(root+pathspt+'downloaded')

if not os.path.isdir(root+pathspt+'files'):
    os.makedirs(root+pathspt+'files')
    
#map to geo mapping file
geo=pd.read_csv('https://raw.githubusercontent.com/open-numbers/ddf--gapminder--dim_geo_countries_and_groups/master/ddf--alias_mapping--country.csv',sep=',',header=0)        
geo.columns=['geobymapping','Region']
geo.set_index('Region', inplace=True)
    
#function to download
def downloadFile(url, directory) :
    localFilename = url.split(pathspt)[-1]
    with open(directory + pathspt + localFilename, 'wb') as f:
        start = time.clock()
        r = requests.get(url, stream=True)
        total_length = r.headers.get('content-length')
        dl = 0
        if total_length is None: # no content length header
            f.write(r.content)
        else:
            for chunk in r.iter_content(1024):
                dl += len(chunk)
                f.write(chunk)
                done = int(50 * int(dl) / int(total_length))
                sys.stdout.write("\033[K")
                sys.stdout.write("\r[%s%s] %s bps\r" % ('=' * done, ' ' * (50-done), dl//(time.clock() - start)))
                print ''
    return (time.clock() - start)

def cmewritecsv(x,df,indseries):
    temp=df[df['Variable']==indseries[x]]
    temp.drop('Variable', axis=1, inplace=True)
    temp.to_csv(root+pathspt+"files"+pathspt+'data_for--'+indseries[x].replace('/','_')+'.csv')
    print("Printed file: data_for--"+indseries[x].replace('/','_')+".csv\n")

#fundtion to find starting and ending location in excel
_ordA = ord('A')
def find_val_in_workbook(wbname, val, sheetname, celltype):
    wb = xlrd.open_workbook(wbname)
    sheet=wb.sheet_by_name(sheetname)
    for rowidx in range(sheet.nrows):
        row = sheet.row(rowidx)
        for colidx, cell in enumerate(row):
            if cell.ctype != celltype:
                continue
            if cell.value != val:
                continue
            if colidx > 26:
                colchar = chr(_ordA + colidx / 26)
            else:
                colchar = ''
            colchar += chr(_ordA + colidx % 26)
            return(rowidx)
            
#function to check if previously downloaded and download file
def checkndownload(outputFilename,filelink,mfile):
    if (not os.path.isfile(outputFilename)):
        time_elapsed = downloadFile(filelink,root+pathspt+"downloaded")
        print "Download complete..."
        print "Time Elapsed: " + str(time_elapsed)
        req = urllib2.Request(filelink)
        url_handle = urllib2.urlopen(req)
        headers = url_handle.info()
        #etag = headers.getheader("ETag")
        last_modified = headers.getheader("Last-Modified")
        last_modified=last_modified[:-4]
        last_modified=last_modified.replace(",", "", 1)
        text_file = open(root+pathspt+"downloaded"+pathspt+mfile, "w")
        text_file.write(last_modified)
        text_file.close()
    else:
        req = urllib2.Request(filelink)
        url_handle = urllib2.urlopen(req)
        headers = url_handle.info()
        #etag = headers.getheader("ETag")
        last_modified = headers.getheader("Last-Modified")
        last_modified=last_modified[:-4]
        last_modified=last_modified.replace(",", "", 1)
        file = open(root+pathspt+"downloaded"+pathspt+mfile, 'r')
        extime=file.read()
        zlmt=time.strptime(last_modified,'%a %d %b %Y %X')
        flmt=time.strptime(extime,'%a %d %b %Y %X')
        if ((time.mktime(zlmt)-time.mktime(flmt))>0):
            time_elapsed = downloadFile(filelink,root+pathspt+"downloaded")
            print "Download complete..."
            print "Time Elapsed: " + str(time_elapsed)
            req = urllib2.Request(filelink)
            url_handle = urllib2.urlopen(req)
            headers = url_handle.info()
            #etag = headers.getheader("ETag")
            last_modified = headers.getheader("Last-Modified")
            last_modified=last_modified[:-4]
            last_modified=last_modified.replace(",", "", 1)
            text_file = open(root+pathspt+"downloaded"+pathspt+mfile, "w")
            text_file.write(last_modified)
            text_file.close()  

#download excel function after comparing time
outputFilename = root+ pathspt+"downloaded"+pathspt + "RatesDeaths_AllIndicators.xlsx"
checkndownload(outputFilename,eufinm,"Zipmd1.txt")

#read and format file 1
book = xlrd.open_workbook(outputFilename)
sheet = book.sheet_by_index(0)
loc=find_val_in_workbook(outputFilename, "ISO Code", sheet.name, 1)
df=pd.read_excel(outputFilename,sheet.name,skiprows=loc)
df.set_index(['ISO Code','CountryName','Uncertainty bounds*'],inplace=True)
stackdata=df.stack()
stackdata.to_csv(root+ pathspt+"downloaded"+pathspt + "stackdata.csv")
df=pd.read_csv(root+ pathspt+"downloaded"+pathspt + "stackdata.csv",header=None,names=['ISO Code','CountryName','Uncertainty bounds*','Variable','Value'])
os.remove(root+ pathspt+"downloaded"+pathspt + "stackdata.csv")
#split variable get the year
df['Year']=df.Variable.str.split('.').str[-1]
df['Variable']=df.Variable.str[0:-5]
df.columns=['Region','CountryName','Uncertainty bounds*','Variable','Value','Year']
df.set_index("Region",inplace=True)
df=df.join(geo,how='left',lsuffix='l',rsuffix='r')
df=df.reset_index()
indseries=pd.unique(df['Variable'].ravel())
for x in range(0,len(indseries)):
    cmewritecsv(x,df,indseries)
    
#download excel function after comparing time
outputFilename = root+ pathspt+"downloaded"+pathspt + "SexSpecificU5MRandIMR.xlsx"
checkndownload(outputFilename,sseufinm,"Zipmd2.txt")

#read and format file 2 sheet1
book = xlrd.open_workbook(outputFilename)
sheet = book.sheet_by_index(0)
loc=find_val_in_workbook(outputFilename, "ISO Code", sheet.name, 1)
df=pd.read_excel(outputFilename,sheet.name,skiprows=loc,header=None)
h1=df.iloc[0,:]
h2=df.iloc[1,:]
colname=[]
for x in range (0,len(df.columns)):
    if (pd.isnull(h1[x]) and x>0):
        val=h1[x-1]
        h1[x]=val
for x in range (0,len(df.columns)):
    if x>2:
        h1[x]=str(h1[x])+str(h2[x]).replace(".0","")
h1[2]="Uncertainty"  
df.columns=h1
df=df.ix[2:]      
df.set_index(['ISO Code','Country','Uncertainty'],inplace=True)
stackdata=df.stack()
stackdata.to_csv(root+ pathspt+"downloaded"+pathspt + "stackdata.csv")
df=pd.read_csv(root+ pathspt+"downloaded"+pathspt + "stackdata.csv",header=None,names=['ISO Code','Country','Uncertainty','Variable','Value'])
os.remove(root+ pathspt+"downloaded"+pathspt + "stackdata.csv")
#split variable get the year
df['Year']=df.Variable.str[-4:]
df['Variable']=df.Variable.str[0:-4]+"_U5MR"
df.columns=['Region','CountryName','Uncertainty','Variable','Value','Year']
df.set_index("Region",inplace=True)
df=df.join(geo,how='left',lsuffix='l',rsuffix='r')
df=df.reset_index()
indseries=pd.unique(df['Variable'].ravel())
for x in range(0,len(indseries)):
    cmewritecsv(x,df,indseries)
    
#read and format file 2 sheet2
book = xlrd.open_workbook(outputFilename)
sheet = book.sheet_by_index(1)
loc=find_val_in_workbook(outputFilename, "ISO Code", sheet.name, 1)
df=pd.read_excel(outputFilename,sheet.name,skiprows=loc,header=None)
h1=df.iloc[0,:]
h2=df.iloc[1,:]
colname=[]
for x in range (0,len(df.columns)):
    if (pd.isnull(h1[x]) and x>0):
        val=h1[x-1]
        h1[x]=val
for x in range (0,len(df.columns)):
    if x>2:
        h1[x]=str(h1[x])+str(h2[x]).replace(".0","")
h1[2]="Uncertainty"  
df.columns=h1
df=df.ix[2:]      
df.set_index(['ISO Code','Country','Uncertainty'],inplace=True)
stackdata=df.stack()
stackdata.to_csv(root+ pathspt+"downloaded"+pathspt + "stackdata.csv")
df=pd.read_csv(root+ pathspt+"downloaded"+pathspt + "stackdata.csv",header=None,names=['ISO Code','Country','Uncertainty','Variable','Value'])
os.remove(root+ pathspt+"downloaded"+pathspt + "stackdata.csv")
#split variable get the year
df['Year']=df.Variable.str[-4:]
df['Variable']=df.Variable.str[0:-4]+"_IMR"
df.columns=['Region','CountryName','Uncertainty','Variable','Value','Year']
df.set_index("Region",inplace=True)
df=df.join(geo,how='left',lsuffix='l',rsuffix='r')
df=df.reset_index()
indseries=pd.unique(df['Variable'].ravel())
for x in range(0,len(indseries)):
    cmewritecsv(x,df,indseries)
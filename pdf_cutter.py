import pandas as pd
import os
import re
import datetime as dt
from tabula import read_pdf
from PyPDF2 import PdfFileWriter, PdfFileReader

'''
Project built to cut 1 PDF into smaller files for each corresponding
account in Salesforce.

Code expects additional input to properly id Accounts in Salesforce.

Code builds a file I'm calling a manifest which is used by Data Loader:
- creates a file's file
- associates file with Account
'''

#gathers files in root directory and returns only pdf files
def get_files_from_dir(filepath, ext = '.pdf'):
    filesindir = os.listdir(filepath)
    #tilda indicates open temp file, excluding these
    xlsxfiles = [f for f in filesindir if ext in f and not '~' in f]
    if len(xlsxfiles) == 0:
        print('No files found, try checking the extension.')
    else:
        return xlsxfiles

root = os.getcwd()
account_loc = root + '\\extract.csv'
datestamp = str(dt.datetime.now().strftime('%m-%d-%Y'))
#outputpath = root + '\\Cut Files\\'
pdfdrop = root + '\\PDFdrop\\'


#set up format of manifest for ContentVersion
contentVersion = pd.DataFrame(columns = ['Title','Description','VersionData',\
                                'PathOnClient','FirstPublishLocationId'])
#get account IDs by SCEIS code from Salesforce csv
accountids = pd.read_csv(account_loc)
#build dictionary because i don't know how to do this right
acctid_dict = {}
for index, row in accountids.iterrows():
    acctid_dict[row['CODE__C']] = row['ID']

print('Splitting PDF.')
#https://stackoverflow.com/questions/490195/split-a-multi-page-pdf-file-into-multiple-pdf-files-with-python
mainpdf = get_files_from_dir(root)
inputpdf = PdfFileReader(open(mainpdf[0], 'rb'))
for i in range(inputpdf.numPages):
    output = PdfFileWriter()
    output.addPage(inputpdf.getPage(i))
    with open(pdfdrop + "\\document-page%s.pdf" % i, "wb") as outputStream:
        output.write(outputStream)

print('Gathering pdfs to parse.')
#get files to process
pdf_location = get_files_from_dir(pdfdrop)
for p in pdf_location:
    #read pdf and put in dataframe
    pdfpage = read_pdf(pdfdrop + p, pages = 'all')
    df = pdfpage[0]

    #make files idenifiers
    agycode = df.iloc[0,0]
    pdate = df.iloc[0,3].replace('/','-')#.strftime('%m-%d-%Y')
    #had to remove random characters like I
    pdate = re.sub(r'[A-z]*','', pdate)
    #because assumed datatype was float
    invoiceno = str(df.iloc[0,4])[:-2]
    invoiceamt = str(df.iloc[len(df)-1,10])
    #rebuild date for sort
    tdate = '20'+pdate[-2:] +'-'+ pdate[:2] +'-'+ pdate[3:5]
    gendate = datestamp
    filename = p
    outputpath = pdfdrop
    customername = str(df.iloc[0,2])

    titledate = tdate + ' - ' + invoiceamt + ' - ' + customername +\
                ' - Invoice ' + invoiceno
    printfilename = agycode + ' Invoice Date ' + pdate
    desc = 'Billing for services on ' + pdate + '. Generated on ' +\
            gendate

    #gets Salesforce ID for account
    idofaccount = acctid_dict[agycode]

    #generating ContentVersion manifest
    nextentry = pd.Series([titledate, desc, outputpath + filename, \
                            outputpath + filename, idofaccount], \
                            index = contentVersion.columns)
    contentVersion = contentVersion.append(nextentry, ignore_index = True)

    print('Logging ' + printfilename + ' ' + invoiceno + ' - doc id - ' + filename)

print('Creating manifest for ContentVersion')
contentVersion.to_csv(outputpath + 'ContentVersion Generated On ' + datestamp +\
                        '.csv', index = False)

print('Operation Complete!')

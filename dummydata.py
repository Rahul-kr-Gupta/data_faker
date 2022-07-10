import pandas as pd
import os
import sys
import argparse
import readline
import re
import pyinputplus as pyip
mapping={'char': 'string',
 'date': 'datetime64[ns]',
 'datetime': 'datetime64[ns]',
 'float': 'float64',
 'nchar': 'string',
 'nvarchar': 'string',
 'real': 'float64',
 'smalldatetime': 'datetime64[ns]',
 'smallint': 'int32',
 'tinyint': 'int16',
 'uniqueidentifier': 'string',
 'varbinary': 'bytes',
 'varchar': 'string',
 'int':'int64',
 'string':'string',
 'integer': 'int64',
 'bigint': 'int64',
 'DD/MM/YYYY':'datetime64[ns]',
 'DD/M/YYY':'datetime64[ns]',
 'Date(YYYY-MM-DD)':'datetime64[ns]'
 }
def dummy_data_csv(fileloc,filename,meta,format,count=100):
    count=str(count)
    filename=filename+'.'+format
    fileloc=fileloc.strip()
    meta=meta.strip()
    metafile=metafile=pd.read_fwf("""{}""".format(meta).replace('"',"").encode('unicode_escape').decode(),header=None)
    metafile=metafile.iloc[:,0].apply(lambda x: pd.Series(str(x).split("||")))
    metafile.rename(columns={0:'column_name',1:'sql_type',2:'faker_var'},inplace=True)
    # metafile.sql_type=metafile.sql_type.str.replace(" ","")
    metafile.sql_type=[re.findall(r'\w+',x)[0] for x in metafile.sql_type]
    metafile['python_types']=metafile['sql_type'].map(lambda x: x.lower().split('(')[0]).str.strip().map({k.lower(): v for k, v in mapping.items()})
    metafile=metafile.apply(lambda x: x.str.strip())
    column_mapping=dict(list(zip(metafile.column_name,metafile.python_types)))
    print("""datafaker file {} {} {} --meta {} --format {}""".format(fileloc,filename,count,meta,format))

    # ! calls the datafaker in cmd.
    
    os.system("""datafaker file {} {} {} --meta {} --format {}""".format(fileloc,filename,count,meta,format))
    # print()
    path = """{}""".format(fileloc).replace('"',"")+'/'+filename
    # print()
    df=pd.read_json(path.encode('unicode_escape').decode(),lines=True)
    df=df.astype(column_mapping)
    path = """{}""".format(fileloc).replace('"',"")+'/'+filename.replace('.'+format,"")
    outputtype=input("select the output file format of file 1:csv , 2:Parquet: ")
    
    if outputtype=='1':
        gz_format=pyip.inputYesNo(prompt='do you want to convert the file into Gz format (yes or no): ')
        # gz_format=input("do you want to convert the file into Gz format type Y or N (in capitals)")
        s=input("please input the seperator you want to use: (,|:;)")
        if gz_format.lower()=='yes':
            df.to_csv(path+".csv.gz".encode('unicode_escape').decode(),index=False,sep=s)
        elif gz_format.lower()=='no':
            df.to_csv(path+".csv".encode('unicode_escape').decode(),index=False,sep=s)
    if outputtype=='2':
        df.to_parquet(path+".parquet".encode('unicode_escape').decode(),index=False)
    
    print("File Created sucessfully")
    


if __name__== "__main__":

    fileloc =input("Enter the locatation where you want to store the file: ")
    filename = input("Enter the name of file: ")
    count =int(input("enter number of records needed in dummy data: "))
    meta =input("Enter the location of metadata file(Include the file name also): ")
    format = input("Enter the type of output file (either json(provide us the column names)or text(CSV)): ")
    dummy_data_csv(fileloc=fileloc,filename=filename,count=count,meta=meta,format=format)
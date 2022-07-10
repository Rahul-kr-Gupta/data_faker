import pandas as pd
import os
import re
import pyinputplus as pyip


from configparser import ConfigParser
  
configur = ConfigParser()
configur.read('config.ini')


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
    count = str(count)
    output_filename = filename +'.'+format
    fileloc = fileloc.strip()
    meta=meta.strip()
    metafile = pd.read_fwf("""{}""".format(meta).replace('"',"").encode('unicode_escape').decode(),header=None)
    metafile = metafile.iloc[:,0].apply(lambda x: pd.Series(str(x).split("||")))
    metafile.rename(columns={0:'column_name',1:'sql_type',2:'faker_var'},inplace=True)
    # metafile.sql_type=metafile.sql_type.str.replace(" ","")
    metafile.sql_type = [re.findall(r'\w+',x)[0] for x in metafile.sql_type]
    metafile['python_types']=metafile['sql_type'].map(lambda x: x.lower().split('(')[0]).str.strip().map({k.lower(): v for k, v in mapping.items()})
    metafile = metafile.apply(lambda x: x.str.strip())
    
    column_mapping = dict(list(zip(metafile.column_name,metafile.python_types)))
    print("""datafaker file {} {} {} --meta {} --format {}""".format(fileloc,output_filename,count,meta,format))

    # ! calls the datafaker in cmd.
    
    os.system("""datafaker file {} {} {} --meta {} --format {}""".format(fileloc,output_filename,count,meta,format))
    # print()
    path = """{}""".format(fileloc).replace('"',"")+'/'+output_filename
    # print()
    df = pd.read_json(path.encode('unicode_escape').decode(),lines=True)
    
    df = df.astype(column_mapping)
    
    
    # ! create csv file
    csv_path = configur.get('csv','output_path') + filename
    gz_format= configur.get('csv','zip')
    s =  configur.get('csv','delimiter')
    
    if gz_format.lower()=='yes':
        df.to_csv(csv_path+".csv.gz".encode('unicode_escape').decode(),index=False,sep=s)
    elif gz_format.lower()=='no':
        df.to_csv(csv_path+".csv".encode('unicode_escape').decode(),index=False,sep=s)
            
            
    # ! create parquet file
    parquet_path = csv_path = configur.get('parquet','output_path')+ filename
    df.to_parquet(parquet_path+".parquet".encode('unicode_escape').decode(),index=False)
    
    
    # ! create json
    
    json_path =  configur.get('json','output_path')+ filename
    df.to_json(json_path+".json",orient='records')
    
    print("File Created sucessfully")
    

if __name__== "__main__":
    fileloc = configur.get('datafaker','output_path')
    filename =  configur.get('datafaker','output_filename')
    count = configur.get('datafaker','count')
    meta = configur.get('datafaker','meta_path')
    format = 'json'
    
    # delete the previous created data
    if os.path.exists(fileloc+filename+".json"):
        os.remove(fileloc+filename+".json")
    
    
    dummy_data_csv(fileloc=fileloc,filename=filename,count=count,meta=meta,format=format)
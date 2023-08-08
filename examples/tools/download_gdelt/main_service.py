import sys
import os
import pandas as pd
import urllib.request
from time import time, sleep
import json
from minio import Minio

from flask import Flask, request, jsonify
from apiflask import APIFlask
import schema

# app = Flask(__name__)
app = APIFlask(__name__, spec_path='/specs', docs_path ='/docs')

names = ['GKGRECORDID', 'DATE', 'SOURCECOLLECTIONIDENTIFIER',
         'SOURCECOMMONNAME', 'DOCUMENTIDENTIFIER', 'COUNTS', 'COUNTS_2',
         'THEMES', 'ENHANCEDTHEMES', 'LOCATIONS', 'ENHANCEDLOCATIONS',
         'PERSONS', 'ENHANCEDPERSONS', 'ORGANIZATIONS',
         'ENHANCEDORGANIZATIONS', '5TONE', 'ENHANCEDDATES', 'GCAM',
         'SHARINGIMAGE', 'RELATEDIMAGES', 'SOCIALIMAGEEMBEDS',
         'SOCIALVIDEOEMBEDS', 'QUOTATIONS', 'ALLNAMES', 'AMOUNTS',
         'TRANSLATIONINFO', 'EXTRASXML']

sel_names = ['GKGRECORDID', 'DATE', 'SOURCECOMMONNAME', 'DOCUMENTIDENTIFIER',
             'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS', '5TONE']

def download_list(prefix, out):
    df_files = pd.read_csv(f'{out}temp/{prefix}/masterfilelist.txt', header=None, sep=' ')
    df_files[2] = df_files[2].fillna('')
    df_files = df_files.loc[df_files[2].apply(lambda x: 'gkg' in x)]

    files = pd.Series([file for file in df_files[2] if prefix in file])
    #files = files.head(5)
    
    no_files = 0
    for file in files:
        out_file = file.replace('http://data.gdeltproject.org/gdeltv2/', '')
        if not os.path.exists(f"{out}temp/{prefix}/{out_file}"):
            print(f'Downloading {file}\r', end='')
            try:
                urllib.request.urlretrieve(file, 
                                       f"{out}temp/{prefix}/{out_file}")
                sleep(1)
                no_files += 1
            except Exception as e:
                print("Missed this file")
    return {'original_files': len(files), 'downloaded_files': no_files}
            
    
def filter_datasets(prefix, out, out_file):
    # out_file = f'{out}csv/{prefix}_filtered.csv'
    
    # if os.path.exists(out_file):
    #     return {}
    
    themes = set(pd.read_csv('./agro_themes.csv', header=None).apply(lambda x: x[0], axis=1).values)
    original_no_articles = 0
    filtered_no_articles = 0
    with open(out_file, 'w') as o:
        for file in os.listdir(f'{out}temp/{prefix}'):
            try:
                if not file.endswith(".zip"):
                    continue
                print(f'Filtering {file}\r', end='')
                df = pd.read_csv(f'{out}temp/{prefix}/{file}', sep='\t', names=names, usecols=sel_names, encoding="ISO-8859-1")
                
                original_no_articles += df.shape[0]
                df = df.loc[df['THEMES'].fillna('').apply(lambda x: len(set(x.split(';')) & themes) > 0)]
                filtered_no_articles += df.shape[0]
                df.to_csv(o, header=None, index=None)
            except:
                continue
            
    return {'original_no_articles': original_no_articles, 
            'filtered_no_articles': filtered_no_articles}

def run_function(outfile, prefix, out):
    t1 = time()
    # outfile = f'{out}csv/{prefix}_filtered.csv'
    
    tempdir = f'{out}temp/{prefix}'
    
    # if not os.path.exists(outfile):
    # print(f'Creating {outfile}')
    if not os.path.exists(tempdir):
        os.mkdir(tempdir)
        
    if not os.path.exists(f'{out}temp/{prefix}/masterfilelist.txt'):    
        urllib.request.urlretrieve("http://data.gdeltproject.org/gdeltv2/masterfilelist.txt", 
                                f"{out}temp/{prefix}/masterfilelist.txt")   
        
    log1 = download_list(prefix, out)
    log2 = filter_datasets(prefix, out, outfile)
    # log['metrics'] = log1 | log2
    log = dict(log1.items() | log2.items())
    
    for file in os.listdir(tempdir):
        os.remove(f'{tempdir}/{file}')
    os.rmdir(tempdir)
        
    t2 = time()
    log['time'] = t2-t1
    # with open(logfile, 'w') as o:
    #    o.write(json.dumps(log))  
    return log

@app.route('/', methods=['POST'])
@app.input(schema.Input, location='json', example={"input":[],"parameters":{"prefix":"20230706","out":"./temp/","output_file":"./output.csv"},"minio":{"endpoint_url":"url","id":"XXXXXX","key":"YYYYY","bucket":"bucket"}})
@app.output(schema.Output, status_code=200)
def process_data(data):
    try:
        j = request.json
        outfile = j['parameters']['output_file']
        prefix = j['parameters']['prefix']
        out = j['parameters']['out']
        minio = j['minio']
    
        log = run_function(outfile, prefix, out)
        
        basename = outfile.split('/')[-1]
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        result = client.fput_object(minio['bucket'], basename, outfile)
        object_path = f"s3://{result.bucket_name}/{result.object_name}"
        
        return jsonify({'output': [object_path], 'metrics': log})
    except Exception as e:
        return jsonify({
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9066)

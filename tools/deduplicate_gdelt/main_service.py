import pandas as pd
from pyTokenJoin.jaccard.jaccard_delta import JaccardTokenJoin
from minio import Minio
import json

from flask import Flask, request, jsonify
from apiflask import APIFlask
import schema

# app = Flask(__name__)
app = APIFlask(__name__, spec_path='/specs', docs_path ='/docs')

@app.route('/', methods=['POST'])
@app.input(schema.Input, location='json', example={"input":["s3_path"],"parameters":{"col_id":0,"col_text":7,"delta":0.7,"jointFilter":True,"posFilter":True,"verification_alg":0,"output_file":"./output.csv"},"minio":{"endpoint_url":"url","id":"XXXX","key":"YYYY","bucket":"bucket"}})
@app.output(schema.Output, status_code=200)
def run_function(data):
    # Add your custom logic here
    try:
        j = request.json
        
        print(j)
        input_file = j['input'][0]
        output_file = j['parameters']['output_file']
        col_id = j['parameters']['col_id']
        col_text = j['parameters']['col_text']
        delta = j['parameters']['delta']
        jointFilter = j['parameters']['jointFilter']
        posFilter = j['parameters']['posFilter']
        verification_alg = j['parameters']['verification_alg']
        minio = j['minio']
        
        if input_file.startswith('s3://'):
            bucket, key = input_file.replace('s3://', '').split('/', 1)
            client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
            df = pd.read_csv(client.get_object(bucket, key), header=None)
        else:
            df = pd.read_csv(input_file, header=None)
        
        df[col_text] = df[col_text].str.split(';')
        df = df.loc[~(df[col_text].isna())]
        df[col_text] = df[col_text].apply(lambda x: list(set(x)))
        
        df.columns = [str(col) for col in df.columns]
        
        pairs, log = JaccardTokenJoin().tokenjoin_self(df, id=str(col_id), join=str(col_text),
                                                          delta=delta, jointFilter=jointFilter,
                                                          posFilter=posFilter, verification_alg=verification_alg)
        pairs.to_csv(output_file)
        basename = output_file.split('/')[-1]
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        result = client.fput_object(minio['bucket'], basename, output_file)
        object_path = f"s3://{result.bucket_name}/{result.object_name}"
        
        return jsonify({'output': [object_path], 'metrics': log})
    except Exception as e:
        print(e)
        return jsonify({
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9067)
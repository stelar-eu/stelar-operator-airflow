import pandas as pd
from pytokenjoin.jaccard.join_knn import JaccardTokenJoin
from minio import Minio
import json
import uuid
import asyncio
import websockets
import sys

def prep_df(input_file, col_text, separator, minio):
    """
    Prepare DataFrame from input file.
    """
    if input_file.startswith('s3://'):
        bucket, key = input_file.replace('s3://', '').split('/', 1)
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        df = pd.read_csv(client.get_object(bucket, key), header=None)
    else:
        df = pd.read_csv(input_file, header=None)
    
    col_text = df.columns[col_text]
    df[col_text] = df[col_text].str.split(separator)
    df = df.loc[~(df[col_text].isna())]
    df[col_text] = df[col_text].apply(lambda x: list(set(x)))
    
    df.columns = [str(col) for col in df.columns]
    return df

def synchronous_entity_linking(j):
    try:
        inputs = j['input']
        input_file_left = inputs[0]
        col_id_left = j['parameters']['col_id_left']
        col_text_left = j['parameters']['col_text_left']
        separator_left = j['parameters']['separator_left']        
        input_file_right = inputs[1]
        col_id_right = j['parameters']['col_id_right']
        col_text_right = j['parameters']['col_text_right']        
        separator_right = j['parameters']['separator_right']                
        output_file = j['parameters']['output_file']
        k = j['parameters']['k']
        delta_alg = j['parameters']['delta_alg']
        minio = j['minio']
        
        df_left = prep_df(input_file_left, col_text_left, separator_left, minio)
        df_right = prep_df(input_file_right, col_text_right, separator_right, minio)
        
        pairs, log = JaccardTokenJoin().tokenjoin_foreign(df_left, df_right, str(col_id_left), str(col_id_right),
                                                          str(col_text_left), str(col_text_right),
                                                          k=k, delta_alg=delta_alg, keepLog=True)
        pairs.to_csv(output_file)

        # basename = output_file.split('/')[-1]
        basename = str(uuid.uuid4()) + "." + output_file.split('.')[-1]
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        result = client.fput_object(minio['bucket'], basename, output_file)
        object_path = f"s3://{result.bucket_name}/{result.object_name}"

        return json.dumps({'message': 'TokenJoin project executed successfully!',
                           'output': [object_path], 'metrics': log, 'status': 200})
    except Exception as e:
        return json.dumps({
            'message': 'An error occurred during data processing.',
            'error': str(e),
            'status': 500
        })
    
async def asynchronous_wrapper(j):
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, synchronous_entity_linking, j)
    return response
    
async def echo(websocket, path):
    async for message in websocket:
        j = json.loads(message)
        
        response = await asynchronous_wrapper(j)
        await websocket.send(response)    
    
async def main(port):
    async with websockets.serve(echo, None, port):
        await asyncio.Future()  # Serve forever    

if __name__ == '__main__':
    asyncio.run(main(int(sys.argv[1])))

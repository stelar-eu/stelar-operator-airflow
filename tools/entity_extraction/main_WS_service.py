import pandas as pd
from minio import Minio
from entity_extraction import entity_extraction
from backend_functions import make_df_by_argument
from llm_foodNER_functions import prepare_dataset, prepare_dataset_ak
import json
import uuid
import asyncio
import websockets
from time import time
import sys

def synchronous_entity_extraction(j):
    try:
        outfile, log = "", {}
        inputs = j['input']
        INPUT_FILE = inputs[0]
        # File Parameters
        output_file = j['parameters']['output_file']
        text_column = j['parameters']['text_column']
        ground_truth_column = j['parameters'].get('ground_truth_column')
        csv_delimiter = j['parameters']['csv_delimiter']
        
        #Algorithm Parameters
        N = j['parameters']['N']
        extraction_type = j['parameters']['extraction_type']
        model = j['parameters']['model']
        syntactic_analysis_tool = j['parameters'].get('syntactic_analysis_tool')
        if syntactic_analysis_tool is None:
            syntactic_analysis_tool = 'stanza'
        prompt_id = j['parameters'].get('prompt_id')
        if prompt_id is None:
            prompt_id = 0
        minio = j['minio']
        
        if extraction_type == 'generic':
            df = make_df_by_argument(INPUT_FILE, text_column = text_column, 
                                     ground_truth_column = ground_truth_column,
                                     csv_delimiter = csv_delimiter, minio=minio)
            
        elif extraction_type == 'food':
            if 'incidents' in INPUT_FILE:
                df = prepare_dataset_ak(INPUT_FILE, text_column = text_column,
                                        preprocess = False, minio=minio)
            else:
                df = prepare_dataset(INPUT_FILE, text_column = text_column,
                                          ground_truth_column = ground_truth_column, minio=minio) 

        # CHANGE HERE : START
        t = time()
        outfile, log = entity_extraction(df, extraction_type, model, 
                                         output_file=output_file, N = N,
                                         syntactic_analysis_tool = syntactic_analysis_tool, 
                                         prompt_id = prompt_id)
        t = time() - t
        # log = {k: float(v[:-1]) for k, v in log['score'].items()}
        
        # basename = outfile.split('/')[-1]
        # basename = str(uuid.uuid4()) + "." + outfile.split('.')[-1]
        # client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        # result = client.fput_object(minio['bucket'], basename, outfile)
        # object_path = f"s3://{result.bucket_name}/{result.object_name}"
        
        # Keep only FOOD Tags
        outfile2 = outfile.replace('.csv', '_food.csv')
        df = pd.read_csv(outfile)
        df_stats = pd.DataFrame()
        df_stats['total_tags'] = df.groupby('text_id')['phrase_id'].count()
        df = df.loc[df.tag=='FOOD']
        df_stats['food_tags'] = df.groupby('text_id')['phrase_id'].count()
        log = df_stats.mean().to_dict()
        log['execution_time'] = t
        df.to_csv(outfile2, index=False, header=True)
        
        # basename = outfile2.split('/')[-1]
        basename = str(uuid.uuid4()) + "." + outfile2.split('.')[-1]
        client = Minio(minio['endpoint_url'], access_key=minio['id'], secret_key=minio['key'])
        result = client.fput_object(minio['bucket'], basename, outfile2)
        object_path2 = f"s3://{result.bucket_name}/{result.object_name}"

        return json.dumps({'message': 'Entity Extraction executed successfully!',
                           # 'output': [object_path, object_path2], 'metrics': log, 'status': 200})
                           'output': [object_path2], 'metrics': log, 'status': 200})
    except Exception as e:
        return json.dumps({
            'message': 'An error occurred during data processing.',
            'error': str(e),
            'status': 500
        })


async def asynchronous_wrapper(j):
    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, synchronous_entity_extraction, j)
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

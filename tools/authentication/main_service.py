from flask import Flask, request, jsonify
import json

app = Flask(__name__)

@app.route('/', methods=['POST'])
def run_function():
    # Add your custom logic here
    try:
        users = json.load(open('users.json'))
        j = request.json
        
        for field in ['username', 'password']:
            if field not in j:
                return jsonify({
                    'message': f'{field.capitalize()} is missing',
                    }), 500
        
        username = j['username']
        password = j['password']
        if username not in users:
            return jsonify({
                'message': 'The user is not registered.',
            }), 500
        
        user = users[username]
        
        if user['password'] != password:
            return jsonify({
                'message': 'The password is not correct.',
            }), 500
        

        return jsonify({'message': 'User is correctly authenticated',
                        'CKAN_Token': user['CKAN_Token'], 
                        'minio_id': user['minio_id'],
                        'minio_key': user['minio_key'],
                        }), 200
    except Exception as e:
        return jsonify({
            'message': 'An error occurred.',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9072)

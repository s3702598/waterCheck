import json
import boto3
import botocore
import requests
import secrets
from decimal import Decimal
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
from statistics import median
from flask import Flask, render_template, request, redirect, url_for, session, jsonify

app = Flask(__name__, static_url_path='/static')
app.secret_key = secrets.token_hex(16)

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
dynamodb_session = boto3.session.Session(region_name='us-east-1', profile_name='default')
client = dynamodb_session.client('dynamodb')
toronto_table = dynamodb.Table('toronto')
toronto_rain_table = dynamodb.Table('toronto-rain')
toronto_day_before_p = dynamodb.Table('toronto_day_before_p')


@app.route('/upload', methods=['POST'])
def upload_file():
    # get the file from the request
    file = request.files['file']
    # save the file to S3 bucket
    s3.upload_fileobj(file, 'watercheck-storage-bucket', file.filename)
    return 'File uploaded successfully'

# function that retrieves file with water quality checks
def get_s3_data():
    try:
        obj = s3.Object('watercheck-storage-bucket', 'TorontoBeachesData.json')
        data = obj.get()['Body'].read().decode('utf-8')
        return json.loads(data)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print('Error: The object does not exist.')
        else:
            print(e.response)
        return None
    
# function that puts data from water quality file into table
def put_dynamodb_data(beach_name, date):
    try:
        response = toronto_table.put_item(
            Item={
                'beach_name': beach_name,
                'date': date
            }
        )
        return True
    except ClientError as e:
        print(f'Error putting item to DynamoDB: {e}')
        return False


# Route to place beach name and closure dates data in a table
@app.route('/add_to_beach_table')
def process_data():
    data = get_s3_data()
    if data is None:
            return 'Error retrieving data from S3 bucket'
    for entry in data:
        if entry['data'] is None:
            continue
        collection_date = entry['CollectionDate']
        for beach in entry['data']:
            beach_name = beach['beachName']
            if beach['statusFlag'] == 'UNSAFE':
                put_dynamodb_data(beach_name, collection_date)
    return 'Data processed successfully'

    
def get_beach_names():
    response = toronto_table.scan(ProjectionExpression='beach_name')
    items = response['Items']
    beach_names = list(set(item['beach_name'] for item in items))
    return beach_names

# display beach names from the water quality document
@app.route('/beach_names')
def beach_names():
    beach_names = get_beach_names()
    return render_template('beach_names.html', beach_names=beach_names)

# get precipitaion information from an API
def get_precipitation_from_api(url):
    
    response = requests.get(url)
    
    # Process API response
    data = response.json()['daily']
    for i in range(len(data['time'])):
        date = data['time'][i]
        precipitation = Decimal(str(data['precipitation_sum'][i]))
    
        # Save data to DynamoDB
        item = {
            'date': date,
            'precipitation': precipitation
        }
        
        try:
            # Save data to DynamoDB
            toronto_rain_table.put_item(Item={
                'precipitation': precipitation,
                'date': date
            })
            print(f"Saved item: {item}")
        except Exception as e:
            print(f"Error saving item {item}: {e}")
        
    return "Data saved successfully."


@app.route('/precipitation', methods=['GET', 'POST'])
def precipitation():
    if request.method == 'POST':
        url = request.form['url']
        try:
            get_precipitation_from_api(url)
            print("Data saved successfully.")
            return "Data saved successfully."
        except Exception as e:
            print("Error: {}".format(e))
            return f"Error: {e}"
    

def get_precipitation(date_str):
    # Get precipitation for a given date
    try:
        response = toronto_rain_table.query(
            KeyConditionExpression=Key('date').eq(date_str)
        )
        items = response['Items']
        if items:
            return items[0]['precipitation']
        else:
            return None
    except toronto_rain_table.meta.client.exceptions.ResourceNotFoundException:
        return None
    
  
# function that combines data from beach quality and precipitation tables
def put_rain_data(table_name,beach_name, date, precipitation):
    try:
        table_name.put_item(
            Item={
                'beach_name': beach_name,
                'date' : date,
                'precipitation': precipitation
            }
        )
        return True
    except ClientError as e:
        print(f'Error putting item to DynamoDB: {e}')
        return False  
    
@app.route('/process_date_before')
def process_dates():
    # Query all dates in toronto table
    response = toronto_table.scan()
    for item in response['Items']:
        date_str = item['date']
        beach_name = item['beach_name']
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        day_before_obj = date_obj - timedelta(days=1)
        day_before_str = day_before_obj.strftime('%Y-%m-%d')
        precipitation = get_precipitation(day_before_str)
        put_rain_data(toronto_day_before_p,beach_name, day_before_str, precipitation)
                
    return "Processed all dates"

        
@app.route('/check_table')
def checkTableSchema():
    dynamodb = boto3.client('dynamodb')
    table_name = 'toronto-rain'

    response = dynamodb.describe_table(
        TableName=table_name
    )

    print(response['Table']['KeySchema'])
    return (response['Table']['KeySchema'])

# Calulate 90% precentile of precipitaion for each beach and add to table
@app.route('/get_90_precentile')
def get90Precentile():
    beach_avg_precipitation = {}
    table = dynamodb.Table('beach-precipitation')
    
    # Get precipitation for each beach
    response = toronto_day_before_p.scan(ProjectionExpression='beach_name,precipitation')
    items = response['Items']
    while response.get('LastEvaluatedKey'):
        response = toronto_day_before_p.scan(
            ProjectionExpression='beach_name,precipitation',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    for item in items:
        beach_name = item['beach_name']
        precipitation = Decimal(str(item['precipitation']))
        if beach_name not in beach_avg_precipitation:
            beach_avg_precipitation[beach_name] = []
        beach_avg_precipitation[beach_name].append(precipitation)

    for beach_name, precipitation_list in beach_avg_precipitation.items():
        non_zero_precipitation_list = [p for p in precipitation_list if p != 0]  # filter out 0 values
        non_zero_precipitation_list.sort()
        avg_90th_percentile_precipitation = (sum(non_zero_precipitation_list[int(len(non_zero_precipitation_list)*0.1)
                                                                             :int(len(non_zero_precipitation_list)*0.9)]) / 
                                             len(non_zero_precipitation_list[int(len(non_zero_precipitation_list)*0.1)
                                                                             :int(len(non_zero_precipitation_list)*0.9)]))
        avg_90th_percentile_precipitation = round(avg_90th_percentile_precipitation, 2)

        # Save the beach name and precipitation information to DynamoDB
        response = table.put_item(
            Item={
                'beach_name': beach_name,
                'precipitation': avg_90th_percentile_precipitation
            }
        )
    return "Beach names and precipitation saved to DynamoDB table"

@app.route('/forecast')
def getForecast():
    table = dynamodb.Table('toronto-rain-forecast')

    # Fetch data from API
    url = 'https://api.open-meteo.com/v1/forecast?latitude=43.70&longitude=-79.42&daily=precipitation_sum&forecast_days=14&timezone=America%2FNew_York'
    response = requests.get(url)
    data = response.json()

    # Extract relevant data and save to DynamoDB
    for i, date_str in enumerate(data['daily']['time']):
        date = datetime.fromisoformat(date_str).date()
        precipitation = Decimal(str(data['daily']['precipitation_sum'][i]))
        item = {'date': str(date), 'precipitation': precipitation}
        table.put_item(Item=item)
        
    return "Forecast saved to DynamoDB table"

def get_beach_status():
    # Fetch data from toronto-rain-forecast table
    forecast_table = dynamodb.Table('toronto-rain-forecast')
    response = forecast_table.scan()
    forecast_data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = forecast_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        forecast_data.extend(response['Items'])

    # Fetch data from beach-precipitation table
    beach_table = dynamodb.Table('beach-precipitation')
    response = beach_table.scan()
    beach_data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = beach_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        beach_data.extend(response['Items'])

    # Compare data and create result dictionary
    results = {}
    for beach in beach_data:
        beach_name = beach['beach_name']
        precipitation_threshold = Decimal(str(beach['precipitation']))
        results[beach_name] = {}
        for forecast in forecast_data:
            forecast_date = forecast['date']
            forecast_precipitation = Decimal(str(forecast['precipitation']))
            status = "SAFE" if forecast_precipitation < precipitation_threshold else "UNSAFE"
            if forecast_date in results[beach_name]:
                current_status = results[beach_name][forecast_date][-1]  # Get the current status
                if current_status != status:  # Check if the status has changed
                    results[beach_name][forecast_date] = [status] 
            else:
                results[beach_name][forecast_date] = [status]
        
            
    return results

@app.route('/get_beach_status' , methods=['GET'])
def get_beach_status_route():
    beach_status = get_beach_status()
    print(beach_status)
    return jsonify(beach_status)

@app.route('/login', methods=['GET', 'POST'])
def login():
    error_message = None
    if request.method == 'POST':
        email = request.form['email'].lower()
        password = request.form['password']

        table = dynamodb.Table('users')
        try:
            user = table.query(
                KeyConditionExpression=Key('email').eq(email)
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return "Resource not found", 404
            else:
                error_message = e.response['Error']['Message']
                return render_template('login.html', message=error_message)

        if user['Count'] == 0:
            return render_template('login.html', message='Email or password is invalid.')
        
        user_password = user['Items'][0].get('password')
        if not user_password:
            return render_template('login.html', message='Email or password is invalid.')
        
        if user_password == password:
            # Check if the user exists in DynamoDB and get their attributes
            dynamodb_user = client.get_item(
                TableName='users',
                Key={
                    'email': {'S': email}
                }
            )
            if 'Item' not in dynamodb_user:
                message = 'Email or password is invalid.'
                return render_template('login.html', message=message)
            else:
                session['user_email'] = email
                session['username'] = dynamodb_user['Item']['user_name']['S']
                return redirect(url_for('main'))
        else:
            message = 'Email or password is invalid'
            return render_template('login.html', message=message)
    else:
        return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        username = request.form['username']
        password = request.form['password']

        table = dynamodb.Table('users')
        user = table.query(
            KeyConditionExpression=Key('email').eq(email)
        )
        if user['Count'] == 1:
            message = 'The email already exists'
            return render_template('register.html', message=message)
        else:
            table.put_item(
                Item={
                    'email': email,
                    'user_name': username,
                    'password': password
                }
            )
            session['user_email'] = email
            session['username'] = username
            return redirect(url_for('login'))
    else:
        return render_template('register.html')
    
# Define user route
@app.route('/user', methods=['GET', 'POST'])
def user():
    if 'user_email' not in session:
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        # Handle change password form submission
        old_password = request.form['old_password']
        new_password = request.form['new_password']
        confirm_password = request.form['confirm_password']

        table = dynamodb.Table('users')
        email = session['user_email']
        user = table.get_item(Key={'email': email})
        user_password = user['Item'].get('password')

        if user_password != old_password:
            message = 'The old password is incorrect'
            return render_template('user.html', message=message)

        if new_password != confirm_password:
            message = 'The new password and confirm password do not match'
            return render_template('user.html', message=message)

        table.update_item(
            Key={'email': email},
            UpdateExpression='SET #password = :new_password',
            ExpressionAttributeNames={'#password': 'password'},
            ExpressionAttributeValues={':new_password': new_password}
        )

        message = 'Password changed successfully'
        return render_template('user.html', message=message)

    else:
        # Render change password form
        return render_template('user.html')
    
@app.route('/logout')
def logout():
    session.pop('user_email', None)
    session.pop('user_name', None)
    return redirect(url_for('main'))

# Define the main route
@app.route('/', methods=['GET', 'POST'])
def main():
    if 'user_email' not in session:
        # User is not logged in, redirect to the login page
        logged_in = False
    else:
        logged_in = True
        
    # Code to show prognosis
    status_dict = get_beach_status()
    status_list = []
    for beach_name, beach_data in status_dict.items():
        for forecast_date, status in beach_data.items():
            date_obj = datetime.strptime(forecast_date, '%Y-%m-%d').date()
            tomorrow = datetime.now().date() + timedelta(days=1)
            if date_obj >= tomorrow:
                if 'SAFE' in status:
                    status_list.append([beach_name, forecast_date, 'SAFE'])
                else:
                    status_list.append([beach_name, forecast_date, 'UNSAFE'])
    sorted_status_list = sorted(status_list, key=lambda x: x[1])    
        
    return render_template('main.html', logged_in=logged_in,  
                           status_list=sorted_status_list,
                           beach_names=beach_names)   
    
@app.route('/add_favorite_beach', methods=['POST'])
def add_favorite_beach():
    if 'user_email' not in session:
        return redirect(url_for('login'))
    
    beach_names = get_beach_names()

    beach_name = request.form['beach_name']

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('favorite_beaches')
    table.put_item(Item={
        'email': session['user_email'],
        'beach_name': beach_name
    })

    return redirect(url_for('user'))
    
    
# Lambda function to get weather forecast
tableForForecast = boto3.resource('dynamodb').Table('toronto-rain-forecast')

def getForecast(event, context):
    # Fetch data from API
    url = 'https://api.open-meteo.com/v1/forecast?latitude=43.70&longitude=-79.42&daily=precipitation_sum&forecast_days=14&timezone=America%2FNew_York'
    response = requests.get(url)
    data = response.json()

    # Extract relevant data and save to DynamoDB
    for i, date_str in enumerate(data['daily']['time']):
        date = datetime.fromisoformat(date_str).date()
        precipitation = Decimal(str(data['daily']['precipitation_sum'][i]))
        item = {'date': str(date), 'precipitation': precipitation}
        tableForForecast.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': json.dumps('Forecast saved to DynamoDB table')
    }


if __name__ == "__main__":
	app.run()

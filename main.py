import json
import boto3
import requests

kinesis = boto3.client('kinesis')

def main():
    
    url = "https://randomuser.me/api/?inc=gender,name,location,dob&results=10"
    json_data = requests.get(url).json()
    
    # print(json.dumps(json.loads(data).get("results")))
    # print(data)
    response = kinesis.put_record(
        Data=json.dumps(json_data.get("results")),
        PartitionKey="x",
        StreamARN='arn:aws:kinesis:us-east-1:309168395754:stream/stream'
    )
    print(json_data.get("results"))
    print(response)

if __name__ == "__main__":
    main()

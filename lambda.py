import json
import base64

def parse_data(data):
    return {
        "first": data["name"]["first"],
        "last": data["name"]["last"],
        "age": data["dob"]["age"],
        "gender": data["gender"],
        "lat":data["location"]["coordinates"]["latitude"],
        "lon":data["location"]["coordinates"]["longitude"]
    }
    
def lambda_handler(event, context):
    assert "records" in event
    response=[]
    
    for record in event["records"]:
        assert "data" in record  
        data_encoded = base64.b64decode(record.get("data","")).decode('utf-8')
        data = json.loads(data_encoded)
        print(f"Data: {data}")
        data_response=[]
        if len(data) > 0:
            for d in data:
                if d["dob"]["age"] >= 21:
                    data_response.append(parse_data(d))
            res = json.dumps(data_response)+"\n"
            response.append({
                "data": base64.b64encode(res.encode('utf-8')).decode("utf-8", "ignore"),
                "result": "Ok",
                "recordId": record.get("recordId")
            }) 
        else:
            response.append({
                "data": record.get("data"),
                "result": "Dropped",
                "recordId": record.get("recordId")
            })         
        print(f"Final: {data_response}")
    
    print(f"Response: {response}")
    return {"records": response}

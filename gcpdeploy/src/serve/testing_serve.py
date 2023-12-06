import requests
import json

health_check_url = 'http://127.0.0.1:8080/ping'

response_health = requests.get(health_check_url)
health_status = response_health.json()

print(health_status)


predict_url  = 'http://127.0.0.1:8080/predict'

data = {
  "instances": [
    {
      "PC1": 1000.595596,
      "PC2": -0.944713,
      "PC3": 0.340492,
      "PC4": 1.335999,
      "PC5": 0.135310,
      "PC6": 0.506377
    }
  ]
}


response_predict = requests.post(predict_url, json=data)
predict_status = response_predict.json()

print(predict_status)
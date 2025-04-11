from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='3.95.137.41:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

account_numbers = list(range(1, 10000)) 
banks = list(range(1, 301))  

while True:
    transaction_id = str(uuid.uuid4()) 
    account_no = random.choice(account_numbers)
    transaction_timestamp = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
    amount = round(random.uniform(10, 5500), 2)
    if random.random() > 0.5:  
        amount = -amount  

    balance_amt = round(random.uniform(5000, 200000), 2)  
    bank_id = random.choice(banks)  

    transaction = {
        "transaction_id": transaction_id,
        "account_no": account_no,
        "transaction_timestamp": transaction_timestamp,
        "amount": amount,
        "balance_amt": balance_amt,
        "bank_id": bank_id
    }
 
    producer.send('bank-transactions', transaction)
    print(f"Sent: {transaction}")

    time.sleep(3) 

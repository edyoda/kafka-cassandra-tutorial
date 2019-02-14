import threading, logging, time
import multiprocessing

import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

class ExampleModel(Model):
    example_id      = columns.UUID(primary_key=True, default=uuid.uuid4)
    example_type    = columns.Integer(index=True)
    created_at      = columns.DateTime()
    description     = columns.Text(index=True)



from kafka import KafkaConsumer, KafkaProducer


class Consumer():
    def __init__(self):
        multiprocessing.Process.__init__(self)
        connection.setup(['127.0.0.1'], "emp1", protocol_version=3)
        sync_table(ExampleModel)
        print ('init done')

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['my-topic'])

        for message in consumer:
            print(message.value)
            em = ExampleModel.create(example_type=0, description=str(message.value), created_at=datetime.now())
            print ('****abc',em)


c = Consumer()
c.run()




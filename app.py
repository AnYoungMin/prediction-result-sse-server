"""

"""

import json
import flask
from kafka import KafkaConsumer, TopicPartition
from flask_caching import Cache
import os


broker = os.environ.get("KAFKA_BOOTSTRAP_SERVER")
topic = os.environ.get("PREDICTION_RESULT_TOPIC")
if broker is None or topic is None:
    print("You must give os.environment ['KAFKA_BOOTSTRAP_SERVER', 'PREDICTION_RESULT_TOPIC'")
    exit(1)
host = os.environ.get("ADVERTISED_HOST", "0.0.0.0")
port = os.environ.get("PORT", "5000")
cache_timeout = int(os.environ.get("CACHE_TIMEOUT", "86400"))

app = flask.Flask(__name__)
config = {
    "CACHE_TYPE" : "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT" : cache_timeout
}
app.config.from_mapping(config)
cache = Cache(app)


def event_stream():
    print("new sse connection")
    consumer = KafkaConsumer(bootstrap_servers=[broker],
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    consumer.assign([TopicPartition(topic, 0)])
    consumer.assignment()
    for message in consumer:
        print(message.value)
        cache.set(message.value['y_hat']['variables'][0]['id'], message.value)  # todo: value format
        yield f"data: {json.dumps(message.value).encode('utf-8')}\n\n"


@app.route('/prediction/stream')
def stream():
    return flask.Response(event_stream(),
                          headers={'Access-Control-Allow-Origin': '*'},
                          mimetype="text/event-stream")


@app.route('/prediction/<sensor_id>/latest')
def prediction(sensor_id):
    cached_prediction = cache.get(sensor_id)  # todo: if not?
    if cached_prediction is None:
        flask.abort(404)
    return flask.jsonify(cached_prediction)


if __name__ == '__main__':
    app.debug = True
    app.run(host=host, port=port)

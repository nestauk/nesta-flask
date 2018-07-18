# FAILS if model_name does not exist
# FAILS if query not given
# FAILS if any other args given (https://webargs.readthedocs.io/en/latest/api.html#webargs.core.Parser.use_args)

from flask import Flask
from flask import jsonify
from webargs import fields
from webargs.flaskparser import use_args
import pickle
import boto3
import botocore
import io

# Module scope resources
app = Flask(__name__)
s3 = boto3.resource('s3')


class dummy_model:
    def query(self, q):
        return "Query had length {}".format(len(q))


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/<string:model_name>', methods=['GET'])
@use_args(argmap={'q': fields.Str(required=True)}, as_kwargs=True)
def process_query(q, model_name):
    # Check whether the model exists
    file_name = '{}.pkl'.format(model_name)
    try:
        s3.Object('clio-models', file_name).load()
    except botocore.exceptions.ClientError as e:
        raise InvalidUsage('No model named "{}"'.format(model_name),
                           status_code=400)

    # Retrieve the model
    with io.BytesIO() as data:
        s3.Bucket('clio-models').download_fileobj(file_name, data)
        data.seek(0)  # move back to the beginning after writing
        model = pickle.load(data)
        results = model.query(q)

    # Done
    return jsonify({'model_name': model_name,
                    'query': q,
                    'results': results})


if __name__ == '__main__':
    app.run(debug=True)

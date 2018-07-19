from flask import Flask
from flask import jsonify
from webargs import fields
from webargs.flaskparser import use_args
import pickle
import boto3
import botocore
import io
from dummy_module import dummy_model


# Module scope resources
application = Flask(__name__)
s3 = boto3.resource('s3')


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


@application.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@application.route('/<string:model_name>', methods=['GET'])
@use_args(argmap={'q': fields.Str(required=True)}, as_kwargs=True)
def process_query(q, model_name):
    # Check whether the model exists
    file_name = '{}.pkl'.format(model_name)
    try:
        s3.Object('clio-models', file_name).load()
    except botocore.exceptions.ClientError as e:
        err_msg = 'No model named "{}"'.format(model_name)
        raise InvalidUsage(err_msg, status_code=400)

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
    application.run()

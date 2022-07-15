import flask
from gear.project import  python_test 
from flask_restful import Api
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask_apispec.extension import FlaskApiSpec
from flask_cors import CORS
    
app = flask.Flask(__name__)
CORS(app)
api = Api(app)


app.config["DEBUG"] = True # 啟動Debug模式

app.config.update({
    'APISPEC_SPEC': APISpec(
        title='Python考題',
        version='v777', 
        plugins=[MarshmallowPlugin()],
        openapi_version='2.0.0'
    ),
    'APISPEC_SWAGGER_URL': '/swagger/',  # URI to access API Doc JSON
    'APISPEC_SWAGGER_UI_URL': '/swagger-ui/' , # URI to access UI of API Doc
})
docs = FlaskApiSpec(app)

api.add_resource(python_test, '/')
docs.register(python_test)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000) #8000 port
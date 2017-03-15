from flask import Flask, request

app = Flask(__name__)



@app.route('/')
def hello():
    print(request.remote_addr)
    return 'HELLO FLASK!'


app.run(host='0.0.0.0')
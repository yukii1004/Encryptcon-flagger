import flask
from flask import Flask, render_template, url_for, redirect, session, request

app = Flask(__name__, static_folder='static', template_folder='templates')
app.secret_key ='secret_key'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/register')
def register():
    return render_template('register.html')

if __name__ == '__main__':
    app.run(debug=True)
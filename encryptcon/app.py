from flask import Flask, render_template, jsonify
import mysql.connector
import database

app = Flask(__name__)

# Establish MySQL connection
conn = database.mysql.connector.connect(host = "localhost",user = "root",password = "root",database = "encryptcon")
cursor = conn.cursor(dictionary=True) 

@app.route('/api/home',methods=['GET'])
def index():
    query = "SELECT * FROM account"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    return jsonify({'index': data})

@app.route('/api/users',methods=['GET'])
def users():
    query = "SELECT * FROM transactions"
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    return jsonify({'users': data})

if __name__ == '__main__':
    app.run(debug=True)
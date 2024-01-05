from flask import Flask, render_template, jsonify
import mysql.connector

app = Flask(__name__)

# MySQL Configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'encrypton'
}

# Establish MySQL connection
conn = mysql.connector.connect(**mysql_config)
cursor = conn.cursor(dictionary=True) 

@app.route('/api/home',methods=['GET'])
def index():
    query = "SELECT * FROM account"
    cursor.execute(query)
    data = cursor.fetchall()
    return jsonify({'index': data})

@app.route('/api/users',methods=['GET'])
def users():
    query = "SELECT * FROM transactions"
    cursor.execute(query)
    data = cursor.fetchall()
    return jsonify({'users': data})

if __name__ == '__main__':
    app.run(debug=True)
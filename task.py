from functions import SQLInterface
from flask import Flask, render_template, jsonify

s = SQLInterface()

app = Flask(__name__)

#main page of API with data query 
@app.route("/", methods= ["GET"])
def choose_files():
    return render_template("data_query.html")

#page shows all suitable files for our chosen conditions 
@app.route("/submit", methods = ['POST', 'GET'])
def submit():
    try:
        s.submitted_data_query()
        return jsonify({"filtered_files": s.filtered_files})
    
    except Exception as e:
        return jsonify({"nothing to show"}), 500

if __name__ == '__main__':
    s.get_data_and_upload_everything_to_minio_and_postgres()
    app.run()
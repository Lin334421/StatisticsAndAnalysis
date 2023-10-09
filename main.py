# -*-coding:utf-8-*-
from flask import Flask

from CkHouse.ck_house import ck_bp
from person_contribution.person_contributions import person_bp
from repo.repo_contributions import repo_bp

app = Flask(__name__)

app.register_blueprint(person_bp, url_prefix='/person_contribution')
app.register_blueprint(ck_bp, url_prefix='/clickhouse')
app.register_blueprint(repo_bp, url_prefix='/repo_contributions')

if __name__ == '__main__':
    # app.run(host='192.168.8.110',debug=True)
    app.run(host='0.0.0.0', debug=True)
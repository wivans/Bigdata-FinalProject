from flask import Blueprint, Flask, render_template, request

main = Blueprint('main', __name__)

import json
from engine import ClusteringEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@main.route("/AppleStore/model<int:model_numb>", methods=["POST"])
def app_store(model_numb):
    
    user_rating_fetched = request.form.get('user_rating')
    user_rating_ver_fetched = request.form.get('user_rating_ver')

    
    cluster_location = clustering_engine.app_store(user_rating_fetched, user_rating_ver_fetched, model_numb)
    return json.dumps(cluster_location)

def create_app(spark_session, dataset_path):
    global clustering_engine

    clustering_engine = ClusteringEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
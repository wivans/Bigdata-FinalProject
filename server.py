import time, sys, cherrypy, os
import findspark
import logging
findspark.init()
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Python Spark Clustering").getOrCreate()

def run_server(app):

    app_logged = TransLogger(app)


    cherrypy.tree.graft(app_logged, '/')


    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })


    cherrypy.engine.start()
    print("Engine Started !")
    cherrypy.engine.block()


if __name__ == "__main__":

    enginepyloc = os.path.join(os.getcwd(), "engine.py")
    apppyloc = os.path.join(os.getcwd(), "app.py")
    spark.sparkContext.addPyFile(enginepyloc)
    spark.sparkContext.addPyFile(apppyloc)


    dataset_path = os.path.join(os.getcwd(), 'dataset-kafka')
    app = create_app(spark, dataset_path)


    run_server(app)


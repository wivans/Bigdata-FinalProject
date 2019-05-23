import os
import logging
import pandas as pd
# from pyspark.sql import Row
# from pyspark.sql import types
# from pyspark.sql.functions import explode
# import pyspark.sql.functions as func
from sklearn.preprocessing import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transformDF(dataframe):
    dataframe = dataframe.withColumn("user_rating", dataframe["user_rating"].cast("double"))
    dataframe = dataframe.withColumn("user_rating_ver", dataframe["user_rating_ver"].cast("double"))
    assembler = VectorAssembler(
        inputCols=["user_rating", "user_rating_ver"],
        outputCol='features')
    dataframe = assembler.transform(dataframe)
    return dataframe

class ClusteringEngine:
    """An Apple Store clustering engine
    """

    def __train_model(self):
        """Train the model with the current dataset
        """

        """Train the model with the current dataset
        """
        logger.info("Splitting dataset into 2 model...")
        # Model 1: 1/2 data pertama.
        # Model 2: semua data (1/2 data pertama + 1/2 data kedua).
        self.df1 = self.dforiginal.limit(int(self.dataset_count / 2))
        self.df2 = self.dforiginal
        print('df 1 count = ' + str(self.df1.count()))
        print('df 2 count = ' + str(self.df2.count()))
        logger.info("Dataset Splitted !")

        logger.info("Training model 1...")
        kmeans_1 = KMeans().setK(3).setSeed(1)
        model_1 = kmeans_1.fit(self.dforiginal)
        self.predictions_1 = model_1.transform(self.dforiginal)
        logger.info("Model 1 built!")
        logger.info("Evaluating the model 1...")
        evaluator_1 = ClusteringEvaluator()
        silhouette_1 = evaluator_1.evaluate(self.predictions_1)
        logger.info("Silhouette with squared euclidean distance = " + str(silhouette_1))
        self.centers_1 = model_1.clusterCenters()
        logger.info("Model 1 Done !")


        logger.info("Training model 2...")
        kmeans_2 = KMeans().setK(4).setSeed(1)
        model_2 = kmeans_2.fit(self.dforiginal)
        self.predictions_2 = model_2.transform(self.dforiginal)
        logger.info("Model 2 built!")
        logger.info("Evaluating the model 2...")
        evaluator_2 = ClusteringEvaluator()
        silhouette_2 = evaluator_2.evaluate(self.predictions_2)
        logger.info("Silhouette with squared euclidean distance = " + str(silhouette_2))
        self.centers_2 = model_2.clusterCenters()
        logger.info("Model 2 Done !")

    def app_store(self, user_rating_fetched, user_rating_ver_fetched, model_numb):
        
        distance = []
        if model_numb == 1:
            center_varname = self.centers_1
        elif model_numb == 2:
            center_varname = self.centers_2
        for center in center_varname:
            distance.append((pow((float(center[0]) - float( user_rating_fetched)), 2)) + (pow((float(center[1]) - float(user_rating_ver_fetched)), 2)))
        cluster = distance.index(min(distance))
        return cluster

    def __init__(self, spark_session, dataset_folder_path):
        """Init the clustering engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Clustering Engine: ")
        self.spark_session = spark_session
        logger.info("Loading Data...")
        filecount = 0
        while True:
            file_name = 'result' + str(filecount) + '.txt'
            dataset_file_path = os.path.join(dataset_folder_path, file_name)
            exist = os.path.isfile(dataset_file_path)
            #self.dforiginal = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
            if exist:
                if filecount == 0:
                    self.dforiginal = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
                else:
                    dfnew = spark_session.read.csv(dataset_file_path, header=None, inferSchema=True)
                    self.dforiginal = self.dforiginal.union(dfnew)
                self.dataset_count = self.dforiginal.count()
                print('dataset loaded = ' + str(self.dataset_count))
                print(file_name + 'Loaded !')
                filecount += 1
            else:
                break

        self.dforiginal = self.dforiginal.selectExpr("_c1 as id", "_c2 as track_name", "_c3 as size_bytes", "_c4 as currency", "_c5 as price", "_c6 as rating_count_tot",\
         "_c7 as rating_count_ver",  "_c8 as user_rating",  "_c9 as user_rating_ver",  "_c10 as ver", "_c11 as cont_rating", "_c12 as prime_genre",\
          "_c13 as sup_device_num", "_c14 as ipadSc_urls_num", "_c15 as lang_num", "_c16 as vpp_lic")
        self.dforiginal.show()
        #print(self.dforiginal.count())
        self.dforiginal = transformDF(self.dforiginal)
        self.__train_model()
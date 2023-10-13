from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
import os

#изменить путь перед запуском
hadoopFilesPath = r"C:/winutil"
os.environ["HADOOP_HOME"] = hadoopFilesPath
os.environ["hadoop.home.dir"] = hadoopFilesPath
os.environ["PATH"] = os.environ["PATH"] + f";{hadoopFilesPath}\\bin"

#создание сессии 
spark = SparkSession.builder.master("local").appName("lab_spark").getOrCreate()

#данные
products = [["1", "Первый продукт"],
            ["2", "Второй продукт"],
            ["3", "Второй продукт"]]
products_colums = ["ID", "Title"]

categories = [["1", "Первая категория"],
              ["2", "Вторая категория"],
              ["3", "Третья категория"],
              ["4", "Четвертая категория"]]
categories_colums = ["ID", "Title"]

productscategories = [["1", "1", "1"],
                      ["2", "1", "2"],
                      ["3", "2", "3"]
                      ]
productscategories_colums = ["ID", "ProductID", "CategoriesID"]

#создание DataFrame
p = spark.createDataFrame(products, products_colums) 
c = spark.createDataFrame(categories, categories_colums)
pc = spark.createDataFrame(productscategories, productscategories_colums)

p.createOrReplaceTempView("Products")
c.createOrReplaceTempView("Categories")
pc.createOrReplaceTempView("ProductsCategories")  
p.show() 
c.show()
pc.show()

#метод для получения данных
def makeRequest():
    result_df = p.join(pc, (p.ID == pc.ProductID), "outer").join(c, (c.ID == pc.CategoriesID), "left").select(p.ID, p.Title, c.Title)
    result_df.show()

makeRequest()

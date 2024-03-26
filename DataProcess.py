#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, sum as _sum, when
from pyspark.sql.window import Window


'''
* @author  chenyingyan
'''
class DataProcess:
    def __init__(self, spark):
        self.spark = spark

    def filter_data(self, df):
        # filter peer_id contains id_2 ，then output column
        result = df.filter(col("peer_id").contains(col("id_2"))).select("peer_id", "id_1","id_2","year")
        return result

    def flatmap_aggr(self, df, df_pre):
        result = df.alias("a")\
        .join(df_pre.alias("b"), col("a.peer_id") == col("b.peer_id"), 'left')\
        .filter(col("a.year") <= col("b.year"))\
        .groupBy("a.peer_id","a.year")\
        .count()\
        .orderBy("peer_id", "year", ascending=False)
        return result

    
    def accu_find(self, df, size):
        # accu window
        window = Window.partitionBy("peer_id").orderBy(col("year").desc())
        
        
        group_df = df.withColumn("rn", row_number().over(window))
        
        # Calculate cumulative count
        accu_df = group_df.withColumn("cumulative_count", _sum("count").over(window))
        
        # filter 
        result = accu_df.filter((col("rn") == 1) & (col("count") >= size) |  (col("cumulative_count") <= size)).select("peer_id","year").distinct()
        return result

class TestDataProcess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("DataAnalisysSparkInit").getOrCreate()
        data = [
            ("ABC17969 (AB)", "1", "ABC17969", 2022),
            ("ABC17969 (AB)", "2", "CDC52533", 2022),
            ("ABC17969 (AB)", "3", "DEC59161", 2023),
            ("ABC17969 (AB)", "4", "F43874", 2022),
            ("ABC17969 (AB)", "5", "MY06154", 2021),
            ("ABC17969 (AB)", "6", "MY4387", 2022),
            ("AE686 (AE)", "7", "AE686", 2023),
            ("AE686 (AE)", "8", "BH2740", 2021),
            ("AE686 (AE)", "9", "EG999", 2021),
            ("AE686 (AE)", "10", "AE0908", 2021),
            ("AE686 (AE)", "11", "QA402", 2022),
            ("AE686 (AE)", "12", "OM691", 2022)
        ]
        columns = ["peer_id", "id_1", "id_2", "year"]
        cls.df = cls.spark.createDataFrame(data, schema = columns)
        cls.processor = DataProcess(cls.spark)

    def test_func(self):
        result_df1 = self.processor.filter_data(self.df)

        assert_rt1 = [("ABC17969 (AB)", "1", "ABC17969", 2022), ("AE686 (AE)", "7", "AE686", 2023)]

        self.assertEqual(result_df1.collect(), assert_rt1)
        
        result_df2 = self.processor.flatmap_aggr(self.df, result_df1)
        assert_rt2 = [("ABC17969 (AB)", 2022, 4)]
        
        self.assertEqual(result_df2.collect(), assert_rt2)

        result_df3 = self.processor.accu_find(result_df2, 3)
        assert_rt3 = [("ABC17969 (AB)", 2022), ("AE686 (AE)", 2023)]
        self.assertEqual(result_df3.collect(), assert_rt3)
        
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
if __name__ == "__main__":

    # test case
    # unittest.main()
    
    # init spark
    spark = SparkSession.builder.appName("DataAnalisysSparkInit").getOrCreate()
    data = [
            ("ABC17969 (AB)", "1", "ABC17969", 2022),
            ("ABC17969 (AB)", "2", "CDC52533", 2022),
            ("ABC17969 (AB)", "3", "DEC59161", 2023),
            ("ABC17969 (AB)", "4", "F43874", 2022),
            ("ABC17969 (AB)", "5", "MY06154", 2021),
            ("ABC17969 (AB)", "6", "MY4387", 2022),
            ("AE686 (AE)", "7", "AE686", 2023),
            ("AE686 (AE)", "8", "BH2740", 2021),
            ("AE686 (AE)", "9", "EG999", 2021),
            ("AE686 (AE)", "10", "AE0908", 2021),
            ("AE686 (AE)", "11", "QA402", 2022),
            ("AE686 (AE)", "12", "OM691", 2022)
        ]
    columns = ["peer_id", "id_1", "id_2", "year"]
    
    # init dataframe
    df = spark.createDataFrame(data, schema = columns)
    processor = DataProcess(spark)

    '''
    @Desc: For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
    '''
    step1_df = processor.filter_data(df)
    step1_df.show()

    '''
    @Desc: Given a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1)
    '''
    step2_df = processor.flatmap_aggr(df, step1_df)
    step2_df.show()

    '''
    @Desc: Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year. 
            If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number. 
    '''
    step3_df = processor.accu_find(step2_df, 3)
    step3_df.show()
    
    # end spark
    spark.stop()



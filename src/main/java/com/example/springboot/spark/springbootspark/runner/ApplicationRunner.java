package com.example.springboot.spark.springbootspark.runner;

import com.example.springboot.spark.springbootspark.dto.Count;
import com.example.springboot.spark.springbootspark.dto.Word;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.Long.sum;
import static org.apache.commons.lang3.CharSetUtils.count;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

@Component
public class ApplicationRunner implements CommandLineRunner{

    private static final Logger logger = LoggerFactory.getLogger(ApplicationRunner.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private JavaSparkContext sc;

    @Override
    public void run(String... args) throws Exception {

        logger.info("==> Application started with command-line arguments: {} . \n To kill this application, press Ctrl + C.", Arrays.toString(args));

        //countWords();
        
        dataFrameExample();

    }

    private void dataFrameExample() {

        StructType schema = new StructType()
                .add("ID", LongType, true)
                .add("statecode", StringType, true)
                .add("country", StringType, true)
                .add("number", DoubleType, true);



        Dataset<Row> df = sparkSession.read()
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .csv("src/main/resources/FL_insurance_sample.csv");


        // Displays the content of the DataFrame to stdout
        df.show();

        // Print the schema in a tree format
        df.printSchema();

        // Select only the "country" column
       df.show();

        RelationalGroupedDataset dfResult = df.groupBy("country");

        // After Spark 1.6 columns mentioned in group by will be added to result by default

        dfResult.count().show();//for testing


        // Select people older than 0
        df.filter(col("number").gt(0)).show();

    }

    public void countWords() throws Exception{


        String path = "/usr/local/spark/README.md";


        /**
         * Text file RDDs can be created using SparkContext’s textFile method textFile function. This method takes an URI for the file
         * either a local path on the machine, or a hdfs://, s3n://, etc URI) and reads it as a collection of lines. Every line in the file is
         * an element in the JavaRDD list.
         *
         * Important **Note**: This line defines a base RDD from an external file. This dataset is not loaded in memory or otherwise acted on:
         * lines is merely a pointer to the file.
         *
         * Here is an example invocation:
         */

        JavaRDD<String> rdd = sc.textFile(path);

        /**
         * The function collect, will get all the elements in the RDD into memory for us to work with them.
         * For this reason it has to be used with care, specially when working with large RDDs. In the present
         * example we will filter all the words that contain @ to check all the references to other users in twitter.
         *
         * The function collect return a List
         */

        JavaPairRDD<String, Integer> counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);


        List<Tuple2<String, Integer>> finalCounts = counts.filter((x) -> x._1().contains("@"))
                .collect();

        for(Tuple2<String, Integer> count: finalCounts)
            System.out.println(count._1() + " " + count._2());

        /**
         * This function allow to compute the number of occurrences for a particular word, the first instruction flatMap allows to create the key of the map by splitting
         * each line of the JavaRDD. Map to pair do not do anything because it only define that a map will be done after the reduce function reduceByKey.
         *
         */

        counts = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);

        /**
         * This function allows you to filter the JavaPairRDD for all the elements that the number
         * of occurrences are bigger than 1.
         */

        counts = counts.filter((x) -> x._2() > 0);

        long time = System.currentTimeMillis();
        long countEntries = counts.count();
        logger.info("Count entries: " + countEntries + ", takes in ms: " + String.valueOf(System.currentTimeMillis() - time));

        /**
         * The RDDs can be save to a text file by using the saveAsTextFile function which export the RDD information to a text representation,
         * either a local path on the machine, or a hdfs://, s3n://, etc URI)
         */
        //counts.saveAsTextFile(outputPath);

        for (Tuple2<String, Integer> wordToCounter : counts.collect()) {
            logger.info("Word: " + wordToCounter._1 + " -> " + wordToCounter._2);
        }

        //sc.close();


        JavaRDD<String> lines = sparkSession.read().textFile(path).javaRDD();

        JavaRDD<String> wordsRdd = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
 
        Dataset<Row> dataFrame = sparkSession.createDataFrame(wordsRdd, Word.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).countWords();
        List<Count> wordCounts = rows.stream().map(new Function<Row, Count>() {
            @Override
            public Count apply(Row row) {
                return new Count(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());


        for ( Count count : wordCounts) {
             logger.info("W: " + count.getWord() + " -> " + count.getCount());
        }
    }
}

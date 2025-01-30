import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class KafkaStructuredStreamingConsumer {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("KafkaStructuredStreamingConsumer")
                .master("local[*]")
                .getOrCreate();

        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "test-topic";

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .load();

        Dataset<Row> messages = kafkaStream.selectExpr("CAST(value AS STRING)");

        try {
            StreamingQuery query = messages.writeStream()
                    .format("console")
                    .outputMode(OutputMode.Append())
                    .start();

            query.awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}

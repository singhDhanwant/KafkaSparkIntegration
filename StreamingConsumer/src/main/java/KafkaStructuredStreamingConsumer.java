import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

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

        kafkaStream.createOrReplaceTempView("table");
        Dataset<Row> messages = spark.sql("select cast(value as string) from table");

        try {
            messages.writeStream()
                    .format("console")
                    .outputMode(OutputMode.Append())
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }
}

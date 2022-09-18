package assessment;

/**
 * Assessment to work on using IMDB data
 *
 * Authors:
 * @author Sandeep B
 */

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class TitleRatings {
    static StructType schema = DataTypes.createStructType(
            new StructField[]{
                    createStructField("tconst", StringType, false),
                    createStructField("averageRating", StringType, false),
                    createStructField("numVotes", IntegerType, false)
            });

    static Row parseTitleRatings(String line) {
        String[] data = line.split("\\t");

        return RowFactory.create(data[0], data[1],
                data[2].equals("") ? 0 : Integer.parseInt(data[2]));
    }

    static ExpressionEncoder<Row>  encoder() {
        return RowEncoder.apply(schema);
    }
}

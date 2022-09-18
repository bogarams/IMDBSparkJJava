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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.Arrays;

import java.sql.Array;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

public class TitleAKAS {
    static StructType schema = DataTypes.createStructType(
            new StructField[]{
                    createStructField("titleId", StringType, false),
                    createStructField("ordering", IntegerType, false),
                    createStructField("title", StringType, false),
                    createStructField("region", StringType, false),
                    createStructField("language", BooleanType, false),
                    createStructField("types", new ArrayType(DataTypes.StringType, false), false),
                    createStructField("attributes", new ArrayType(DataTypes.StringType, false), false),
                    createStructField("isOriginalTitle", BooleanType, false),
            });

    static Row parseTitleAKAS(String line) {
        String[] data = line.split("\t");

        return RowFactory.create(data[0], Integer.parseInt(data[1]), data[2], data[3], Boolean.parseBoolean(data[4]), data[5].split(","), data[6].split(","),
                Boolean.parseBoolean(data[7]));
    }

    static ExpressionEncoder<Row> encoder() {
        return RowEncoder.apply(schema);
    }
}

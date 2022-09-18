package assessment;

/**
 * Assessment to work on using IMDB data
 *
 * Authors:
 * @author Sandeep B
 */

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class NameBasics {
    static StructType schema = DataTypes.createStructType(
            new StructField[]{
                    createStructField("nconst", StringType, false),
                    createStructField("primaryName", StringType, false),
                    createStructField("birthYear", StringType, false),
                    createStructField("deathYear", StringType, false),
                    createStructField("primaryProfession", new ArrayType(DataTypes.StringType, false), false),
                    createStructField("knownForTitles", new ArrayType(DataTypes.StringType, false), false),
            });

    static Row parseNameBasics(String line) {
        String[] data = line.split("\t");

        return RowFactory.create(data[0], data[1], data[2], data[3], data[4].split(","), data[5].split(","));
    }

    static ExpressionEncoder<Row> encoder() {
        return RowEncoder.apply(schema);
    }
}

package org.example.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
//import org.apache.flink.types.Row;
import java.util.Optional;

public class ScalaFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.registerFunction("scalarTest", new ScalaFunctionTest.TestScalaFunction(" ---"));

        tEnv.executeSql("create table orders(" +
                "order_id bigint," +
                "price decimal(10,2)," +
                "order_time timestamp" +
                ") " +
                "with (" +
                "   'connector' = 'datagen'" + "," +
                "   'rows-per-second' = '1'" + "," +
                "   'fields.order_id.min' = '1'" + "," +
                "   'fields.order_id.max' = '1000'" + "," +
                "   'fields.price.min' = '1.0'" + "," +
                "   'fields.price.max' = '1000.0'" +
                ")"
        );

        TableResult resTable = tEnv.executeSql("" +
                "select " +
                "   order_id " + "," +
                "   price " + "," +
                "   scalarTest(cast(order_id as string), 'INT') as val_a " + "," +
                "   scalarTest(cast(price as string), 'DOUBLE') as val_b " +
                "from orders");
        resTable.print();
    }

    public static class TestScalaFunction extends ScalarFunction {

        String suffix = "";

        public TestScalaFunction(String suffix) {
            this.suffix = suffix;
        }

        public String eval(String s, String type) {
            switch (type) {
                case "INT":
                    return "INT : " +Integer.valueOf(s) + this.suffix;
                case "DOUBLE":
                    return "Double : " + Double.valueOf(s) + this.suffix;
                case "STRING":
                default:
                    return s + this.suffix;
            }
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    // specify typed arguments
                    // parameters will be casted implicitly to those types if necessary
                    .typedArguments(DataTypes.STRING(), DataTypes.STRING())
                    // specify a strategy for the result data type of the function
                    .outputTypeStrategy(callContext -> {
                        if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
                            throw callContext.newValidationError("Literal expected for second argument.");
                        }
                        // return a data type based on a literal
                        final String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
                        switch (literal) {
                            case "INT":
                                return Optional.of(DataTypes.INT().notNull());
                            case "DOUBLE":
                                return Optional.of(DataTypes.DOUBLE().notNull());
                            case "STRING":
                            default:
                                return Optional.of(DataTypes.STRING());
                        }
                    })
                    .build();
        }
    }
}


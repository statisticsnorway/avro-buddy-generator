package no.ssb.avro.generate;

import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Random;

public class FieldChildGenerator implements GenerateSyntheticData.FieldChildHandler {
    private final Random random = new Random(0);
    long persIdNumber = 1_000_000_000;
    int rowNumber = 0;

    @Override
    public String field(Schema.Type type, String field, String defaultValue, int rowNumber) {
        switch (type) {
            case STRING:
                return filedString(field, defaultValue);
            default:
                return filedString(field, defaultValue);
//                throw new IllegalArgumentException("type:" + type + "  not supported");
        }
    }

    private String generatedData(SchemaBuddy schema, int arrayElementCount) {
        assert schema.isSimpleType();
        if (schema.getType() == Schema.Type.STRING) {
            return schema.getName() + "_" + rowNumber + "_" + arrayElementCount;
        }
        return Integer.toString(random.nextInt(100_000));
    }

    private String filedString(String field, String defaultValue) {
        if (field.equals("personidentifikator")) {
            return Long.toString(persIdNumber + rowNumber);
        }
        return defaultValue;
    }

    @Override
    public int getChildCount(int rowNumber) {
        return 2;
        //(rowNumber % 3) + 1;
    }

    @Override
    public int onNewRow(int rowNumber) {
        return rowNumber;
    }
}

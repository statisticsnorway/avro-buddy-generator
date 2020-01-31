package no.ssb.avro.generate;

import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Random;

public abstract class FieldChildGenerator implements GenerateSyntheticData.FieldChildHandler {
    private final Random random = new Random(0);

    protected String generatedData(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        assert schema.isSimpleType();
        if (schema.getType() == Schema.Type.STRING) {
            return schema.getName() + "_" + rowNum + "_" + arrayElementNum;
        }
        return Integer.toString(random.nextInt(100_000));
    }
}

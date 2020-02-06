package no.ssb.avro.generate;

import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Random;

public abstract class FieldInterceptor implements GenerateSyntheticData.Interceptor {
    protected final Random random;

    public FieldInterceptor() {
        random = new Random();
    }

    public FieldInterceptor(long seed) {
        random = new Random(seed);
    }

    @Override
    public GeneratedField field(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        GeneratedField fieldResult = handleField(schema, rowNum, arrayElementNum);
        if (fieldResult.shouldAutoGenerate()) {
            return generatedData(schema, rowNum, arrayElementNum);
        }
        return fieldResult;
    }

    protected abstract GeneratedField handleField(SchemaBuddy schema, int rowNum, int arrayElementNum);

    protected GeneratedField createRandom(int bound) {
        return GeneratedField.fromLong(random.nextInt(bound));
    }

    protected GeneratedField generatedData(SchemaBuddy schema, int rowNum, int arrayElementNum) {
        assert schema.isSimpleType();
        if (schema.getType() == Schema.Type.STRING) {
            return GeneratedField.fromGeneratedValue(schema.getName() + "_" + rowNum + "_" + arrayElementNum);
        }
        if (schema.getType() == Schema.Type.BOOLEAN) {
            return GeneratedField.fromGeneratedBoolean(random.nextBoolean());
        }
        return GeneratedField.fromGeneratedValue(Integer.toString(random.nextInt(100_000)));
    }
}

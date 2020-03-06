package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Iterator;

public class GenerateSyntheticData implements Iterable<DataElement> {

    public interface FieldHandler {
        GeneratedField field(SchemaBuddy schema, int rowNum, int arrayElementNum);
    }

    public interface RecordInterceptor {
        boolean skipRecord(SchemaBuddy schema, int rowNum, int level);
    }

    public interface FieldInterceptor {
        boolean skipField(SchemaBuddy schema, int rowNum, int level);
    }

    public interface ChildCountHandler {
        int getChildCount(int rowNum);
    }

    public interface Interceptor extends FieldHandler, ChildCountHandler, RecordInterceptor, FieldInterceptor {

    }

    private int rowNum;

    private final SchemaBuddy schemaBuddy;
    private final int numToGenerate;
    private final FieldHandler fieldHandler;
    private final RecordInterceptor recordInterceptor;
    private final FieldInterceptor fieldInterceptor;
    private final ChildCountHandler childCountHandler;

    public GenerateSyntheticData(Schema schema, int numToGenerate, Interceptor interceptor, int startRowNum) {
        this.schemaBuddy = SchemaBuddy.parse(schema);
        this.numToGenerate = numToGenerate;
        this.fieldHandler = interceptor;
        this.recordInterceptor = interceptor;
        this.fieldInterceptor = interceptor;
        this.childCountHandler = interceptor;
        this.rowNum = startRowNum;
    }

    public SchemaBuddy getSchemaBuddy() {
        return schemaBuddy;
    }

    DataElement generate() {
        DataElement root = new DataElement(schemaBuddy.getName());
        return generate(root, schemaBuddy, 0, 0);
    }

    DataElement generate(DataElement dataElement, SchemaBuddy schemaBuddy, int arrayElementCount, int level) {
        for (SchemaBuddy childSchema : schemaBuddy.getChildren()) {
            if (childSchema.isArrayType()) {
                for (int i = 0; i < childCountHandler.getChildCount(0); i++) {
                    generate(dataElement, childSchema, i, level + 1);
                }
                continue;
            }
            DataElement childElement = new DataElement(childSchema.getName());
            if (childSchema.isSimpleType()) {
                if (childSchema.isOptionalWithCheckOfAllChildren() && fieldInterceptor.skipField(childSchema, rowNum, level)) {
                    continue;
                }
                GeneratedField generatedField = getData(childSchema, arrayElementCount);
                switch (generatedField.status) {
                    case IS_GENERATED:
                    case VALUE:
                    case NULL:
                        dataElement.addChild(childElement);
                        childElement.setValue(generatedField.value);
                        break;
                    case MISSING:
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + generatedField.status);
                }
            } else {
                if (childSchema.isOptionalWithCheckOfAllChildren() && recordInterceptor.skipRecord(childSchema, rowNum, level)) {
                    continue;
                }
                dataElement.addChild(childElement);
                generate(childElement, childSchema, arrayElementCount, level + 1);
            }
        }
        return dataElement;
    }

    GeneratedField getData(SchemaBuddy schema, int arrayElementCount) {
        return fieldHandler.field(schema, rowNum, arrayElementCount);
    }

    @Override
    public Iterator<DataElement> iterator() {

        return new Iterator<DataElement>() {
            @Override
            public boolean hasNext() {
                return rowNum < numToGenerate;
            }

            @Override
            public DataElement next() {
                rowNum++;
                if (rowNum % 100 == 0) System.out.print(rowNum + "\r");
                return generate();
            }
        };
    }
}

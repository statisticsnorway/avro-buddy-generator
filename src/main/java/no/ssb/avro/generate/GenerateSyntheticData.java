package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Iterator;

public class GenerateSyntheticData implements Iterable<DataElement> {

    public interface FieldHandler {
        String field(SchemaBuddy schema, int rowNum, int arrayElementNum);
    }

    public interface ChildCountHandler {
        int getChildCount(int rowNum);
    }

    public interface Interceptor extends FieldHandler, ChildCountHandler {

    }

    private int rowNum = 0;

    private final SchemaBuddy schemaBuddy;
    private final int numToGenerate;
    private final FieldHandler fieldHandler;
    private final ChildCountHandler childCountHandler;

    public GenerateSyntheticData(Schema schema, int numToGenerate, Interceptor interceptor) {
        this.schemaBuddy = SchemaBuddy.parse(schema);
        this.numToGenerate = numToGenerate;
        this.fieldHandler = interceptor;
        this.childCountHandler = interceptor;
    }

    public SchemaBuddy getSchemaBuddy() {
        return schemaBuddy;
    }

    DataElement generate() {
        DataElement root = new DataElement(schemaBuddy.getName());
        return generate(root, schemaBuddy, 0);
    }

    DataElement generate(DataElement dataElement, SchemaBuddy schemaBuddy, int arrayElementCount) {
        for (SchemaBuddy childSchema : schemaBuddy.getChildren()) {
            if (childSchema.isArrayType()) {
                for (int i = 0; i < childCountHandler.getChildCount(0); i++) {
                    generate(dataElement, childSchema, i);
                    arrayElementCount++;
                }
                continue;
            }

            DataElement childElement = new DataElement(childSchema.getName());
            dataElement.addChild(childElement);
            if (childSchema.isSimpleType()) {
                childElement.setValue(getData(childSchema, arrayElementCount));
            } else {
                generate(childElement, childSchema, arrayElementCount);
            }
        }
        return dataElement;
    }

    String getData(SchemaBuddy schema, int arrayElementCount) {
        return fieldHandler.field(schema, rowNum, arrayElementCount);
    }

    @Override
    public Iterator<DataElement> iterator() {

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rowNum < numToGenerate;
            }

            @Override
            public DataElement next() {
                rowNum++;
                return generate();
            }
        };
    }
}

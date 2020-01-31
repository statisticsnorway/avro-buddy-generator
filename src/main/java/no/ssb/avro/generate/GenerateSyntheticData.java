package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.Random;

public class GenerateSyntheticData implements Iterable<DataElement> {

    public interface FieldHandler {
        String field(SchemaBuddy schema, int rowNum, int arrayElementNum);
    }

    public interface ChildCountHandler {
        int getChildCount(int rowNum);
    }

    public interface RowHandler {
        int onNewRow(int rowNum);
    }

    public interface FieldChildHandler extends FieldHandler, ChildCountHandler, RowHandler {

    }

    private Integer count = 0;

    private final SchemaBuddy schemaBuddy;
    private final int numToGenerate;
    private final FieldHandler fieldHandler;
    private final ChildCountHandler childCountHandler;
    private final RowHandler rowHandler;

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldChildHandler fieldChildHandler) {
        this(schema, numToGenerate, fieldChildHandler, fieldChildHandler);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler, ChildCountHandler childCountHandler) {
        this(schema, numToGenerate, fieldHandler, childCountHandler, null);
    }

    public GenerateSyntheticData(Schema schema, int numToGenerate, FieldHandler fieldHandler, ChildCountHandler childCountHandler, RowHandler rowHandler) {
        schemaBuddy = SchemaBuddy.parse(schema);
        this.numToGenerate = numToGenerate;
        this.childCountHandler = childCountHandler;
        this.fieldHandler = fieldHandler;
        if (rowHandler == null) {
            rowHandler = rowNum -> count;
        }
        this.rowHandler = rowHandler;
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
        return fieldHandler.field(schema, count, arrayElementCount);
    }

    @Override
    public Iterator<DataElement> iterator() {

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return count < numToGenerate;
            }

            @Override
            public DataElement next() {
                rowHandler.onNewRow(count++);
                return generate();
            }
        };
    }
}

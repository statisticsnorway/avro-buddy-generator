package no.ssb.avro.generate;

public class GeneratedField {
    enum Status {VALUE, NULL, MISSING, SHOULD_BE_GENERATED, IS_GENERATED}

    final String value;
    final Status status;

    public static GeneratedField fromInteger(int number) {
        return new GeneratedField(Integer.toString(number));
    }

    public static GeneratedField fromLong(long number) {
        return new GeneratedField(Long.toString(number));
    }

    public static GeneratedField fromString(String value) {
        return new GeneratedField(value);
    }

    public static GeneratedField shouldBeGenerated() {
        return new GeneratedField(Status.SHOULD_BE_GENERATED);
    }

    public static GeneratedField nullStatus() {
        return new GeneratedField(Status.NULL);
    }

    public static GeneratedField missingStatus() {
        return new GeneratedField(Status.MISSING);
    }

    public static GeneratedField fromGeneratedValue(String value) {
        return new GeneratedField(value, Status.IS_GENERATED);
    }

    boolean shouldAutoGenerate() {
        return status == Status.SHOULD_BE_GENERATED;
    }

     boolean hasValue() {
        return status == Status.IS_GENERATED || status == Status.VALUE;
    }

    public GeneratedField(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        this.value = value;
        this.status = Status.VALUE;
    }

    public GeneratedField(Status status) {
        if (status == Status.VALUE) {
            throw new IllegalArgumentException("Status:" + status + " but no value provided");
        }
        this.value = null;
        this.status = status;
    }

    private GeneratedField(String value, Status status) {
        if (status != Status.IS_GENERATED) {
            throw new IllegalArgumentException("Can only be used to provide generated value");
        }
        this.value = value;
        this.status = status;
    }
}

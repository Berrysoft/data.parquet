package berrysoft.data;

import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;

public class ParquetColumnSeq extends ASeq {
    private long col;

    private Object current;

    public ParquetColumnSeq(long col) {
        this.col = col;
    }

    public ParquetColumnSeq(IPersistentMap meta, long col) {
        super(meta);
        this.col = col;
    }

    @Override
    public Object first() {
        if (current == null) {
            current = ParquetNative.columnNext(col);
        }
        return current;
    }

    @Override
    public ISeq next() {
        first();
        ParquetColumnSeq nextSeq = new ParquetColumnSeq(col);
        if (nextSeq.first() == null) {
            return null;
        }
        return nextSeq;
    }

    @Override
    public ParquetColumnSeq withMeta(IPersistentMap meta) {
        if (meta() == meta)
            return this;
        return new ParquetColumnSeq(meta, col);
    }
}

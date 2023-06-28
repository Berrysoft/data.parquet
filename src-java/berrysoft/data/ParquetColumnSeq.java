package berrysoft.data;

import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;

public class ParquetColumnSeq extends ASeq {
    private ParquetColumnIterator iter;

    private Object current;

    public ParquetColumnSeq(ParquetColumnIterator iter) {
        this.iter = iter;
    }

    public ParquetColumnSeq(IPersistentMap meta, ParquetColumnIterator iter) {
        super(meta);
        this.iter = iter;
    }

    @Override
    public Object first() {
        if (current == null) {
            this.current = iter.next();
        }
        return current;
    }

    @Override
    public ISeq next() {
        ParquetColumnSeq nextSeq = new ParquetColumnSeq(iter);
        if (nextSeq.first() == null) {
            return null;
        }
        return nextSeq;
    }

    @Override
    public ParquetColumnSeq withMeta(IPersistentMap meta) {
        if (meta() == meta)
            return this;
        return new ParquetColumnSeq(meta, iter);
    }
}

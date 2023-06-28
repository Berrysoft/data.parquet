package berrysoft.data;

import java.lang.AutoCloseable;
import java.util.Iterator;

public class ParquetColumnIterator implements Iterator<Object>, AutoCloseable {
    private long col;

    private Object current;

    public ParquetColumnIterator(long col) {
        this.col = col;
    }

    private Object fetchNext() {
        return ParquetNative.columnNext(col);
    }

    @Override
    public boolean hasNext() {
        return getCurrent() != null;
    }

    @Override
    public Object next() {
        if (current == null) {
            return fetchNext();
        } else {
            Object res = current;
            current = null;
            return res;
        }
    }

    public Object getCurrent() {
        if (current == null) {
            current = fetchNext();
        }
        return current;
    }

    @Override
    public void close() throws Exception {
        ParquetNative.closeColumn(col);
        col = 0;
    }
}

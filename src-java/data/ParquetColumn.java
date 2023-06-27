package data;

import java.util.Iterator;
import java.io.Closeable;
import java.io.IOException;

public class ParquetColumn implements Iterator<Object>, Closeable {
    private long col;

    private Object current;

    public ParquetColumn(long col) {
        this.col = col;
    }

    private Object fetchNext() {
        return ParquetNative.columnNext(col);
    }

    @Override
    public boolean hasNext() {
        if (current == null) {
            current = fetchNext();
        }
        return current != null;
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

    @Override
    public void close() throws IOException {
        ParquetNative.closeColumn(col);
    }
}

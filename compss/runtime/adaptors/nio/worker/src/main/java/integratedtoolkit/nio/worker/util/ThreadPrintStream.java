package integratedtoolkit.nio.worker.util;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


public class ThreadPrintStream extends PrintStream {

    private Map<Long, PrintStream> threadToStream = new HashMap<>();
    private final String end;
    private final PrintStream defaultStream;


    public ThreadPrintStream(String end, PrintStream defaultStream) {
        super(defaultStream);
        this.end = end;
        this.defaultStream = defaultStream;
    }

    public void registerThread(String name) {
        try {
            PrintStream ps = new PrintStream(name + end);
            threadToStream.put(Thread.currentThread().getId(), ps);
            ps.print("");
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    public void unregisterThread() {
        try {
            close();
            threadToStream.remove(Thread.currentThread().getId());
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void flush() {
        getStream().flush();

    }

    @Override
    public void close() {
        getStream().close();
    }

    @Override
    public boolean checkError() {
        return getStream().checkError();
    }

    @Override
    public void write(int b) {
        getStream().write(b);
    }

    @Override
    public void write(byte buf[], int off, int len) {
        getStream().write(buf, off, len);
    }

    @Override
    public void print(boolean b) {
        getStream().print(b);
    }

    @Override
    public void print(char c) {
        getStream().print(c);
    }

    @Override
    public void print(int i) {
        getStream().print(i);
    }

    @Override
    public void print(long l) {
        getStream().print(l);
    }

    @Override
    public void print(float f) {
        getStream().print(f);
    }

    @Override
    public void print(double d) {
        getStream().print(d);
    }

    @Override
    public void print(char s[]) {
        getStream().print(s);
    }

    @Override
    public void print(String s) {
        getStream().print(s);
    }

    @Override
    public void print(Object obj) {
        getStream().print(obj);
    }

    @Override
    public void println() {
        getStream().println();
    }

    @Override
    public void println(boolean x) {
        getStream().println(x);
    }

    @Override
    public void println(char x) {
        getStream().println(x);
    }

    public void println(int x) {
        getStream().println(x);
    }

    @Override
    public void println(long x) {
        getStream().println(x);
    }

    @Override
    public void println(float x) {
        getStream().println(x);
    }

    @Override
    public void println(double x) {
        getStream().println(x);
    }

    @Override
    public void println(char x[]) {
        getStream().println(x);
    }

    @Override
    public void println(String x) {
        getStream().println(x);
    }

    @Override
    public void println(Object x) {
        getStream().println(x);
    }

    @Override
    public PrintStream printf(String format, Object... args) {
        return getStream().printf(format, args);
    }

    @Override
    public PrintStream printf(Locale l, String format, Object... args) {
        return getStream().format(l, format, args);
    }

    @Override
    public PrintStream format(String format, Object... args) {
        return getStream().format(format, args);
    }

    @Override
    public PrintStream format(Locale l, String format, Object... args) {
        return getStream().format(l, format, args);
    }

    @Override
    public PrintStream append(CharSequence csq) {
        return getStream().append(csq);
    }

    @Override
    public PrintStream append(CharSequence csq, int start, int end) {
        return getStream().append(csq, start, end);
    }

    @Override
    public PrintStream append(char c) {
        return getStream().append(c);
    }

    public PrintStream getStream() {
        PrintStream ps = threadToStream.get(Thread.currentThread().getId());
        if (ps == null) {
            ps = defaultStream;
        }
        return ps;
    }

}

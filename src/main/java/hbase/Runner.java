package hbase;

import java.io.IOException;

abstract class Runner {
    void start() throws IOException {
        pre();
        run();
        end();
    }

    protected abstract void pre() throws IOException;
    protected abstract void run() throws IOException;
    protected abstract void end() throws IOException;
}
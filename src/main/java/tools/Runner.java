package tools;

import java.io.IOException;

public abstract class Runner {
    public void start() throws IOException {
        pre();
        run();
        end();
    }

    protected abstract void pre() throws IOException;
    protected abstract void run() throws IOException;
    protected abstract void end() throws IOException;
}
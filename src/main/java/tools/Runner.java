package tools;

import java.io.IOException;

/**
 * 用于启动任务。
 * @author RailgunHamster（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/7
 */
public abstract class Runner {
    /**
     * 供用户调用，不可重写
     */
    final public void start() throws IOException {
        pre();
        run();
        end();
    }

    protected abstract void pre() throws IOException;
    protected abstract void run() throws IOException;
    protected abstract void end() throws IOException;
}
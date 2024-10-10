package com.test.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public abstract class CustomSourceFunction<T> extends RichParallelSourceFunction<T> {
    private volatile boolean stop;
    protected int indexOfSubtask;


    @Override
    final public void open(Configuration parameters) throws Exception {
        indexOfSubtask = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        while (!stop) {
            ctx.collect(elementGene());
        }
    }

    public abstract T elementGene();

    @Override
    public void cancel() {
        stop = true;
    }
}

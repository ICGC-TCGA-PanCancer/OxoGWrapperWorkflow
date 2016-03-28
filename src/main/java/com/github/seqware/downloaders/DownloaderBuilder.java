package com.github.seqware.downloaders;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DownloaderBuilder<T extends WorkflowFileDownloader> {

    private final Supplier<T> instantiator;

    private List<Consumer<T>> instanceModifiers = new ArrayList<>();

    public DownloaderBuilder(Supplier<T> instantiator) {
        this.instantiator = instantiator;
    }

    public static <T extends WorkflowFileDownloader> DownloaderBuilder<T> of(Supplier<T> instantiator) {
        return new DownloaderBuilder<T>(instantiator);
    }

    public <U> DownloaderBuilder<T> with(BiConsumer<T, U> consumer, U value) {
        Consumer<T> c = instance -> consumer.accept(instance, value);
        instanceModifiers.add(c);
        return this;
    }

    public T build() {
        T value = instantiator.get();
        instanceModifiers.forEach(modifier -> modifier.accept(value));
        instanceModifiers.clear();
        return value;
    }
}
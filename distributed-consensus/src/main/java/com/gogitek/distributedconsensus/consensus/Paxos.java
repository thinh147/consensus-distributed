package com.gogitek.distributedconsensus.consensus;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;

@Service
public class Paxos {
    public void prompt(@NotNull String ask, @Nullable String answer, @NotNull Consumer<String> callback, String input) {
        if (answer == null || answer.equals(input))
            callback.accept(input);
    }
}
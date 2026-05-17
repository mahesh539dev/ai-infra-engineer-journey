package com.ai.learning.kafka.integration.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class RecursiveCharacterTextSplitter {

    private static final List<String> SEPARATORS = List.of("\n\n", "\n", " ", "");

    private final int chunkSize;
    private final int chunkOverlap;

    public RecursiveCharacterTextSplitter(int chunkSize, int chunkOverlap) {
        if (chunkSize <= 0) throw new IllegalArgumentException("chunkSize must be > 0");
        if (chunkOverlap < 0 || chunkOverlap >= chunkSize) throw new IllegalArgumentException("chunkOverlap must be >= 0 and < chunkSize");
        this.chunkSize = chunkSize;
        this.chunkOverlap = chunkOverlap;
    }

    public List<String> split(String text) {
        if (text == null || text.isBlank()) return List.of();
        List<String> result = new ArrayList<>();
        doSplit(text.strip(), SEPARATORS, result);
        return result;
    }

    private void doSplit(String text, List<String> separators, List<String> output) {
        if (text.length() <= chunkSize) {
            output.add(text);
            return;
        }

        String chosenSep = "";
        int chosenIdx = separators.size() - 1;
        for (int i = 0; i < separators.size(); i++) {
            String s = separators.get(i);
            if (s.isEmpty() || text.contains(s)) {
                chosenSep = s;
                chosenIdx = i;
                break;
            }
        }

        List<String> nextSeps = chosenIdx + 1 < separators.size()
                ? separators.subList(chosenIdx + 1, separators.size())
                : List.of();

        String[] parts = chosenSep.isEmpty()
                ? splitByChar(text)
                : text.split(Pattern.quote(chosenSep), -1);

        List<String> goodParts = new ArrayList<>();
        for (String part : parts) {
            String p = part.strip();
            if (p.isEmpty()) continue;
            if (p.length() > chunkSize) {
                mergeInto(goodParts, chosenSep, output);
                goodParts.clear();
                doSplit(p, nextSeps.isEmpty() ? List.of("") : nextSeps, output);
            } else {
                goodParts.add(p);
            }
        }
        mergeInto(goodParts, chosenSep, output);
    }

    private String[] splitByChar(String text) {
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < text.length(); i += chunkSize - chunkOverlap) {
            parts.add(text.substring(i, Math.min(i + chunkSize, text.length())));
        }
        return parts.toArray(String[]::new);
    }

    private void mergeInto(List<String> parts, String sep, List<String> output) {
        if (parts.isEmpty()) return;
        int sepLen = sep.length();
        List<String> current = new ArrayList<>();

        for (String part : parts) {
            int projected = totalLen(current, sepLen) + (current.isEmpty() ? 0 : sepLen) + part.length();
            if (projected > chunkSize && !current.isEmpty()) {
                output.add(String.join(sep, current));
                while (!current.isEmpty() && totalLen(current, sepLen) > chunkOverlap) {
                    current.remove(0);
                }
            }
            current.add(part);
        }
        if (!current.isEmpty()) output.add(String.join(sep, current));
    }

    private int totalLen(List<String> parts, int sepLen) {
        if (parts.isEmpty()) return 0;
        return parts.stream().mapToInt(String::length).sum() + Math.max(0, parts.size() - 1) * sepLen;
    }
}

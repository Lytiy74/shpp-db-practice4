package shpp.azaika.util;

import java.util.List;

@FunctionalInterface
public interface EntityGenerator<T> {
    List<T> generate(int quantity);
}

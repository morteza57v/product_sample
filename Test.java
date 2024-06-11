package net.navoshgaran.mavad.cnv;

import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Test {

    public static void main(String[] args) {
//        System.out.println(new DecimalFormat("00.0").format(myNumber));
        String str1 = "My String";
        String str2 = new String("My String");
//        System.out.println(Stream.of("green","yellow","blue").max((s1,s2) -> s1.compareTo(s2))
//                .filter(s -> s.endsWith("n")).orElse("yellow"));

//        System.out.println(str1.hashCode() == str2.hashCode());

        Supplier<String> i = () -> "Car";

        Consumer<String> c = x -> System.out.println(x.toLowerCase());
        Consumer<String> d = x -> System.out.println(x.toUpperCase());

        c.andThen(d).accept(i.get());

        System.out.println();
    }
}

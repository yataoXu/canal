package com.evan.canal;

/**
 * @Description
 * @ClassName test
 * @Author Evan
 * @date 2019.10.22 10:39
 */
public class test {
    public static void main(String[] args) {
        String demo ="tt.id,tt.age,tt.name";
        String dd= demo.substring(0,demo.indexOf(","));
        System.out.println(dd);
    }
}

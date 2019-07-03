package com.meirkhan.kafka;

public class MyTests {
    public static void main(String[] args) {
        String query = "db.fggvhbvvghghh.find(sdfsdf)";
        Boolean isRightPattern = query.matches("^db\\.(.+)find([\\(])(.*)([\\)])");
        System.out.println(isRightPattern);
    }
}

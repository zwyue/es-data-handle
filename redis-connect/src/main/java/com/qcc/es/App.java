package com.qcc.es;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        batchSet();
    }


    public static void batchIdc() {
        Jedis jedis;
        try (JedisPool pool = ConnectRedis.returnJedisPool(37)) {
            jedis = pool.getResource();

            Set<String> keys = jedis.keys("*");
            keys.forEach(key -> {

                if (key.contains("1620396577096810540")) {
                    String dateStr = jedis.get(key);
                    System.out.println(key + ":" + dateStr);
//                    jedis.set(key, "2023-06-08 10:00:00");
                    jedis.del(key);
//                    return;
                }
//                String dateStr = jedis.get(key);
//                System.out.println(key + ":" + dateStr);
//                jedis.set(key, "2023-05-22 14:00:00");
            });
        }
    }


    public static void batchSet() {
        Jedis jedis;
        try (JedisPool pool = ConnectRedis.returnJedisPool(17)) {
            jedis = pool.getResource();

            Set<String> keys = jedis.keys("*");
            keys.forEach(key -> {

                if (key.contains("1620396577096810594")) {
                    String dateStr = jedis.get(key);
                    System.out.println(key + ":" + dateStr);
//                    jedis.set(key, "2023-06-01 06:00:00");
                    jedis.del(key);
//                    return;
                }
//                if (
//                        key.contains("proCompanyEmployeesAiV1Handler")
//                ) {
//                    String dateStr = jedis.get(key);
//                    System.out.println(key+":"+dateStr);
//                    jedis.del(key);
//                } else {
////                String dateStr = jedis.get(key);
////                System.out.println(key + ":" + dateStr);
////                jedis.set(key, "2023-05-15 00:00:00");
//                }
            });
        }
    }

    public static void roverSet() {
        Jedis jedis;
        try (JedisPool pool = ConnectRedis.returnJedisPool(40)) {
            jedis = pool.getResource();

            Set<String> keys = jedis.keys("*");
            keys.forEach(key -> {

//                if (key.startsWith("checkBlacklistNewHandler")) {
//                    if (key.startsWith("sql")) {
//                        jedis.del(key);
//                        return;
//                    }
//                    String dateStr = jedis.get(key);
//                    System.out.println(key + ":" + dateStr);
//                    jedis.set(key, "2023-01-01 00:00:00");
//
//                }
            });
        }
    }

    public static void buildingSet() {
        JedisPool pool = ConnectRedis.returnJedisPool(34);
        Jedis jedis = pool.getResource();

        Set<String> keys = jedis.keys("*");

        keys.forEach(key -> {

//            if (key.startsWith("proInBuildingRoadAchievementSyncV1Handler")) {
//                jedis.del(key);
//                return;
//            }

            if (
                key.startsWith("ratingJobHandler")
            ) {
                String dateStr = jedis.get(key);
                System.out.println(key+":"+dateStr);
//                jedis.set(key, "2023-06-07 14:00:00");
            } else {
//                String dateStr = jedis.get(key);
//                System.out.println(key + ":" + dateStr);
//                jedis.set(key, "2023-05-15 00:00:00");
            }
        });
    }

    public static void changeDate() {

        JedisPool pool = ConnectRedis.returnJedisPool(34);
        Jedis jedis = pool.getResource();

        Set<String> keys = jedis.keys("*");

//        jedis.del("ratingJobHandler_lastTime0","ratingJobHandler_lastTime1",
//            "ratingJobHandler_lastTime2","ratingJobHandler_lastTime3","ratingJobHandler_lastTime4"
//            ,"ratingJobHandler_lastTime5","ratingJobHandler_lastTime6",
//            "ratingJobHandler_lastTime7","ratingJobHandler_lastTimeh",
//            "ratingJobHandler_lastTimes","ratingJobHandler_lastTimeg") ;

        keys.forEach(key -> {
            if (key.startsWith("sql")) {
                return;
            }

            String dateStr = jedis.get(key);
//            System.out.println(key+":"+dateStr);
//owner，item，register，company
            if (
                key.startsWith("company")
                    || key.startsWith("register")
                    || key.startsWith("item")
                    || key.startsWith("owner")
                    || key.startsWith("rating")
            ) {
//                String dateStr = jedis.get(key);
//                System.out.println(key+":"+dateStr);
//                jedis.set(key, "2023-05-01 00:00:00");
                return;
//                }

//                Thread.sleep(1000*5L);
            }
//            jedis.set(key, "2023-05-01 00:00:00");
            System.out.println(key + ":" + dateStr);

//            System.out.println(key);
            if (
                key.contains("ratingV2JobHandler")
                    || key.contains("awardsV2JobHandler")
//                || key.contains("certificateJobHandler")
//                || key.contains("buildingBaseInfoJobHandler")
//                || key.contains("itemDrawsJobHandler_lastTime")
//                || key.contains("ownerContactHandler_lastTime")
//                || key.contains("ownerAgentHandler_lastTime")
//                || key.contains("ownerTenderHandler_lastTime")
//                || key.contains("companyItemHandler_lastTime")
//                || key.contains("awardsV2JobHandler_lastTime")
//                || key.contains("itemDetailsJobHandler_lastTime")
//                || key.contains("registerRecordsV2JobHandler_lastTime")
//                || key.contains("itemContractsJobHandler_lastTime")
//                || key.contains("ownerJobHandler")
//                || key.contains("companyBuildingHandler"
//                )
//                || key.contains("buildingBaseInfoJobHandler")
//                || key.contains("importCustomerHandler")
//                || key.contains("ownerTenderHandler")

            ) {
                jedis.del(key);
                return;
            }
            jedis.set(key, "2023-05-01 00:00:00");
//            String dateStr = jedis.get(key);
//            System.out.println(key+":"+dateStr);
            String dateStr2 = "2023-05-01 12:00:00";
//            String dateStr3 = "2023-05-12 21:55:00";
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date1 = null;
            Date date2 = null;
            Date date3 = null;
            try {
//                date1 = format.parse(dateStr);
//                date3 = format.parse(dateStr3);
//                date2 = format.parse(dateStr2);
//
//                if (date1.before(date2)) {
//                    jedis.del(key) ;
//                }
////
//                if (date1.after(date3)) {
//                    jedis.set(key, dateStr2);
////                }
//
//                    Thread.sleep(1000*5L);

            } catch (Exception e) {

            }
        });

        jedis.close();

    }
}

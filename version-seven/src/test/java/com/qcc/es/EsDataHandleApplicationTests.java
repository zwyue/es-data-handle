package com.qcc.es;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class EsDataHandleApplicationTests {

    @Test
    void contextLoads() {

        Map map = new HashMap() ;
        map.put("1",1) ;
        map.put("2",1) ;
        map.put("3",1) ;

        Map map1 = new HashMap() ;
        map1.putAll(map);
        map1.remove("1") ;

        System.out.println(map);
        System.out.println(map1);
    }

}

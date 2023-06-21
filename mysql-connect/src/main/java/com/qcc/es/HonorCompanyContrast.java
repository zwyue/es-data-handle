package com.qcc.es;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//@Log4j2
public class HonorCompanyContrast {


    public static void start() {

        String[] tables = new String[] {"0", "1", "2", "3", "4", "5", "6", "7", "g", "h", "s"};

        ExecuteParams executeParams0 = new ExecuteParams(MysqlServer.COMPANY, null, null, null,
            null, null, null, "12");
        Object obj = ConnectUtil.execute(executeParams0);

        int count = (int) obj;

        int forCycle = count % 1000 == 0 ? count / 1000 : count / 1000 + 1;

        for (int i = 0; i < forCycle; i++) {
            System.out.println("...... for " + i + " times ......");
            int from = i * 1000;

            ExecuteParams executeParams = new ExecuteParams(MysqlServer.COMPANY, null, null,
                null,
                null, from, 1000, "13");
            Object result = ConnectUtil.execute(executeParams);

            List list = (List) result;

            List<String> ids = new ArrayList<>();

            for (Object record : list) {
                HashMap hashMap = (HashMap) record;
                String id = (String) hashMap.get("comp_keyno");
                ids.add(id);
            }

            int length = 0;

            boolean notExist = true;

            for (String table : tables) {
                executeParams = new ExecuteParams(MysqlServer.COMPANY, table, null, null,
                    ids, null, null, "11");
                Object company = ConnectUtil.execute(executeParams);
                List corps = (List) company;

                length += corps.size();

                if (ids.size() == length) {
                    notExist = false;
                    break;
                }
            }

            if (notExist) {

                for (String id : ids) {
                    notExist = true;
                    for (String table : tables) {
                        List<String> newList = new ArrayList<>();
                        newList.add(id);
                        executeParams =
                            new ExecuteParams(MysqlServer.COMPANY, table, null, null, newList,
                                null, null, "11");
                        Object company = ConnectUtil.execute(executeParams);
                        List corps = (List) company;

                        if (corps.size() == 1) {
                            notExist = false;
                            break;
                        }
                    }

                    if (notExist) {
                        System.out.println("...... id :" + id + " does not exist .....");
                        System.exit(0);
                    }
                }
            }
        }
        System.exit(0);
    }
}

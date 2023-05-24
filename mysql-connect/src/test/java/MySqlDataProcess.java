import com.qcc.es.ConnectUtil;
import com.qcc.es.MysqlServer;
import com.qcc.es.SearchDataLabel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MySqlDataProcess {
    public static void main(String[] args) {
//        modifyMarkedData();
//        replaceData();
        replaceDataOrigin();
        System.exit(0);
    }


    public static void modifyMarkedData() {

        String whereSql =
            " lab like '%FIRM_M IND_M%' and author = 'zhuwy' ";
        Object obj = ConnectUtil.execute(MysqlServer.DATA_CENTER, whereSql, null, "2");

        List<Map<String, Object>> listMap = (List<Map<String, Object>>) obj;

        List<Map<String, Object>> newMapList = new LinkedList<>();
        assert listMap != null;
        listMap.forEach(map -> {
            String lab = (String) map.get("lab");
            String searchKey = (String) map.get("searchkey");


            String[] keys = searchKey.split(" ");
            String[] codes = lab.split(" ");

            try {
                for (int i = 0; i < keys.length; i++) {
                    Map<String, Object> identityHashMap = new HashMap<>();
                    identityHashMap.putAll(map);
                    identityHashMap.put("searchkey", keys[i]);
                    identityHashMap.put("lab", codes[i]);
                    identityHashMap.put("old_key", searchKey);
                    identityHashMap.put("old_code", lab);
                    identityHashMap.put("old_key_no_blank", searchKey.replaceAll(" ", ""));
                    newMapList.add(identityHashMap);
                }
            } catch (Exception e) {
                System.out.println("...... something wrong .....");
            }

            if (newMapList.size() >= 1000) {
                ConnectUtil.execute(MysqlServer.DATA_CENTER, "", newMapList, "3");
                newMapList.clear();
            }
        });

        ConnectUtil.execute(MysqlServer.DATA_CENTER, "", newMapList, "3");
    }


    public static void replaceData() {

        String whereSql = " new_id >= 5191 and new_id < 6268 ";
        Object obj = ConnectUtil.execute(MysqlServer.DATA_CENTER, whereSql, null, "4");
        List<SearchDataLabel> listMap = (List<SearchDataLabel>) obj;

        assert listMap != null;
        Map<String, List<SearchDataLabel>> tempMap =
            listMap.stream().collect(Collectors.groupingBy(SearchDataLabel::getId));

        List<Map<String, Object>> newMapList = new LinkedList<>();
        tempMap.keySet().forEach(key -> {
            List<SearchDataLabel> list = tempMap.get(key);
            StringBuilder newKey = new StringBuilder();
            list.forEach(keyInfo -> newKey.append(keyInfo.getLab()).append(" "));

            Map<String, Object> map = new HashMap<>();
            map.put("id", key);
            map.put("newLab", newKey.substring(0, newKey.lastIndexOf(" ")));
            newMapList.add(map);
        });

        ConnectUtil.execute(MysqlServer.DATA_CENTER, "", newMapList, "5");
    }

    public static void replaceDataOrigin() {
        Object obj = ConnectUtil.execute(MysqlServer.DATA_CENTER, null, null, "6");
        List<Map<String, Object>> listMap = (List<Map<String, Object>>) obj;
        ConnectUtil.execute(MysqlServer.DATA_CENTER, "", listMap, "7");
    }
}

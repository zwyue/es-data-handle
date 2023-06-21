import com.qcc.es.ConnectUtil;
import com.qcc.es.ExecuteParams;
import com.qcc.es.MysqlServer;
import com.qcc.es.SearchDataLabel;
import java.util.ArrayList;
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

        ExecuteParams
            executeParams = new ExecuteParams(MysqlServer.DATA_CENTER,null,whereSql,null,
            null,null,null,"2");
        Object obj = ConnectUtil.execute(executeParams);

        List<Map<String, Object>> listMap = (List<Map<String, Object>>) obj;

        List<Map<String, Object>> newMapList = new LinkedList<>();
        assert listMap != null;
        for (Map<String, Object> map : listMap) {
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

                executeParams = new ExecuteParams(MysqlServer.DATA_CENTER, null, null, newMapList,
                    null, null, null, "3");
                ConnectUtil.execute(executeParams);
            }
        }

        executeParams = new ExecuteParams(MysqlServer.DATA_CENTER, null, null, newMapList,
            null, null, null, "3");
        ConnectUtil.execute(executeParams);
    }


    public static void replaceData() {

        String whereSql = " new_id >= 5191 and new_id < 6268 ";


        ExecuteParams
            executeParams = new ExecuteParams(MysqlServer.DATA_CENTER,null,whereSql,null,
            null,null,null,"4");
        Object obj = ConnectUtil.execute(executeParams);
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

        executeParams = new ExecuteParams(MysqlServer.DATA_CENTER, null, null, newMapList,
            null, null, null, "5");
        ConnectUtil.execute(executeParams);
    }

    public static void replaceDataOrigin() {

        ExecuteParams
            executeParams = new ExecuteParams(MysqlServer.DATA_CENTER,null,null,null,
            null,null,null,"6");

        Object obj = ConnectUtil.execute(executeParams);
        List<Map<String, Object>> listMap = (List<Map<String, Object>>) obj;

        executeParams = new ExecuteParams(MysqlServer.DATA_CENTER, null, null, listMap,
            null, null, null, "7");
        ConnectUtil.execute(executeParams);
    }
}

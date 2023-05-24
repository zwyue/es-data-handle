package com.qcc.es;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

public class ConnectUtil {

    private ConnectUtil() {
        throw new IllegalStateException("ConnectUtil class");
    }

    private static final String URL_PATTER = "jdbc:mysql://%s/%s?Unicode=true&characterEncoding" +
        "=UTF-8&useSSL=false&serverTimezone=CTT&autoReconnect=true&connectTimeout=3000&socketTimeout=3000&autoReconnect=true";

    public static SqlSessionFactory mysqlClient(MysqlServer mysqlServer) throws Exception {

        PooledDataSource pooledDataSource = new PooledDataSource();
        pooledDataSource.setDriver("com.mysql.cj.jdbc.Driver");
        pooledDataSource.setUrl(String.format(URL_PATTER,mysqlServer.getUrl(),mysqlServer.getDatabase()));
        pooledDataSource.setUsername(mysqlServer.getUsername());
        pooledDataSource.setPassword(mysqlServer.getPassword());
        pooledDataSource.setLoginTimeout(10000);
        pooledDataSource.setPoolTimeToWait(10000);
        pooledDataSource.setDefaultNetworkTimeout(10000);

        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        //配置mapper路径
        Resource[] resources = resolver.getResources("classpath:/DynamicMapper.xml");

        MybatisSqlSessionFactoryBean sqlSessionFactoryBean = new MybatisSqlSessionFactoryBean();
        sqlSessionFactoryBean.setMapperLocations(resources);
        sqlSessionFactoryBean.setDataSource(pooledDataSource);

        MybatisConfiguration mybatisConfiguration = new MybatisConfiguration();
        mybatisConfiguration.setDefaultStatementTimeout(30);

        sqlSessionFactoryBean.setConfiguration(mybatisConfiguration);

        SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBean.getObject();

        if (sqlSessionFactory == null) {
            throw new ConnectException("连接失败");
        }

        return sqlSessionFactory;
    }

    public static Object execute(MysqlServer mysqlServer, String whereSql,
                                 List<Map<String,Object>> list, String method) {
        try (SqlSession sqlSession = mysqlClient(mysqlServer).openSession()) {
            if (sqlSession == null) {
                return null;
            }

            DynamicMapper dynamicMapper = sqlSession.getMapper(DynamicMapper.class);


            return switch (method) {
                case "1" -> dynamicMapper.tenderInfo(whereSql);
                case "2" -> dynamicMapper.dataLabel(whereSql);
                case "3" -> dynamicMapper.insertLab(list);
                case "4" -> dynamicMapper.dataLabelNew(whereSql);
                case "5" -> dynamicMapper.updateLab(list);
                case "6" -> dynamicMapper.dataLabelSingle(whereSql);
                case "7" -> dynamicMapper.updateLabOrigin(list);
                case "8" -> dynamicMapper.dataRegister(whereSql);
                default -> null;
            };
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}

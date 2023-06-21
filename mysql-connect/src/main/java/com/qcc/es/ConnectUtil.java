package com.qcc.es;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import java.net.ConnectException;
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
        "=UTF-8&useSSL=false&serverTimezone=CTT&allowMultiQueries=true&zeroDateTimeBehavior=CONVERT_TO_NULL&autoReconnect=true&connectTimeout=3000&socketTimeout=3000&autoReconnect=true";

    public static SqlSessionFactory mysqlClient(MysqlServer mysqlServer) throws Exception {

        PooledDataSource pooledDataSource = new PooledDataSource();
        pooledDataSource.setDriver("com.mysql.cj.jdbc.Driver");
        pooledDataSource.setUrl(String.format(URL_PATTER,mysqlServer.getUrl(),mysqlServer.getDatabase()));
        pooledDataSource.setUsername(mysqlServer.getUsername());
        pooledDataSource.setPassword(mysqlServer.getPassword());
        pooledDataSource.setLoginTimeout(28800);
        pooledDataSource.setPoolTimeToWait(28800);
        pooledDataSource.setDefaultNetworkTimeout(28800);

        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        //配置mapper路径
        Resource[] resources = resolver.getResources("classpath:/DynamicMapper.xml");

        MybatisSqlSessionFactoryBean sqlSessionFactoryBean = new MybatisSqlSessionFactoryBean();
        sqlSessionFactoryBean.setMapperLocations(resources);
        sqlSessionFactoryBean.setDataSource(pooledDataSource);

        MybatisConfiguration mybatisConfiguration = new MybatisConfiguration();
        mybatisConfiguration.setDefaultStatementTimeout(28800);

        sqlSessionFactoryBean.setConfiguration(mybatisConfiguration);

        SqlSessionFactory sqlSessionFactory = sqlSessionFactoryBean.getObject();

        if (sqlSessionFactory == null) {
            throw new ConnectException("连接失败");
        }

        return sqlSessionFactory;
    }

    public static Object execute(ExecuteParams executeParams) {
        try (SqlSession sqlSession = mysqlClient(executeParams.mysqlServer()).openSession()) {
            if (sqlSession == null) {
                return null;
            }

            DynamicMapper dynamicMapper = sqlSession.getMapper(DynamicMapper.class);


            return switch (executeParams.method()) {
                case "1" -> dynamicMapper.tenderInfo(executeParams.whereSql());
                case "2" -> dynamicMapper.dataLabel(executeParams.whereSql());
                case "3" -> dynamicMapper.insertLab(executeParams.list());
                case "4" -> dynamicMapper.dataLabelNew(executeParams.whereSql());
                case "5" -> dynamicMapper.updateLab(executeParams.list());
                case "6" -> dynamicMapper.dataLabelSingle(executeParams.whereSql());
                case "7" -> dynamicMapper.updateLabOrigin(executeParams.list());
                case "8" -> dynamicMapper.dataRegister(executeParams.whereSql());
                case "9" -> dynamicMapper.dataRating(executeParams.whereSql());
                case "10" -> dynamicMapper.dataHonor(executeParams.whereSql());
                case "11" ->
                    dynamicMapper.dataCompany(executeParams.table(), executeParams.list1());
                case "12" -> dynamicMapper.dataHonorCount();
                case "13" ->
                    dynamicMapper.dataHonorPage(executeParams.from(), executeParams.size());
                default -> null;
            };
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}

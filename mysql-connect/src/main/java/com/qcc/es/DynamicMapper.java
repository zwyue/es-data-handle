package com.qcc.es;


import java.util.List;
import java.util.Map;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * <p>
 * 数据源信息表-rds Mapper 接口
 * </p>
 *
 * @author qcc
 * @since 2022-04-13
 */
@Mapper
public interface DynamicMapper {

    List<Map<String,Object>> tenderInfo(String whereSql);

    List<Map<String,Object>> dataLabel(String whereSql);

    List<SearchDataLabel> dataLabelNew(String whereSql);

    List<Map<String,Object>> dataLabelSingle(String whereSql);

    List<Map<String,Object>> dataRegister(String whereSql);

    int insertLab(@Param("list") List<Map<String,Object>> list);

    int updateLab(@Param("list") List<Map<String,Object>> list);

    int updateLabOrigin(@Param("list") List<Map<String,Object>> list);
}

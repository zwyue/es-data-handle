package com.qcc.es.records;

import java.util.Map;

public record EsDataInfo(String esId, String route, Map<String, Object> sourceMap) {
}

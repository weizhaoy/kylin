/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.collect.Maps;

/**
 * Handle query requests.
 * 
 * @author xduo
 */
@Controller
public class QueryController extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public SQLResponse query(@RequestBody PrepareSqlRequest sqlRequest) {
        return queryService.doQueryWithCache(sqlRequest);
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData
    @RequestMapping(value = "/query/prestate", method = RequestMethod.POST, produces = "application/json")
    @ResponseBody
    public SQLResponse prepareQuery(@RequestBody PrepareSqlRequest sqlRequest) {
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return queryService.doQueryWithCache(sqlRequest);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public void saveQuery(@RequestBody SaveSqlRequest sqlRequest) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(sqlRequest.getName(), sqlRequest.getProject(), sqlRequest.getSql(),
                sqlRequest.getDescription());

        queryService.saveQuery(creator, newQuery);
    }

    @RequestMapping(value = "/saved_queries/{id}", method = RequestMethod.DELETE, produces = { "application/json" })
    @ResponseBody
    public void removeQuery(@PathVariable String id) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeQuery(creator, id);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public List<Query> getQueries(@RequestParam(value = "project", required = false) String project) throws IOException {
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        return queryService.getQueries(creator, project);
    }

    @RequestMapping(value = "/query/format/{format}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public void downloadQueryResult(@PathVariable String format, SQLRequest sqlRequest, HttpServletResponse response) {
        KylinConfig config = queryService.getConfig();
        Message msg = MsgPicker.getMsg();

        if ((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed())) {
            throw new ForbiddenException(msg.getEXPORT_RESULT_NOT_ALLOWED());
        }

        SQLResponse result = queryService.doQueryWithCache(sqlRequest);
        response.setContentType("text/" + format + ";charset=utf-8");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date now = new Date();
        String nowStr = sdf.format(now);
        response.setHeader("Content-Disposition", "attachment; filename=\"" + nowStr + ".result." + format + "\"");
        ICsvListWriter csvWriter = null;

        try {
            csvWriter = new CsvListWriter(response.getWriter(), CsvPreference.STANDARD_PREFERENCE);

            List<String> headerList = new ArrayList<String>();

            for (SelectedColumnMeta column : result.getColumnMetas()) {
                headerList.add(column.getLabel());
            }

            String[] headers = new String[headerList.size()];
            csvWriter.writeHeader(headerList.toArray(headers));

            for (List<String> row : result.getResults()) {
                csvWriter.write(row);
            }
        } catch (IOException e) {
            throw new InternalErrorException(e);
        } finally {
            IOUtils.closeQuietly(csvWriter);
        }
    }

    @RequestMapping(value = "/tables_and_columns", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public List<TableMeta> getMetadata(MetaRequest metaRequest) {
        try {
            return queryService.getMetadata(metaRequest.getProject());
        } catch (SQLException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    @RequestMapping(value = "/tables_and_columns_2", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public List<TableMetaWithType> getMetadata2(MetaRequest metaRequest) {
        try {
            return queryService.getMetadataV2(metaRequest.getProject());
        } catch (SQLException | IOException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Get queryable table (if exists): dimensions and measures with measure type (if exists)
     * @return
     */
    @RequestMapping(value = "/tables_and_columns/meta/{projectName}/{schemaName}/{tableName:.+}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    private Map<String, Object> getTableMetadata(@PathVariable String projectName, @PathVariable String schemaName, @PathVariable String tableName) {
        Map<String, Object> result = new HashMap<>();
        TableMetaWithType matchedTable = null;
        try{
            List<TableMetaWithType> tableMetaList = queryService.getMetadataV2(projectName);
            for (TableMetaWithType tableMeta : tableMetaList) {
                if (tableMeta.getTABLE_NAME().equalsIgnoreCase(tableName) ) {
                    logger.info("Table Name matches: {}, Schema: {} / {}", tableName, tableMeta.getTABLE_SCHEM(), schemaName);
                    if (tableMeta.getTABLE_SCHEM().equalsIgnoreCase(schemaName)) {
                        logger.info("Table matches: {}.{}", schemaName, tableName);
                        matchedTable = tableMeta;
                        break;
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }

        if (null == matchedTable) {
            throw new InternalErrorException("Cannot get matching queryable table: " + schemaName + "." + tableName);
        }

        // get all measures in the project, may create a method for this
        List<CubeInstance> cubeInstances = cubeService.listAllCubes(projectName);
        Set<MeasureDesc> measuresInProject = new HashSet<>();
        for (CubeInstance cubeInstance : cubeInstances) {
            if (cubeInstance.isReady() && cubeInstance.getDescriptor() != null) {
                measuresInProject.addAll(cubeInstance.getMeasures());
            }
        }

        // get all measures in the table, may create a method for this
        Set<MeasureDesc> measuresInTable = new HashSet<>();
        Map<String, Set<MeasureDesc>> columnMeasureMap = new HashMap<>();
        ParameterDesc parameterDesc;
        String value;
        String table;
        String column;
        for (MeasureDesc measureDesc : measuresInProject) {
            parameterDesc = measureDesc.getFunction().getParameter();
            if (parameterDesc.getType().equalsIgnoreCase("column")) {
                logger.info("GetColRef(): {}", parameterDesc.getColRef());
                value = parameterDesc.getValue();
                table = value.substring(0, value.indexOf('.'));
                logger.info("table name for measure {}: {}", measureDesc.getName(), table);
                if (tableName.equalsIgnoreCase(table)) {
                    measuresInTable.add(measureDesc);
                    if (!columnMeasureMap.containsKey(value)) {
                        columnMeasureMap.put(value, new HashSet<>());
                    }
                    logger.info("Measure: {}, \nDependentMeasureRef: {}, ", measureDesc.toString(), measureDesc.getDependentMeasureRef());
                    columnMeasureMap.get(value).add(measureDesc);
                }
            }
        }

        result.put("measures", columnMeasureMap);
        result.put("table_meta", matchedTable);

        // get dimensions and measures
//        for (ColumnMeta columnMeta : matchedTable.getColumns()) {
//            if (columnMeta instanceof ColumnMetaWithType) {
//                ColumnMetaWithType columnMetaWithType = (ColumnMetaWithType) columnMeta;
//                if (columnMetaWithType.getTYPE().contains(ColumnMetaWithType.columnTypeEnum.MEASURE)) {
//                    // find its measure type
//                    String colName = columnMetaWithType.getCOLUMN_NAME();
//                    Set<MeasureDesc> measuresForCol = new HashSet<>();
//                    for (MeasureDesc measureDesc : measuresInTable) {
//                        value = measureDesc.getFunction().getParameter().getValue();
//                        column = value.substring(value.indexOf('.') + 1);
//                        logger.info("column name for measure {}: {}", measureDesc.getName(), column);
//                        if (colName.equalsIgnoreCase(column)) {
//                            measuresForCol.add(measureDesc);
//                        }
//                    }
//                }
//            }
//        }
        return result;
    }


    /**
     *
     * @param runTimeMoreThan in seconds
     * @return
     */
    @RequestMapping(value = "/query/runningQueries", method = RequestMethod.GET)
    @ResponseBody
    public TreeSet<QueryContext> getRunningQueries(
            @RequestParam(value = "runTimeMoreThan", required = false, defaultValue = "-1") int runTimeMoreThan) {
        if (runTimeMoreThan == -1) {
            return QueryContextFacade.getAllRunningQueries();
        } else {
            return QueryContextFacade.getLongRunningQueries(runTimeMoreThan * 1000);
        }
    }

    @RequestMapping(value = "/query/{queryId}/stop", method = RequestMethod.PUT)
    @ResponseBody
    public void stopQuery(@PathVariable String queryId) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        logger.info("{} tries to stop the query: {}, but not guaranteed to succeed.", user, queryId);
        QueryContextFacade.stopQuery(queryId, "stopped by " + user);
    }

    public void setQueryService(QueryService queryService) {
        this.queryService = queryService;
    }
}

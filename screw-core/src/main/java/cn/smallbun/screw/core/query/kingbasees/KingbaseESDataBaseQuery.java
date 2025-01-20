/*
 * screw-core - 简洁好用的数据库表结构文档生成工具
 * Copyright © 2020 SanLi (qinggang.zuo@gmail.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.smallbun.screw.core.query.kingbasees;

import cn.smallbun.screw.core.exception.QueryException;
import cn.smallbun.screw.core.mapping.Mapping;
import cn.smallbun.screw.core.metadata.Column;
import cn.smallbun.screw.core.metadata.Database;
import cn.smallbun.screw.core.metadata.PrimaryKey;
import cn.smallbun.screw.core.query.AbstractDatabaseQuery;
import cn.smallbun.screw.core.query.kingbasees.model.KingbaseESColumnModel;
import cn.smallbun.screw.core.query.kingbasees.model.KingbaseESDatabaseModel;
import cn.smallbun.screw.core.query.kingbasees.model.KingbaseESPrimaryKeyModel;
import cn.smallbun.screw.core.query.kingbasees.model.KingbaseESTableModel;
import cn.smallbun.screw.core.util.Assert;
import cn.smallbun.screw.core.util.CollectionUtils;
import cn.smallbun.screw.core.util.ExceptionUtils;
import cn.smallbun.screw.core.util.JdbcUtils;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static cn.smallbun.screw.core.constant.DefaultConstants.PERCENT_SIGN;

/**
 * mariadb 数据库查询
 *
 * @author SanLi
 * Created by qinggang.zuo@gmail.com / 2689170096@qq.com on 2020/3/18 13:59
 */
public class KingbaseESDataBaseQuery extends AbstractDatabaseQuery {
    /**
     * 构造函数
     *
     * @param dataSource {@link DataSource}
     */
    public KingbaseESDataBaseQuery(DataSource dataSource) {
        super(dataSource);
    }

    /**
     * 获取数据库
     *
     * @return {@link Database} 数据库信息
     */
    @Override
    public Database getDataBase() throws QueryException {
        KingbaseESDatabaseModel model = new KingbaseESDatabaseModel();
        model.setDatabase(getCatalog());
        return model;
    }

    @Override
    public List<KingbaseESTableModel> getTables() {
        ResultSet resultSet = null;
        try {
            //查询
            resultSet = getMetaData().getTables(getCatalog(), getSchema(), PERCENT_SIGN,
                    new String[] { "TABLE" });
            //映射
            return Mapping.convertList(resultSet, KingbaseESTableModel.class);
        } catch (SQLException e) {
            throw ExceptionUtils.mpe(e);
        } finally {
            JdbcUtils.close(resultSet, connection);
        }
    }


    /**
     * 获取列信息
     *
     * @param table {@link String} 表名
     * @return {@link List} 表字段信息
     */
    @SuppressWarnings("DuplicatedCode")
    @Override
    public List<KingbaseESColumnModel> getTableColumns(String table) throws QueryException {
        Assert.notEmpty(table, "Table name can not be empty!");
        ResultSet resultSet = null;
        try {
            // 查询
            resultSet = getMetaData().getColumns(getCatalog(), getSchema(), table, PERCENT_SIGN);
            // 映射
            List<KingbaseESColumnModel> list = Mapping.convertList(resultSet, KingbaseESColumnModel.class);
            // 处理并获取唯一表名
            List<String> tableNames = list.stream()
                    .map(KingbaseESColumnModel::getTableName)
                    .distinct()
                    .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(columnsCaching)) {
                // 查询全部
                if (table.equals(PERCENT_SIGN)) {
                    // 获取全部表列信息SQL
                    String sql = "SELECT A.TABLE_NAME, A.COLUMN_NAME, A.DATA_TYPE, A.CHARACTER_MAXIMUM_LENGTH AS COLUMN_LENGTH FROM INFORMATION_SCHEMA.COLUMNS A WHERE A.TABLE_SCHEMA = '%s'";
                    PreparedStatement statement = prepareStatement(String.format(sql, getDataBase().getDatabase()));
                    resultSet = statement.executeQuery();
                } else {
                    // 单表查询
                    String sql = "SELECT A.TABLE_NAME, A.COLUMN_NAME, A.DATA_TYPE, A.CHARACTER_MAXIMUM_LENGTH AS COLUMN_LENGTH FROM INFORMATION_SCHEMA.COLUMNS A WHERE A.TABLE_SCHEMA = '%s' AND A.TABLE_NAME = '%s'";
                    resultSet = prepareStatement(String.format(sql, getDataBase().getDatabase(), table)).executeQuery();
                }
                List<KingbaseESColumnModel> inquires = Mapping.convertList(resultSet, KingbaseESColumnModel.class);
                // 处理列，表名为key，列名为值
                tableNames.forEach(name -> columnsCaching.put(name, inquires.stream()
                        .filter(i -> i.getTableName().equals(name))
                        .collect(Collectors.toList())));
            }
            // 处理备注信息
            list.forEach(i -> {
                List<Column> columns = columnsCaching.get(i.getTableName());
                columns.forEach(j -> {
                    if (i.getColumnName().equals(j.getColumnName()) && i.getTableName().equals(j.getTableName())) {
                        i.setColumnType(j.getColumnType());
                        i.setColumnLength(j.getColumnLength());
                    }
                });
            });
            return list;
        } catch (SQLException e) {
            throw ExceptionUtils.mpe(e);
        } finally {
            JdbcUtils.close(resultSet);
        }
    }

    /**
     * 获取所有列信息
     *
     * @return {@link List} 表字段信息
     * @throws QueryException QueryException
     */
    @Override
    public List<? extends Column> getTableColumns() throws QueryException {
        //获取全部列
        return getTableColumns(PERCENT_SIGN);
    }

    /**
     * 根据表名获取主键
     *
     * @param table {@link String}
     * @return {@link List}
     * @throws QueryException QueryException
     */
    @Override
    public List<? extends PrimaryKey> getPrimaryKeys(String table) throws QueryException {
        ResultSet resultSet = null;
        try {
            //查询
            resultSet = getMetaData().getPrimaryKeys(getCatalog(), getSchema(), table);
            //映射
            return Mapping.convertList(resultSet, KingbaseESPrimaryKeyModel.class);
        } catch (SQLException e) {
            throw ExceptionUtils.mpe(e);
        } finally {
            JdbcUtils.close(resultSet, this.connection);
        }
    }

    /**
     * 根据表名获取主键
     *
     * @return {@link List}
     * @throws QueryException QueryException
     */
    @Override
    public List<? extends PrimaryKey> getPrimaryKeys() throws QueryException {
        ResultSet resultSet = null;
        try {
            // 假设 KingbaseES 使用不同的表来存储主键信息
            // 请根据实际的 KingbaseES 系统表结构调整此查询
            String sql = "SELECT A.TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.CONSTRAINT_NAME AS PK_NAME " +
                    "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A " +
                    "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME " +
                    "AND A.TABLE_SCHEMA = B.TABLE_SCHEMA " +
                    "AND A.TABLE_NAME = B.TABLE_NAME " +
                    "WHERE B.CONSTRAINT_TYPE = 'PRIMARY KEY' AND A.TABLE_SCHEMA = '%s'";
            String database = getDataBase().getDatabase();
            resultSet = prepareStatement(String.format(sql, database)).executeQuery();
            return Mapping.convertList(resultSet, KingbaseESPrimaryKeyModel.class);
        } catch (SQLException e) {
            throw new QueryException(e);
        } finally {
            JdbcUtils.close(resultSet);
        }
    }
}

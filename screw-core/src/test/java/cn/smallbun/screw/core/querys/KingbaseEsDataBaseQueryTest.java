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
package cn.smallbun.screw.core.querys;

import cn.smallbun.screw.core.Configuration;
import cn.smallbun.screw.core.common.Properties;
import cn.smallbun.screw.core.engine.EngineConfig;
import cn.smallbun.screw.core.engine.EngineFileType;
import cn.smallbun.screw.core.engine.EngineTemplateType;
import cn.smallbun.screw.core.exception.QueryException;
import cn.smallbun.screw.core.execute.DocumentationExecute;
import cn.smallbun.screw.core.metadata.Column;
import cn.smallbun.screw.core.metadata.Database;
import cn.smallbun.screw.core.metadata.PrimaryKey;
import cn.smallbun.screw.core.metadata.Table;
import cn.smallbun.screw.core.process.ProcessConfig;
import cn.smallbun.screw.core.query.DatabaseQuery;
import cn.smallbun.screw.core.query.DatabaseQueryFactory;
import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL
 *
 * @author SanLi
 * Created by qinggang.zuo@gmail.com / 2689170096@qq.com on 2020/3/29 22:54
 */
public class KingbaseEsDataBaseQueryTest implements Properties {
    /**
     * 日志
     */
    private final Logger  logger = LoggerFactory.getLogger(this.getClass());
    /**
     * DataBaseQuery
     */
    private DatabaseQuery query;

    /**
     * before
     */
    @BeforeEach
    void before() throws IOException {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(getDriver());
        config.setJdbcUrl(getUrl());
        config.setUsername(getUserName());
        config.setPassword(getPassword());
        DataSource dataSource = new HikariDataSource(config);
        query =

                new DatabaseQueryFactory(dataSource).newInstance();
    }

    /**
     * 查询数据库
     */
    @Test
    void databases() throws QueryException {
        Database database = query.getDataBase();
        logger.info(JSON.toJSONString(database, true));
    }

    /**
     * 查询表
     */
    @Test
    void tables() throws QueryException {
        List<? extends Table> tables = query.getTables();
        logger.info(JSON.toJSONString(tables, true));
    }

    /**
     * 查询列
     */
    @Test
    void column() throws QueryException {
        List<? extends Column> columns = query.getTableColumns("tca_device");
        logger.info(JSON.toJSONString(columns, true));
    }

    /**
     * 查询主键
     */
    @Test
    void primaryKeys() {
        List<? extends PrimaryKey> primaryKeys = query.getPrimaryKeys();
        logger.info(JSON.toJSONString(primaryKeys, true));
    }

    /**
     * 获取配置文件
     * @return {@link java.util.Properties}
     */
    @Override
    public String getConfigProperties() {
        return System.getProperty("user.dir")
               + "/src/main/resources/properties/kingbase.properties";
    }


    public static void main(String[] args) {
        documentGeneration();
    }

    /**
     * 文档生成
     */
    static void documentGeneration() {
        //数据源
        //jdbc:kingbase8://192.168.50.53:54321
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("com.kingbase8.Driver");
        hikariConfig.setJdbcUrl("jdbc:kingbase8://192.168.50.53:54321/zb-test-data?currentSchema=business_data");
        hikariConfig.setUsername("system");
        hikariConfig.setPassword("123456");
        //设置可以获取tables remarks信息
        hikariConfig.addDataSourceProperty("useInformationSchema", "true");
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setMaximumPoolSize(5);
        DataSource dataSource = new HikariDataSource(hikariConfig);

        //本地输出路径
        String fileOutputDir = "C:\\Users\\41524\\Desktop";
        //生成配置
        EngineConfig engineConfig = EngineConfig.builder()
                //生成文件路径
                .fileOutputDir(fileOutputDir)
                //打开目录
                .openOutputDir(true)
                //文件类型
                .fileType(EngineFileType.WORD)
                //生成模板实现
                .produceType(EngineTemplateType.freemarker)
                .build();

        //忽略表
        ArrayList<String> ignoreTableName = new ArrayList<>();
        ignoreTableName.add("test_user");
        ignoreTableName.add("test_group");
        ignoreTableName.add("test_group");
        //忽略表前缀
        ArrayList<String> ignorePrefix = new ArrayList<>();
        ignorePrefix.add("t_");
        //忽略表后缀
        ArrayList<String> ignoreSuffix = new ArrayList<>();
        ignoreSuffix.add("_test");
        ProcessConfig processConfig = ProcessConfig.builder()
                //指定生成逻辑、当存在指定表、指定表前缀、指定表后缀时，将生成指定表，其余表不生成、并跳过忽略表配置
                //根据名称指定表生成
                .designatedTableName(new ArrayList<>())
                //根据表前缀生成
                .designatedTablePrefix(new ArrayList<>())
                //根据表后缀生成
                .designatedTableSuffix(new ArrayList<>())
                //忽略表名
                .ignoreTableName(ignoreTableName)
                //忽略表前缀
                .ignoreTablePrefix(ignorePrefix)
                //忽略表后缀
                .ignoreTableSuffix(ignoreSuffix).build();
        //配置
        Configuration config = Configuration.builder()
                //版本
                .version("1.0.0")
                //描述
                .description("数据库设计文档生成")
                //数据源
                .dataSource(dataSource)
                //生成配置
                .engineConfig(engineConfig)
                //生成配置
                .produceConfig(processConfig)
                .build();
        //执行生成
        new DocumentationExecute(config).execute();
    }
}

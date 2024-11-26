package com.lakesoul.Asstes.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SourceOptions {

    public static final ConfigOption<String> SOURCE_DB_DB_NAME = ConfigOptions
            .key("source_db.db_name")
            .stringType()
            .noDefaultValue()
            .withDescription("source database name");


    public static final ConfigOption<String> SOURCE_DB_USER = ConfigOptions
            .key("source_db.user")
            .stringType()
            .noDefaultValue()
            .withDescription("source database user_name");

    public static final ConfigOption<String> SOURCE_DB_PASSWORD = ConfigOptions
            .key("source_db.password")
            .stringType()
            .noDefaultValue()
            .withDescription("source database access password");


    public static final ConfigOption<String> SOURCE_DB_HOST = ConfigOptions
            .key("source_db.host")
            .stringType()
            .noDefaultValue()
            .withDescription("source database access host_name");

    public static final ConfigOption<Integer> SOURCE_DB_PORT = ConfigOptions
            .key("source_db.port")
            .intType()
            .noDefaultValue()
            .withDescription("source database access port");

    public static final ConfigOption<Integer> SPLIT_SIZE = ConfigOptions
            .key("splitSize")
            .intType()
            .defaultValue(1024)
            .withDescription("split size of flink postgresql cdc");

    public static final ConfigOption<String> SLOT_NAME = ConfigOptions
            .key("slotName")
            .stringType()
            .noDefaultValue()
            .withDescription("slot name of pg");

    public static final ConfigOption<String> PLUG_NAME = ConfigOptions
            .key("plugName")
            .stringType()
            .noDefaultValue()
            .withDescription("plug name of postgresql");

    public static final ConfigOption<String> SCHEMA_LIST = ConfigOptions
            .key("schemaList")
            .stringType()
            .noDefaultValue()
            .withDescription("");


}

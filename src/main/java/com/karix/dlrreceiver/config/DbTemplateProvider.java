package com.karix.dlrreceiver.config;

import com.karix.commonutil.common.DBBasedProperties;
import com.karix.commonutil.common.DataSourceFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DbTemplateProvider {

	@Bean(name = "confProps")
	DBBasedProperties createConfProps(@Value("${conf_location}") String path) {
		DBBasedProperties confProps = new DBBasedProperties();
		confProps.setConfigLocation(path);
		confProps.loadProperties();
		return confProps;
	}

	@Bean(name = "custDBTemplate")
	NamedParameterJdbcTemplate loadCustDBTemplate(@Value("${ds.custdb.file}") String path) {
		return createNamedParameterJdbcTemplate(path);
	}

	@Bean(name = "msgdbNamedJdbcTemplate")
	NamedParameterJdbcTemplate loadMsgDBTemplate(@Value("${ds.msgdb.file}") String path) {
		return createNamedParameterJdbcTemplate(path);
	}

	private NamedParameterJdbcTemplate createNamedParameterJdbcTemplate(String propertyFilePath) {
		var dataSource = new DataSourceFactory().loadDataSourceFromPropertyFile(propertyFilePath);
		return new NamedParameterJdbcTemplate(dataSource);
	}

	@Bean(name = "dataSourceConfig")
	public NamedParameterJdbcTemplate loadRcmTemplate(@Value("${ds.configdb.file}") String path) {

		DataSourceFactory dsf = new DataSourceFactory();
		DataSource dataSource = dsf.loadDataSourceFromPropertyFile(path);

		return new NamedParameterJdbcTemplate(dataSource);
	}
}

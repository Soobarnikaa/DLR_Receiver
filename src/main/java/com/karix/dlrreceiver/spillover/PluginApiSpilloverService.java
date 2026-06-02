package com.karix.dlrreceiver.spillover;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class PluginApiSpilloverService {

	private static final Logger log = LogManager.getLogger(PluginApiSpilloverService.class);

    private final NamedParameterJdbcTemplate namedJdbcTemplate;

	private static final String INSERT_SQL = "INSERT INTO msgdb.plugin_dlrapi_spillover "
			+ "(ackid, client_id, longbase_mid, type, payload, channel, category) "
			+ "VALUES (:ackid, :client_id, :longbase_mid ,:type, :payload, :channel, :category)";

	public PluginApiSpilloverService(
			@Qualifier("msgdbNamedJdbcTemplate") NamedParameterJdbcTemplate namedJdbcTemplate) {
		this.namedJdbcTemplate = namedJdbcTemplate;
	}

	public void insertSpillover(String ackId, String mid, String clientId, String channel,String payload,
			int category, String type) {

		try {

			Map<String, Object> params = new HashMap<>();
			params.put("ackid", ackId);
			params.put("client_id",clientId);
			params.put("longbase_mid",mid);
			params.put("type",type);
			params.put("payload", payload);
			params.put("channel",channel);
			params.put("category",category);

			namedJdbcTemplate.update(INSERT_SQL, params);

			log.warn("Inserted record into plugin_dlrapi_spillover. ackId={}", ackId);

		} catch (DataAccessException dae) {
			log.error("CRITICAL: Spillover DB insert failed for ackId={} and payload is {}", ackId ,payload,dae);
		}
	}
}
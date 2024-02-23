package net.ulukai.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public abstract class JsonKeyToHeader<R extends ConnectRecord<R>> implements Transformation<R> {

	private static final Logger logger = LoggerFactory.getLogger(JsonKeyToHeader.class);

	public static final String OVERVIEW_DOC =
		"SMT to extract JSON part to a value header with JSON key as Header key";

	private interface ConfigName {
		String HEADER_KEY_PREFIX = "header.key.prefix";
		String HEADER_KEY_PATH = "header.key.path";
	}

	public static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(ConfigName.HEADER_KEY_PREFIX, ConfigDef.Type.STRING, "key.prefix", ConfigDef.Importance.LOW, "Prefix value to key header")
			.define(ConfigName.HEADER_KEY_PATH, ConfigDef.Type.STRING, "key.path", ConfigDef.Importance.HIGH, "Path to extract from JSON to header");

	public String headerKeyPrefix = "";
	public String headerJsonPath = null;
	
	@Override
	public void configure(Map<String, ?> props) {
		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
		
		String value = config.getString(ConfigName.HEADER_KEY_PREFIX);
		if ( 0 != value.length() ) {
			this.headerKeyPrefix = value;
			logger.info("Key of header prefix will be {}", this.headerKeyPrefix);
		}

		value = config.getString(ConfigName.HEADER_KEY_PATH);
		if ( 0 != value.length() ) {
			this.headerJsonPath = value;
			logger.info("Value path of header value will be {}", this.headerJsonPath);
		} else {
			String errorMsg = "Value path of header value is missing";
			logger.error(errorMsg);
			throw new IllegalArgumentException(errorMsg);
		}
	}

	public String getJsonNodeContent(String json) {
		String nodeContent = null;
		
		if (0 < this.headerJsonPath.length()) {
			try {
				ObjectMapper objMapper = new ObjectMapper();
				JsonNode rootNode = objMapper.readTree(json);
				nodeContent = rootNode.path(this.headerJsonPath).asText();
			} catch (Exception e) {
				logger.debug("failed to find key path in json");
			}
		}
		return nodeContent;
	}

	abstract public R apply(R record);
	
	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void close() {
	}

	public static class Key<R extends ConnectRecord<R>> extends JsonKeyToHeader<R> {
		@Override
		public R apply(R record) {
			R result = record;
			if ( null != this.headerJsonPath) {
				String value = null;
				if ( record.keySchema().equals(Schema.BYTES_SCHEMA) 
						|| record.keySchema().equals(Schema.STRING_SCHEMA) ) {
					value = new String(record.key().toString());
				} else {
					logger.debug("Schema of value is planned to be converted to string! [keySchema = " + record.keySchema().toString() + " ]");
				}
				if ( null != value ) {
					String subNodeContent = this.getJsonNodeContent(value);
					if ( null != subNodeContent ) {
						Headers newHeaders = record.headers().duplicate();
						newHeaders.addString(this.headerKeyPrefix+"."+this.headerJsonPath, subNodeContent);
						logger.debug("subNodeContent=[{}]", subNodeContent);
						result = record.newRecord(
								record.topic(),
								record.kafkaPartition(),
								record.keySchema(),
								record.key(),
								record.valueSchema(),
								record.value(),
								record.timestamp(),
								newHeaders);
					}
				}
			}
		    return result;
		}
	}

	public static class Value<R extends ConnectRecord<R>> extends JsonKeyToHeader<R> {
		@Override
		public R apply(R record) {
			R result = record;
			if ( null != this.headerJsonPath) {
				String value = null;
				if ( record.valueSchema().equals(Schema.BYTES_SCHEMA) 
						|| record.valueSchema().equals(Schema.STRING_SCHEMA) ) {
					value = new String(record.value().toString());
				} else {
					logger.debug("Schema of value is planned to be converted to string! [valueSchema = " + record.valueSchema().toString() + " ]");
				}
				if ( null != value ) {
					String subNodeContent = this.getJsonNodeContent(value);
					if ( null != subNodeContent ) {
						Headers newHeaders = record.headers().duplicate();
						newHeaders.addString(this.headerKeyPrefix+"."+this.headerJsonPath, subNodeContent);
						logger.debug("subNodeContent=[{}]", subNodeContent);
						result = record.newRecord(
								record.topic(),
								record.kafkaPartition(),
								record.keySchema(),
								record.key(),
								record.valueSchema(),
								record.value(),
								record.timestamp(),
								newHeaders);
					}
				}
			}
		    return result;
		}
	}
}

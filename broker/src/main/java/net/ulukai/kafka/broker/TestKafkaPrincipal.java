package net.ulukai.kafka.broker;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class TestKafkaPrincipal extends KafkaPrincipal {

	public TestKafkaPrincipal(String principalType, String name, boolean tokenAuthenticated) {
		super(principalType, name, tokenAuthenticated);
	}
	
	public TestKafkaPrincipal(String principalType, String name) {
		super(principalType, name);
	}
	
}

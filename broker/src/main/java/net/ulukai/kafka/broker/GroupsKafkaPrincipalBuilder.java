package net.ulukai.kafka.broker;
//Based on package com.opencore.kafka;

import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.SerializationException;


public class GroupsKafkaPrincipalBuilder implements KafkaPrincipalBuilder, KafkaPrincipalSerde, Configurable {
  private Logger logger;

  private String certificateUserField = "CN";
  private String certificateGroupField = "OU";

  @Override
  public KafkaPrincipal build(AuthenticationContext context) {
    // Create a base principal by using the DefaultPrincipalBuilder
    GroupsKafkaPrincipal basePrincipal = new GroupsKafkaPrincipal(KafkaPrincipal.ANONYMOUS);

    // Only Ssl is supported here for group calculation, PLAINTEXT is anonymous
    if (context instanceof PlaintextAuthenticationContext) {
    	// No security at all, nor user declaration
    	// use default anonymous
    	logger.debug("called with Plaintext context, so anonymous is used");
    } else if (context instanceof SslAuthenticationContext) {
    	logger.debug(this.getClass().getName() + " received SSL context - " + ((SslAuthenticationContext)context).toString());
    	SSLSession sslSession = ((SslAuthenticationContext)context).session();
    	try {
        	Principal principal = sslSession.getPeerPrincipal();
            if (!(principal instanceof X500Principal) || principal == KafkaPrincipal.ANONYMOUS) {
            } else {
            	String dn = principal.getName();
            	String userName = getUserFromCertificate(dn);
            	basePrincipal = new GroupsKafkaPrincipal(KafkaPrincipal.USER_TYPE, userName, getGroupsFromDN(dn));
            }        
		} catch (SSLPeerUnverifiedException e) {
			// TODO Auto-generated catch block
			logger.error(this.getClass().getName() + " failed to use SSL context, fallback to anonymous");
			e.printStackTrace();
        } 
    } else {
    	logger.warn("Authentication Context [" + context.toString() + "] not supported for " + this.getClass().toString());
    }
    return basePrincipal;
  }

  private List<KafkaPrincipal> getGroupsFromDN(String userName) {
    List<KafkaPrincipal> groupPrincipals = new ArrayList<KafkaPrincipal>();
    // Add user principal to list as well to make later matching easier
      groupPrincipals.add(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, userName));

      logger.debug("Resolving groups for user [" + userName + "]");
      List<String> groups = groupsFromCN(userName);
      StringBuilder sb = new StringBuilder();
      for (String group : groups) {
          sb.append(group);
          sb.append(",");
      }
      sb.deleteCharAt(sb.length()-1);
      logger.warn("Got list of groups for user [" + userName + "] : [" + sb.toString() + "]");
      for (String group : groups) {
        groupPrincipals.add(new KafkaPrincipal("Group", group));
      }
    return groupPrincipals;
  }

  private String getUserFromCertificate(String certificateString) {
	    // For a SslContext the user will look like CN=username;OU=...;DN=...
	    try {
	      LdapName certificateDetails = new LdapName(certificateString);
	      for (Rdn currentRdn : certificateDetails.getRdns()) {
	        if (currentRdn.getType().equalsIgnoreCase(certificateUserField)) {
	          certificateString = currentRdn.getValue().toString();
	        }
	      }
	    } catch (InvalidNameException e) {
	      logger.warn("Error extracting username from String " + certificateString + ": " + e.getMessage());
	    }
	    return certificateString;
	  }

  private List<String> groupsFromCN(String certificateString) {
	  List<String> groups = new ArrayList<String>();
	    // For a SslContext the user will look like CN=username;OU=...;DN=...
	    try {
	      LdapName certificateDetails = new LdapName(certificateString);
	      for (Rdn currentRdn : certificateDetails.getRdns()) {
	        if (currentRdn.getType().equalsIgnoreCase(certificateGroupField)) {
	          groups.add(currentRdn.getValue().toString());
	        }
	      }
	    } catch (InvalidNameException e) {
	      logger.warn("Error extracting groups from String " + certificateString + ": " + e.getMessage());
	    }
	    return groups;
	  }

  @Override
  public void configure(Map<String, ?> configs) {
	 this.logger = LoggerFactory.getLogger(GroupsKafkaPrincipalBuilder.class.getSimpleName());
	 this.logger.info("Configuration called");
	 
    // Check if options for the principalbuilder were specified in the broker config
    if (configs.containsKey("principal.builder.options.groupKey")) {
      this.certificateGroupField = (String) configs.get("principal.builder.options.groupKey");
    }
  }

	@Override
	public byte[] serialize(KafkaPrincipal principal) throws SerializationException {
		this.logger.debug("serializer called");
		JsonNodeFactory factory = JsonNodeFactory.instance;
		ObjectNode mainNode = factory.objectNode();
		ObjectNode rootNode = factory.objectNode();
		mainNode.put("type", principal.getPrincipalType()).put("name", principal.getName());
		rootNode.set("main", mainNode);
		if ( principal instanceof GroupsKafkaPrincipal ) {
			ArrayNode arrayNode = factory.arrayNode();
			for ( KafkaPrincipal kp : ((GroupsKafkaPrincipal)principal).getPrincipalList()) {
				arrayNode.add(factory.objectNode().put("type", kp.getPrincipalType()).put("name", kp.getName()));
			}
			rootNode.set("all", arrayNode);
		}
		this.logger.info("Serialize result [" + rootNode.toPrettyString() + "]");
		return rootNode.toPrettyString().getBytes();
	}
	
	@Override
	public KafkaPrincipal deserialize(byte[] bytes) throws SerializationException {
		this.logger.debug("deserializer called");
		KafkaPrincipal principal = null;
		try {
			this.logger.info("Deserialize source [" + new String(bytes) + "]");
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readTree(new String(bytes));
		      String mainType = jsonNode.path("main.type").asText();
		      String mainName = jsonNode.path("main.name").asText();
		      List<KafkaPrincipal> allKP = new ArrayList<KafkaPrincipal>();
		      ArrayNode an = (ArrayNode)jsonNode.get("all");
		      for (JsonNode jn : an) {
		    	  allKP.add(new KafkaPrincipal(jn.get("type").asText(), jn.get("name").asText()));
		      }
		      principal = new GroupsKafkaPrincipal(mainType, mainName, allKP);
		} catch (JsonMappingException e) {
			logger.error("failed to deserialize");
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			logger.error("failed to deserialize");
			e.printStackTrace();
		}
		return principal;
	}
}
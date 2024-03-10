package net.ulukai.kafka.broker;
//Based on package com.opencore.kafka;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GroupsKafkaPrincipal extends KafkaPrincipal {
  protected List<KafkaPrincipal> allPrincipals = new ArrayList<KafkaPrincipal>();

  public GroupsKafkaPrincipal(String principalType, String name) {
    super(principalType, name);
    allPrincipals.clear();
    allPrincipals.add(this);
  }

  public GroupsKafkaPrincipal(KafkaPrincipal kafkaPrincipal) {
    this(kafkaPrincipal.getPrincipalType(), kafkaPrincipal.getName());
  }

  public GroupsKafkaPrincipal(String principalType, String name, List<KafkaPrincipal> allPrincipals) {
    this(principalType, name);
    this.allPrincipals.addAll(allPrincipals);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof GroupsKafkaPrincipal){
    	return ( ( this.allPrincipals.containsAll(((GroupsKafkaPrincipal)o).allPrincipals) )
    			&& ( this.allPrincipals.size() == ((GroupsKafkaPrincipal)o).allPrincipals.size() ) );    			
//      allPrincipals.retainAll(((GroupsKafkaPrincipal)o).allPrincipals);
//      return allPrincipals.size() > 0;
    } else if (o instanceof KafkaPrincipal) {
      return allPrincipals.contains(o);
    }
    // For security reason, default result is NOK
    return false;
  }

  public List<KafkaPrincipal> getPrincipalList() {
    return allPrincipals;
  }
}
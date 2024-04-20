package net.ulukai.kafka.broker;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupsKafkaPrincipal extends KafkaPrincipal {
  protected List<KafkaPrincipal> allPrincipals = new ArrayList<KafkaPrincipal>();

  public GroupsKafkaPrincipal(KafkaPrincipal kafkaPrincipal) {
	    this(kafkaPrincipal.getPrincipalType(), kafkaPrincipal.getName());
	  }

  public GroupsKafkaPrincipal(String principalType, String name) {
	  this(principalType, name, null);
  }

  public GroupsKafkaPrincipal(String principalType, String name, List<KafkaPrincipal> addAllPrincipals) {
	    super(principalType, name);
	    this.allPrincipals.clear();
	    this.allPrincipals.add(new KafkaPrincipal(principalType, name));
		Logger logger = LoggerFactory.getLogger(GroupsKafkaPrincipal.class.getSimpleName());
		logger.debug("GroupsKafkaPrincipal 00 constuctor : "+principalType+"|"+name);
		if ( null != addAllPrincipals ) {
			logger.debug("GroupsKafkaPrincipal 01 constuctor : addAllP length = " + addAllPrincipals.size());
			for (KafkaPrincipal kp : addAllPrincipals) {
				logger.debug("GroupsKafkaPrincipal 02 constuctor : kp = ["+kp.getClass().toString()+":"+kp.toString()+"]");
				boolean kpEqualsThis = this.equals(kp);
				logger.debug("GroupsKafkaPrincipal 03 constuctor : this equals kp = " + kpEqualsThis);
				if ( ! kpEqualsThis ) {
					logger.debug("GroupsKafkaPrincipal 04 constuctor : Not equal super=["+this.getClass().toString()+":"+this.toString()+"] and kp=["+kp.getClass().toString()+":"+kp.toString()+"]");
					this.allPrincipals.add(kp);
				}
			}
		}
		logger.debug("GroupsKafkaPrincipal 04 constuctor : created as [" + this.toStringFull() + "]");
  }
  
  @Override
  public boolean equals(Object o) {
	if ( null == o ) {
		return false;
	}
	if ( this == o ) {
		return true;
	}

	if ( o.getClass().equals(KafkaPrincipal.class) ) {
    	KafkaPrincipal inKpObj = (KafkaPrincipal)o;
    	for (KafkaPrincipal inKpThis : this.allPrincipals) {
    		if (inKpThis.equals(inKpObj)) {
    			return true;
    		}
    	}
    } else if ( o.getClass().equals(GroupsKafkaPrincipal.class) ) {
    	GroupsKafkaPrincipal gkpObj = (GroupsKafkaPrincipal)o;
    	for ( KafkaPrincipal inKpObj : gkpObj.allPrincipals ) {
    		for ( KafkaPrincipal inKpThis : this.allPrincipals ) {
    			if ( inKpObj.equals(inKpThis) ) {
    				return true;
    			}
    		}
    	}
    }
    return false;
  }

  public List<KafkaPrincipal> getPrincipalList() {
    return allPrincipals;
  }
  
  public String toStringFull() {
	  String result = "";
	  String allP = "";
	  Logger logger = LoggerFactory.getLogger(GroupsKafkaPrincipal.class.getSimpleName());
	  logger.debug("Size of allP=" + this.allPrincipals.size());
	  for (KafkaPrincipal kp : this.allPrincipals) {
		  logger.debug(kp.toString());
		  allP += kp.toString() + ";";
	  }
	  //if (0 < allP.length()) allP = allP.substring(0,allP.length()-1);
	  result = "Main["
			  + super.toString()
			  + "] allPrincipals["
			  + allP
			  + "]";
	  logger.debug(result);
	  return result;
  }

  public String toString() {
	  return super.toString();
  }
}
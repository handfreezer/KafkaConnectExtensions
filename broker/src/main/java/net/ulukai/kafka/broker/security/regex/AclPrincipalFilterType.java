package net.ulukai.kafka.broker.security.regex;

import java.util.HashMap;
import java.util.Map;

public class AclPrincipalFilterType {
	public static enum aclPrincipalTypeFilter {
	    /**
	     * The principal type string used in ACLs that match a name by regex.
	     */
		PRINCIPAL_FILTER_TYPE_REGEX,
	    /**
	     * The principal type string used in ACLs that match a name by containing a substring.
	     */
		PRINCIPAL_FILTER_TYPE_CONTAINS,
	    /**
	     * The principal type string used in ACLs that match a name starting with a substring.
	     */
	    PRINCIPAL_FILTER_TYPE_STARTSWITH,
	    /**
	     * The principal type string used in ACLs that match a name ending with a substring.
	     */
	    PRINCIPAL_FILTER_TYPE_ENDSWITH,
	}
	
	public final static String ACL_STD_PREFIX_PRINCIPAL_FILTER_TYPE = "acl.std.prefix.principal.filter.type";
	private String prefixPrincipalFilterType = null;
	private Map<String, AclPrincipalFilterType.aclPrincipalTypeFilter> mappingPrincipalFilterType = 
			new HashMap<String, AclPrincipalFilterType.aclPrincipalTypeFilter>();
	
	private static AclPrincipalFilterType me = null;
	
	private AclPrincipalFilterType() {
		this.init("Acl");
	}
	
	private AclPrincipalFilterType(String prefixPrincipalFilterType) {
		this.init(prefixPrincipalFilterType);
	}
	
	private void init(String prefix) {
		this.prefixPrincipalFilterType = prefix;
		this.mappingPrincipalFilterType.put(this.prefixPrincipalFilterType+"Regex",
				AclPrincipalFilterType.aclPrincipalTypeFilter.PRINCIPAL_FILTER_TYPE_REGEX);
		this.mappingPrincipalFilterType.put(this.prefixPrincipalFilterType+"Contains",
				AclPrincipalFilterType.aclPrincipalTypeFilter.PRINCIPAL_FILTER_TYPE_CONTAINS);
		this.mappingPrincipalFilterType.put(this.prefixPrincipalFilterType+"StartsWith",
				AclPrincipalFilterType.aclPrincipalTypeFilter.PRINCIPAL_FILTER_TYPE_STARTSWITH);
		this.mappingPrincipalFilterType.put(this.prefixPrincipalFilterType+"EndsWith",
				AclPrincipalFilterType.aclPrincipalTypeFilter.PRINCIPAL_FILTER_TYPE_ENDSWITH);
	}

	public static AclPrincipalFilterType getInstance (Map<String, ?> configs) {
		if ( null == AclPrincipalFilterType.me ) {
	        Object configValue = configs.get(ACL_STD_PREFIX_PRINCIPAL_FILTER_TYPE);
	        if ( null != configValue )
	        	AclPrincipalFilterType.me = new AclPrincipalFilterType(configValue.toString().trim());
		}
		return AclPrincipalFilterType.getInstance();
    }
	
	public static AclPrincipalFilterType getInstance() {
		if ( null == AclPrincipalFilterType.me )
			AclPrincipalFilterType.me = new AclPrincipalFilterType();
		return AclPrincipalFilterType.me;
	}
	
	public aclPrincipalTypeFilter getAclPrincipalFilterType(String pkType) {
		if ( true == this.mappingPrincipalFilterType.containsKey(pkType) )
			return this.mappingPrincipalFilterType.get(pkType);
		else
			return null;
	}
}


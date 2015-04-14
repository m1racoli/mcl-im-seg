/**
 * 
 */
package zookeeper.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.net.util.IPAddressUtil;

/**
 *  from cassandra CASSANDRA-7431
 *
 */
@SuppressWarnings("restriction")
public class DnsUtil {

	private static final Logger logger = LoggerFactory.getLogger(DnsUtil.class);
	
	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		
		InetAddress address = args == null || args.length == 0 ? InetAddress.getLocalHost() : InetAddress.getByName(args[0]);
		System.out.println("Java getHostAddress: " + address.getHostAddress());
        System.out.println("Java getHostName: " + address.getHostName());
        System.out.println("reverseLookup: " + reverseLookup(address.getHostAddress()));
	}

	public static String localDns(){
		try {
			String dns = reverseLookup(InetAddress.getLocalHost().getHostAddress());
			logger.info("dns resolved: {}",dns);
			return dns;
		} catch (UnknownHostException e) {
			logger.warn("could not resolve dns of localhost");
		}
		return "localhost";
	}
	
	/**
	* Performs a reverse DNS lookup of an IP address
	* by querying the system configured DNS server
	* via JNDI.
	*
	* If the provided argument is not an IPV4 address,
	* the method will return null.
	*
	* If the provided argument is a valid IPV4 address,
	* but does not have an associated hostname, the
	* textual representation of the IP address will
	* be returned (as provided in the input).
	*
	* @param ipAddress the ip to perform reverse lookup
	* @return the hostname associated with the ipAddress
	*/
	public static String reverseLookup(String ipAddress)
	{
		if (!IPAddressUtil.isIPv4LiteralAddress(ipAddress)) {
			return null;
		}
		
		String[] ipBytes = ipAddress.split("\\.");
		String reverseDnsDomain = ipBytes[3] + "." + ipBytes[2] + "." + ipBytes[1] + "." + ipBytes[0] + ".in-addr.arpa";
		
		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put("java.naming.factory.initial","com.sun.jndi.dns.DnsContextFactory");
		try
		{
			DirContext ctx = new InitialDirContext(env);
			Attributes attrs = ctx.getAttributes(reverseDnsDomain,new String[] {"PTR"});
			for (NamingEnumeration<? extends Attribute> ae = attrs.getAll(); ae.hasMoreElements();)
			{
				Attribute attr = (Attribute)ae.next();
				String attrId = attr.getID();
				for (Enumeration<?> vals = attr.getAll(); vals.hasMoreElements();)
				{
					String value = vals.nextElement().toString();
					if ("PTR".equals(attrId))
					{
						final int len = value.length();
						if (value.charAt(len - 1) == '.')
						{
							return value.substring(0, len - 1); // Strip out trailing period
						}
					}
				}
			}
			ctx.close();
		} catch(Exception e)
		{
			return ipAddress; //on exception return original IP address
		}
		
		return ipAddress; //if DNS query returns no result, return IP address
	}
	
}

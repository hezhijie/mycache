package net.mycache.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import com.alibaba.fastjson.JSONObject;

public class JRedisHelp {
	
	private static volatile JRedisHelp instance;
	
	private Properties cacheProperties;
	
	public Properties getCacheProperties() {
		return cacheProperties;
	}

	public void setCacheProperties(Properties cacheProperties) {
		this.cacheProperties = cacheProperties;
	}

	public JedisPoolConfig getConfig() {
		return config;
	}

	public void setConfig(JedisPoolConfig config) {
		this.config = config;
	}

	private JedisPoolConfig config;
	
	private static ShardedJRedisPool pool;
	
	private JRedisHelp(){}
	
	private JRedisHelp(Properties cacheProperties, JedisPoolConfig config){
		this.cacheProperties = cacheProperties;
		this.config = config;
//		init();
	}
	
	public final static JRedisHelp getInstance(Properties cacheProperties, JedisPoolConfig config){
		
		if(instance == null){
			synchronized(JRedisHelp.class){
				if(instance == null){
					instance = new JRedisHelp(cacheProperties, config);
					instance.init();
				}
			}
		}
		
		return instance;
	}
	
	public final JRedisHelp getInstance(){
		
		return getInstance(cacheProperties, config);
		
	}
	
	
	
	private void init(){
		
		String hostport1 = this.cacheProperties.getProperty("redis.ip1");
		String hostport2 = this.cacheProperties.getProperty("redis.ip2");
		String hostport3 = this.cacheProperties.getProperty("redis.ip3");
		String hostport4 = this.cacheProperties.getProperty("redis.ip4");
//		String host = "192.168.73.34";
//        int port1 = 6379;
//        int port2 = 6380;
//        int port3 = 6381;
//        int port4 = 6382;
       
        List<JedisShardInfo> infos = new ArrayList<JedisShardInfo>(); 
        
        try{
        	String host1 = hostport1.split(":")[0];
        	String host2 = hostport2.split(":")[0];
        	String host3 = hostport3.split(":")[0];
        	String host4 = hostport4.split(":")[0];
        	int port1 = Integer.valueOf(hostport1.split(":")[1]);
        	int port2 = Integer.valueOf(hostport2.split(":")[1]);
        	int port3 = Integer.valueOf(hostport3.split(":")[1]);
        	int port4 = Integer.valueOf(hostport4.split(":")[1]);
        	
        	infos.add(new JedisShardInfo(host1, port1));
        	infos.add(new JedisShardInfo(host2, port2));
        	infos.add(new JedisShardInfo(host3, port3));
        	infos.add(new JedisShardInfo(host4, port4));
//        	infos.add(new JedisShardInfo(host, port1));
//            infos.add(new JedisShardInfo(host, port2));
//            infos.add(new JedisShardInfo(host, port3));
//            infos.add(new JedisShardInfo(host, port4));    
        	
        }catch(Throwable t){
        	t.printStackTrace();
        }
        
        pool = new ShardedJRedisPool(this.config, infos);
		
		
	}
	
	public void set(String key, String value){
		
		pool.getResource().set(key, value);
		
	}
	
	public void set(String key, String value, int expireTime){
		
		pool.getResource().setex(key, expireTime, value);
		
	}
	
	public Long setNX(String key, String value){
		
		return pool.getResource().setnx(key, value);
		
	}
	
	public String getSet(String key, String value){
		
		return pool.getResource().getSet(key, value);
				
	}
	
	public Object getSets(String key, Object value){
		
		JSONObject jb = new JSONObject();
		jb.put("value", value);
		String str = this.getSet(key, jb.toJSONString());
		JSONObject jb2 = JSONObject.parseObject(str);
		if(jb2 == null){
			return null;
		}
		return jb2.get("value");
		
	}
	
	public <T> T getSets(String key, Object value, Class<T> clazz){
		
		JSONObject jb = new JSONObject();
		jb.put("value", value);
		String str = this.getSet(key, jb.toJSONString());
		JSONObject jb2 = JSONObject.parseObject(str);
		if(jb2 == null){
			return null;
		}else if(jb2.get("value") == null){
			return null;
		}
//		jb = (JSONObject)jb.get("value");
		T t = JSONObject.parseObject(jb2.get("value").toString(), clazz);
		
		return t;
	}
	
	public Long setsNX(String key, Object value){
		JSONObject jb = new JSONObject();
		jb.put("value", value);
		return this.setNX(key, jb.toJSONString());
	}
	
	public void sets(String key, Object value){
		
		JSONObject jb = new JSONObject();
		jb.put("value", value);
		this.set(key, jb.toJSONString());
		
	}
	
	public void sets(String key, Object value, int expireTime){
		
		JSONObject jb = new JSONObject();
		jb.put("value", value);
		this.set(key, jb.toJSONString(), expireTime);
		
	}
	
	
	
	public String get(String key){
		
		return pool.getResource().get(key);
		
	}
	
	public Object gets(String key){
		
		String str = this.get(key);
		JSONObject jb = JSONObject.parseObject(str);
		if(jb == null){
			return null;
		}
		return jb.get("value");
		
	}
	
	public <T> T gets(String key, Class<T> clazz){
		
		String str = this.get(key);
		JSONObject jb = JSONObject.parseObject(str);
		if(jb == null){
			return null;
		}else if(jb.get("value") == null){
			return null;
		}
//		jb = (JSONObject)jb.get("value");
		T t = JSONObject.parseObject(jb.get("value").toString(), clazz);
		
		return t;
	}
	
	public void remove(String key){
		
		pool.getResource().del(key); 
		
	}
	
	public static void main(String args[]){
		
		JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setMaxIdle(5*1000);
        config.setMaxWaitMillis(1*1000);
		
        JRedisHelp jh = JRedisHelp.getInstance(new Properties(), config);
        jh.set("cc", "11");
        String x = jh.get("cc");
        System.out.println(x);
        jh.remove("cc");
        x = jh.get("cc");
        System.out.println(x);
        if(x == null){
        	System.out.println("aaaaaaaa");
        }
        
        jh.set("rr", "55", 10);
        System.out.println(jh.get("rr"));
//        try {
//			Thread.sleep(5000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//        System.out.println(jh.get("rr"));
        JSONObject jsonObject = new JSONObject();     
        jsonObject.put("name", "kevin");     
        jsonObject.put("Max.score", new Integer(100));     
        jsonObject.put("Min.score", new Integer(50));     
        jsonObject.put("nickname", "picglet"); 
        
        List<String> supplierIdList = new ArrayList<String>();
        supplierIdList.add("qqq");
        
        jh.sets("kk", supplierIdList);
        
//        System.out.println(jh.get("kk"));
        
        System.out.println(jh.gets("kk"));
        List<String> xxx = (List<String>)jh.gets("kk");
        System.out.println(xxx);
        
        PassportUserDetail aa = new PassportUserDetail();
        aa.setId("111");
        aa.setRealname("zf");
        jh.sets("ll", aa);
        System.out.println(aa);
        PassportUserDetail zz = jh.gets("ll", PassportUserDetail.class);
//        JSONObject zzz  =  (JSONObject)zz;
        
//        PassportUserDetail bb = JSONObject.parseObject(zzz.toJSONString(), PassportUserDetail.class);
        
        
//        System.out.println(zz);
//        
//        System.out.println(jh.setsNX("rt1", zz));
//        System.out.println(jh.setsNX("rt1", zz));
//        System.out.println(jh.gets("rt1",PassportUserDetail.class));
        
        jh.sets("hgf", zz);
        zz.setId("222");
        
        System.out.println(jh.getSets("hgf", zz, PassportUserDetail.class));
        System.out.println(jh.gets("hgf", PassportUserDetail.class));
        
        
        
        
        
		
	}
	
	public static class PassportUserDetail implements Serializable{
		
		private static final long serialVersionUID = -5981115032916828000L;

		private String id;
		private String username;
		private String realname;
		private String password;
		private String ext;
		private String email;
		private String phone;
		private int userType;
		private int status;
		
		private List<String> roles;
		private String supplierId;
		private String supplierOrgId;
		
		public boolean isElong(){
			return userType == 1;
		}
		
		public boolean isSupplier(){
			return userType == 2;
		}
		
		public boolean isSupplierAdmin(){
			if(roles == null || roles.size() == 0){
				return false;
			}
			return roles.contains("aaa");
		}
		

		/**
		 * 
		 */
		public PassportUserDetail() {

		}

		/**
		 * @return the id
		 */
		public String getId() {
			return id;
		}

		/**
		 * @param id
		 *            the id to set
		 */
		public void setId(String id) {
			this.id = id;
		}

		/**
		 * @return the username
		 */
		public String getUsername() {
			return username;
		}

		/**
		 * @param username
		 *            the username to set
		 */
		public void setUsername(String username) {
			this.username = username;
		}

		/**
		 * @return the realname
		 */
		public String getRealname() {
			return realname;
		}

		/**
		 * @param realname
		 *            the realname to set
		 */
		public void setRealname(String realname) {
			this.realname = realname;
		}

		/**
		 * @return the password
		 */
		public String getPassword() {
			return password;
		}

		/**
		 * @param password
		 *            the password to set
		 */
		public void setPassword(String password) {
			this.password = password;
		}

		/**
		 * @return the ext
		 */
		public String getExt() {
			return ext;
		}

		/**
		 * @param ext
		 *            the ext to set
		 */
		public void setExt(String ext) {
			this.ext = ext;
		}

		/**
		 * @return the email
		 */
		public String getEmail() {
			return email;
		}

		/**
		 * @param email
		 *            the email to set
		 */
		public void setEmail(String email) {
			this.email = email;
		}

		/**
		 * @return the phone
		 */
		public String getPhone() {
			return phone;
		}

		/**
		 * @param phone
		 *            the phone to set
		 */
		public void setPhone(String phone) {
			this.phone = phone;
		}

		/**
		 * @return the userType
		 */
		public int getUserType() {
			return userType;
		}

		/**
		 * @param userType
		 *            the userType to set
		 */
		public void setUserType(int userType) {
			this.userType = userType;
		}

		/**
		 * @return the status
		 */
		public int getStatus() {
			return status;
		}

		/**
		 * @param status the status to set
		 */
		public void setStatus(int status) {
			this.status = status;
		}

		public List<String> getRoles() {
			return roles;
		}

		public void setRoles(List<String> roles) {
			this.roles = roles;
		}
		
		public String getSupplierId() {
			return supplierId;
		}

		public void setSupplierId(String supplierId) {
			this.supplierId = supplierId;
		}

		public String getSupplierOrgId() {
			return supplierOrgId;
		}

		public void setSupplierOrgId(String supplierOrgId) {
			this.supplierOrgId = supplierOrgId;
		}

		@Override
		public String toString() {
			return "PassportUserDetail [id=" + id + ", username=" + username
					+ ", realname=" + realname + ", password=" + password
					+ ", ext=" + ext + ", email=" + email + ", phone=" + phone
					+ ", userType=" + userType + ", status=" + status + ", roles="
					+ roles + ", supplierId=" + supplierId + ", supplierOrgId="
					+ supplierOrgId + "]";
		}
	}

}

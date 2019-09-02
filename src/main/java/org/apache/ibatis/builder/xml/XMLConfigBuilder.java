/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

	// 是否已经解析过了默认为false
	private boolean parsed;
	// parse对象默认是XPathParse
	private final XPathParser parser;
	private String environment;
	private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

	public XMLConfigBuilder(Reader reader) {
		this(reader, null, null);
	}

	public XMLConfigBuilder(Reader reader, String environment) {
		this(reader, environment, null);
	}

	/**
	 * 调用构造方法初始化该对象， 在初始化XPathParse的时候会将mybatis-config.xml解析成为一个document对象
	 * 
	 * @param reader
	 * @param environment
	 * @param props
	 */
	public XMLConfigBuilder(Reader reader, String environment, Properties props) {
		this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
	}

	public XMLConfigBuilder(InputStream inputStream) {
		this(inputStream, null, null);
	}

	public XMLConfigBuilder(InputStream inputStream, String environment) {
		this(inputStream, environment, null);
	}

	public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
		this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
	}

	/**
	 * 给XMLConfigBuilder对象赋值
	 * 
	 * @param parser
	 * @param environment
	 * @param props
	 */
	private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
		// 调用父类的BaseBuilder构造方法初始化Configuration对象，并获取Configuration对象的typeAliasRegistry和typeHandlerRegistry
		super(new Configuration());
		ErrorContext.instance().resource("SQL Mapper Configuration");
		this.configuration.setVariables(props);
		this.parsed = false;
		this.environment = environment;
		this.parser = parser;
	}

	/**
	 * 开始解析XML文件将它构造成一个Configuration对象。
	 * 
	 * @return
	 */
	public Configuration parse() {
		// 1.如果已经解析过则返回异常
		if (parsed) {
			throw new BuilderException("Each XMLConfigBuilder can only be used once.");
		}
		// 2.标记已经解析过
		parsed = true;
		// 3.真正的解析方法，生成一个configuration对象
		parseConfiguration(parser.evalNode("/configuration"));
		return configuration;
	}

	/**
	 * 通过解析XML中的每一个元素构建一个完整的Configuration对象
	 * 
	 * @param root
	 */
	private void parseConfiguration(XNode root) {
		try {
			// issue #117 read properties first

			// 1.解析Config文件中的properties标签<properties>
			propertiesElement(root.evalNode("properties"));

			// 2.解析xml文件中的setting设置
			Properties settings = settingsAsProperties(root.evalNode("settings"));
			loadCustomVfs(settings);
			loadCustomLogImpl(settings);

			// 3.解析xml中的别名配置，并注册到typeAliasRegistry中
			typeAliasesElement(root.evalNode("typeAliases"));

			// 4.解析插件配置
			pluginElement(root.evalNode("plugins"));

			// 5.解析objectFactory配置
			objectFactoryElement(root.evalNode("objectFactory"));
			objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
			reflectorFactoryElement(root.evalNode("reflectorFactory"));

			// 把xml中setting的值设置到configuration对象中
			settingsElement(settings);
			// read it after objectFactory and objectWrapperFactory issue #631

			// 6.解析环境配置，包含了数据源配置和事务的配置
			environmentsElement(root.evalNode("environments"));

			// 7.解析databaseIdProvider配置
			databaseIdProviderElement(root.evalNode("databaseIdProvider"));

			// 8.解析自定义的类型转换器，注册到typeHandlerRegistry
			typeHandlerElement(root.evalNode("typeHandlers"));

			// 9.解析Mapper标签，将映射文件中的每一项元素转换成某个对象或者某个对象的值
			mapperElement(root.evalNode("mappers"));
		} catch (Exception e) {
			throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
		}
	}

	private Properties settingsAsProperties(XNode context) {
		if (context == null) {
			return new Properties();
		}
		Properties props = context.getChildrenAsProperties();
		// Check that all settings are known to the configuration class
		MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
		for (Object key : props.keySet()) {
			if (!metaConfig.hasSetter(String.valueOf(key))) {
				throw new BuilderException(
						"The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
			}
		}
		return props;
	}

	private void loadCustomVfs(Properties props) throws ClassNotFoundException {
		String value = props.getProperty("vfsImpl");
		if (value != null) {
			String[] clazzes = value.split(",");
			for (String clazz : clazzes) {
				if (!clazz.isEmpty()) {
					@SuppressWarnings("unchecked")
					Class<? extends VFS> vfsImpl = (Class<? extends VFS>) Resources.classForName(clazz);
					configuration.setVfsImpl(vfsImpl);
				}
			}
		}
	}

	private void loadCustomLogImpl(Properties props) {
		Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
		configuration.setLogImpl(logImpl);
	}

	private void typeAliasesElement(XNode parent) {
		if (parent != null) {
			for (XNode child : parent.getChildren()) {
				if ("package".equals(child.getName())) {
					String typeAliasPackage = child.getStringAttribute("name");
					configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
				} else {
					String alias = child.getStringAttribute("alias");
					String type = child.getStringAttribute("type");
					try {
						Class<?> clazz = Resources.classForName(type);
						if (alias == null) {
							typeAliasRegistry.registerAlias(clazz);
						} else {
							typeAliasRegistry.registerAlias(alias, clazz);
						}
					} catch (ClassNotFoundException e) {
						throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
					}
				}
			}
		}
	}

	private void pluginElement(XNode parent) throws Exception {
		if (parent != null) {
			for (XNode child : parent.getChildren()) {
				String interceptor = child.getStringAttribute("interceptor");
				Properties properties = child.getChildrenAsProperties();
				Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).newInstance();
				interceptorInstance.setProperties(properties);
				configuration.addInterceptor(interceptorInstance);
			}
		}
	}

	private void objectFactoryElement(XNode context) throws Exception {
		if (context != null) {
			String type = context.getStringAttribute("type");
			Properties properties = context.getChildrenAsProperties();
			ObjectFactory factory = (ObjectFactory) resolveClass(type).newInstance();
			factory.setProperties(properties);
			configuration.setObjectFactory(factory);
		}
	}

	private void objectWrapperFactoryElement(XNode context) throws Exception {
		if (context != null) {
			String type = context.getStringAttribute("type");
			ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).newInstance();
			configuration.setObjectWrapperFactory(factory);
		}
	}

	private void reflectorFactoryElement(XNode context) throws Exception {
		if (context != null) {
			String type = context.getStringAttribute("type");
			ReflectorFactory factory = (ReflectorFactory) resolveClass(type).newInstance();
			configuration.setReflectorFactory(factory);
		}
	}

	private void propertiesElement(XNode context) throws Exception {
		if (context != null) {
			Properties defaults = context.getChildrenAsProperties();
			String resource = context.getStringAttribute("resource");
			String url = context.getStringAttribute("url");
			if (resource != null && url != null) {
				throw new BuilderException(
						"The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
			}
			if (resource != null) {
				defaults.putAll(Resources.getResourceAsProperties(resource));
			} else if (url != null) {
				defaults.putAll(Resources.getUrlAsProperties(url));
			}
			Properties vars = configuration.getVariables();
			if (vars != null) {
				defaults.putAll(vars);
			}
			parser.setVariables(defaults);
			configuration.setVariables(defaults);
		}
	}

	private void settingsElement(Properties props) {
		configuration.setAutoMappingBehavior(
				AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
		configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior
				.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
		configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
		configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
		configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
		configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
		configuration
				.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
		configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
		configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
		configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
		configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
		configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
		configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
		configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
		configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
		configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
		configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
		configuration.setLazyLoadTriggerMethods(
				stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
		configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
		configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
		configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
		configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
		configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
		configuration
				.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
		configuration.setLogPrefix(props.getProperty("logPrefix"));
		configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
	}

	/**
	 * 解析mybatis-config.xml中的<environments default="development">标签
	 * 
	 * @param context <environments>节点的内容
	 * @throws Exception
	 */
	private void environmentsElement(XNode context) throws Exception {
		if (context != null) {
			if (environment == null) {
				/*
				 * 从<environments default="development">中获取default的值，和<environment
				 * id="development">中的ID值匹配， 如果匹配成功则将当前的environment中的值设置到configuration中
				 * 
				 * 
				 * 从测试的例子中可以看出default的值为development
				 */
				environment = context.getStringAttribute("default");
			}
			/*
			 * 循环解析<environments>标签中的<environment>节点
			 */
			for (XNode child : context.getChildren()) {
				/*
				 * 获取environment中的ID值
				 */
				String id = child.getStringAttribute("id");
				/*
				 * isSpecifiedEnvironment(id)方法：
				 * 如果当前environment的ID值等于default的值当前的environment中的值设置到configuration中。
				 * 
				 * 测试例子中可以看出当前节点的ID值为development，与default的值相等，所以把当前节点中的信息设置到configuration中。
				 */
				if (isSpecifiedEnvironment(id)) {
					/*
					 * 获取事务管理器TransactionFactory
					 */
					TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
					/*
					 * 获取数据源工程DataSourceFactory，产生DataSource
					 */
					DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
					DataSource dataSource = dsFactory.getDataSource();

					/*
					 * 把事务管理器和数据源设置到configuration对象中的Environment属性上
					 */
					Environment.Builder environmentBuilder = new Environment.Builder(id).transactionFactory(txFactory)
							.dataSource(dataSource);
					configuration.setEnvironment(environmentBuilder.build());
				}
			}
		}
	}

	private void databaseIdProviderElement(XNode context) throws Exception {
		DatabaseIdProvider databaseIdProvider = null;
		if (context != null) {
			String type = context.getStringAttribute("type");
			// awful patch to keep backward compatibility
			if ("VENDOR".equals(type)) {
				type = "DB_VENDOR";
			}
			Properties properties = context.getChildrenAsProperties();
			databaseIdProvider = (DatabaseIdProvider) resolveClass(type).newInstance();
			databaseIdProvider.setProperties(properties);
		}
		Environment environment = configuration.getEnvironment();
		if (environment != null && databaseIdProvider != null) {
			String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
			configuration.setDatabaseId(databaseId);
		}
	}

	/**
	 * 获取事务工厂
	 * 
	 * @param context <environment>标签中的 <transactionManager>标签信息
	 * @return
	 * @throws Exception
	 */
	private TransactionFactory transactionManagerElement(XNode context) throws Exception {
		if (context != null) {
			// 获取 <transactionManager type="JDBC">中的type属性值，根据type属性的值来指定使用哪个类型的事务管理器
			String type = context.getStringAttribute("type");
			// 获取 <transactionManager>中<property>的值，包含了name和value。
			Properties props = context.getChildrenAsProperties();
			/*
			 * 通过type的值获取到对应的TransactionFactory，并使用反射创建一个新的对象。2个问题
			 * resolveClass(type)如何获取都对应的类型？
			 * resolveClass()是BaseBuilder中的方法。最终通过org.apache.ibatis.type.TypeAliasRegistry.
			 * resolveAlias(String)获取到对应的Class对象 什么时候将别名注册到了TypeAliasRegistry中？
			 */
			TransactionFactory factory = (TransactionFactory) resolveClass(type).newInstance();
			factory.setProperties(props);
			return factory;
		}
		throw new BuilderException("Environment declaration requires a TransactionFactory.");
	}

	/**
	 * 获取数据源配置
	 * 
	 * @param context <environment>标签中的 <dataSource>标签信息
	 * @return
	 * @throws Exception
	 */
	private DataSourceFactory dataSourceElement(XNode context) throws Exception {
		if (context != null) {
			// 获取 <transactionManager type="JDBC">中的type属性值，根据type属性的值来指定使用哪个类型的数据源
			String type = context.getStringAttribute("type");
			// 获取 <transactionManager>中<property>的值，包含了name和value。
			Properties props = context.getChildrenAsProperties();
			/*
			 * 通过type的值获取到对应的DataSourceFactory，并使用反射创建一个新的对象。
			 */
			DataSourceFactory factory = (DataSourceFactory) resolveClass(type).newInstance();
			factory.setProperties(props);
			return factory;
		}
		throw new BuilderException("Environment declaration requires a DataSourceFactory.");
	}

	private void typeHandlerElement(XNode parent) {
		if (parent != null) {
			for (XNode child : parent.getChildren()) {
				if ("package".equals(child.getName())) {
					String typeHandlerPackage = child.getStringAttribute("name");
					typeHandlerRegistry.register(typeHandlerPackage);
				} else {
					String javaTypeName = child.getStringAttribute("javaType");
					String jdbcTypeName = child.getStringAttribute("jdbcType");
					String handlerTypeName = child.getStringAttribute("handler");
					Class<?> javaTypeClass = resolveClass(javaTypeName);
					JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
					Class<?> typeHandlerClass = resolveClass(handlerTypeName);
					if (javaTypeClass != null) {
						if (jdbcType == null) {
							typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
						} else {
							typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
						}
					} else {
						typeHandlerRegistry.register(typeHandlerClass);
					}
				}
			}
		}
	}

	private void mapperElement(XNode parent) throws Exception {
		if (parent != null) {
			for (XNode child : parent.getChildren()) {
				if ("package".equals(child.getName())) {
					String mapperPackage = child.getStringAttribute("name");
					configuration.addMappers(mapperPackage);
				} else {
					String resource = child.getStringAttribute("resource");
					String url = child.getStringAttribute("url");
					String mapperClass = child.getStringAttribute("class");
					if (resource != null && url == null && mapperClass == null) {
						ErrorContext.instance().resource(resource);
						InputStream inputStream = Resources.getResourceAsStream(resource);
						XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource,
								configuration.getSqlFragments());
						mapperParser.parse();
					} else if (resource == null && url != null && mapperClass == null) {
						ErrorContext.instance().resource(url);
						InputStream inputStream = Resources.getUrlAsStream(url);
						XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url,
								configuration.getSqlFragments());
						mapperParser.parse();
					} else if (resource == null && url == null && mapperClass != null) {
						Class<?> mapperInterface = Resources.classForName(mapperClass);
						configuration.addMapper(mapperInterface);
					} else {
						throw new BuilderException(
								"A mapper element may only specify a url, resource or class, but not more than one.");
					}
				}
			}
		}
	}

	private boolean isSpecifiedEnvironment(String id) {
		if (environment == null) {
			throw new BuilderException("No environment specified.");
		} else if (id == null) {
			throw new BuilderException("Environment requires an id attribute.");
		} else if (environment.equals(id)) {
			return true;
		}
		return false;
	}

}

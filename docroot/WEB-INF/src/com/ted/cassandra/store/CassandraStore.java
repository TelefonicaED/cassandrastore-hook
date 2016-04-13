package com.ted.cassandra.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.exception.SystemException;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portlet.documentlibrary.store.BaseStore;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
public class CassandraStore extends BaseStore {
	private static Cluster cluster;
	private  Session session;
	static String node="10.102.225.54";
	static PreparedStatement insertStatement;
	static PreparedStatement getKeyStatement;
	static PreparedStatement deleteKeyStatement;
	   public Session getSession() 
	   {
		   
		   if(session==null)
		   {
			   connect();
		   }
	      return this.session;
	   }
	   public void createSchema() {
		      session.execute("CREATE KEYSPACE IF NOT EXISTS lr_dl WITH replication " + 
		            "= {'class':'SimpleStrategy', 'replication_factor':1};");
		      session.execute(
		            "CREATE TABLE IF NOT EXISTS lr_dl.dlstore (" +
		                  "key varchar," + 
		                  "data blob," + 
		                  "size int," + 
		                  "PRIMARY KEY ( key )" + 
		                  ");");
		      
		   }
	   public void connect() 
	   {
		  
		  if(cluster==null)
		  {
		      cluster = Cluster.builder()
		            .addContactPoint(node).withCredentials("desarrollo", "d3s4rr0ll0")
		            .build();
		      Metadata metadata = cluster.getMetadata();
		      System.out.printf("Connected to cluster: %s\n", 
		            metadata.getClusterName());
		      for ( Host host : metadata.getAllHosts() ) {
		         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
		               host.getDatacenter(), host.getAddress(), host.getRack());
		      }
		      
		      session = cluster.connect();
		      
		      //createSchema();
		     
		  	 insertStatement = session.prepare(
				      "INSERT INTO lr_dl.dlstore " +
				      "(key, data, size ) " +
				      "VALUES (?, ?, ?);");
		  	getKeyStatement = session.prepare(
							      "select * from lr_dl.dlstore where key= ?;");
			 
		  	deleteKeyStatement = session.prepare(
				      "delete from lr_dl.dlstore where key= ?;");
  
		  }
	     

		   
	  }

	protected String getKey(
			long companyId, long repositoryId, String fileName,
			String versionLabel) {

			StringBundler sb = new StringBundler(7);

			sb.append(companyId);
			sb.append(StringPool.SLASH);
			sb.append(repositoryId);
			sb.append(StringPool.SLASH);
			sb.append(fileName);
			sb.append(StringPool.SLASH);
			sb.append(versionLabel);

			return sb.toString();
		}
	protected String getKey(
			long companyId, long repositoryId, String fileName) {

			StringBundler sb = new StringBundler(6);

			sb.append(companyId);
			sb.append(StringPool.SLASH);
			sb.append(repositoryId);
			sb.append(StringPool.SLASH);
			sb.append(fileName);
			sb.append(StringPool.SLASH);

			return sb.toString();
		}
	protected String getKey(long companyId, long repositoryId) {
		StringBundler sb = new StringBundler(4);

		sb.append(companyId);
		sb.append(StringPool.SLASH);
		sb.append(repositoryId);
		sb.append(StringPool.SLASH);

		return sb.toString();
	}
	@Override
	public String[] getFileNames(long companyId, long repositoryId)
			throws SystemException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateFile(long companyId, long repositoryId, String fileName,
			String newFileName) throws PortalException, SystemException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addDirectory(long companyId, long repositoryId, String dirName)
			throws PortalException, SystemException {
		

	}
	private static byte[] readFully(InputStream input) throws IOException
	{
	    byte[] buffer = new byte[8192];
	    int bytesRead;
	    ByteArrayOutputStream output = new ByteArrayOutputStream();
	    while ((bytesRead = input.read(buffer)) != -1)
	    {
	        output.write(buffer, 0, bytesRead);
	    }
	    return output.toByteArray();
	}
	@Override
	public void addFile(long companyId, long repositoryId, String fileName,
			InputStream is) throws PortalException, SystemException 
	{
		String key=getKey(companyId, repositoryId, fileName, VERSION_DEFAULT);
		try 
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			 
			byte[] tmp = new byte[1024];
			int length=0;
			while (true) {
			    int r = is.read(tmp);
			    if (r == -1) break;
			    length+=r;
			    out.write(tmp,0,r);
			}
			byte[] data= out.toByteArray();
			ByteBuffer buf = ByteBuffer.wrap(data);
			BoundStatement bs=new BoundStatement(insertStatement);
			getSession().execute(bs.bind(key,buf,length));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new SystemException(e);
		}

	}

	@Override
	public void checkRoot(long companyId) throws SystemException {

	}

	@Override
	public void deleteDirectory(long companyId, long repositoryId,
			String dirName) throws PortalException, SystemException {
		

	}

	@Override
	public void deleteFile(long companyId, long repositoryId, String fileName)
			throws PortalException, SystemException {
		String key=getKey(companyId, repositoryId, fileName, VERSION_DEFAULT);
		
		BoundStatement bs=new BoundStatement(deleteKeyStatement);
		ResultSet rs=getSession().execute(bs.bind(key));   
	}

	@Override
	public void deleteFile(long companyId, long repositoryId, String fileName,
			String versionLabel) throws PortalException, SystemException {
		String key=getKey(companyId, repositoryId, fileName, versionLabel);
		BoundStatement bs=new BoundStatement(deleteKeyStatement);
		ResultSet rs=getSession().execute(bs.bind(key));

	}
	
	@Override
	public InputStream getFileAsStream(long companyId, long repositoryId,
			String fileName, String versionLabel) throws PortalException,
			SystemException {
		String key=getKey(companyId, repositoryId, fileName, versionLabel);
		BoundStatement bs=new BoundStatement(getKeyStatement);
		ResultSet rs=getSession().execute(bs.bind(key));
		Row row=rs.one();
		if(row!=null)
		{
			ByteBuffer bb=row.getBytes("data");
			byte[] data=new byte[bb.remaining()];
			bb.get(data);
			return new ByteArrayInputStream(data);
		}
		else
		{
			return null;
		}
	}

	@Override
	public String[] getFileNames(long companyId, long repositoryId,
			String dirName) throws PortalException, SystemException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getFileSize(long companyId, long repositoryId, String fileName)
			throws PortalException, SystemException {
		// TODO Auto-generated method stub
		String key=getKey(companyId, repositoryId, fileName);
		BoundStatement bs=new BoundStatement(getKeyStatement);
		ResultSet rs=getSession().execute(bs.bind(key));
		Row row=rs.one();
		if(row!=null)
		{
			return row.getLong("size");
			
		}
		else
		{
			return 0;
		}
	}

	@Override
	public boolean hasDirectory(long companyId, long repositoryId,
			String dirName) throws PortalException, SystemException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean hasFile(long companyId, long repositoryId, String fileName,
			String versionLabel) throws PortalException, SystemException 
	{

		String key=getKey(companyId, repositoryId, fileName, versionLabel);
		BoundStatement bs=new BoundStatement(getKeyStatement);
		ResultSet rs=getSession().execute(bs.bind(key));
		Row row=rs.one();
		if(row!=null)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	public CassandraStore() {
		super();
		getSession();
	}
	@Override
	public void move(String srcDir, String destDir) throws SystemException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFile(long companyId, long repositoryId,
			long newRepositoryId, String fileName) throws PortalException,
			SystemException {
		String key=getKey(companyId, repositoryId, fileName, VERSION_DEFAULT);
		

	}

	@Override
	public void updateFile(long companyId, long repositoryId, String fileName,
			String versionLabel, InputStream is) throws PortalException,
			SystemException {
		
		String key=getKey(companyId, repositoryId, fileName, versionLabel);
		try 
		{
			ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
			 
			byte[] tmp = new byte[1024];
			int length=0;
			while (true) {
			    int r = is.read(tmp);
			    if (r == -1) break;
			    length+=r;
			    out.write(tmp,0,r);
			}
			byte[] data= out.toByteArray();
			ByteBuffer buf = ByteBuffer.wrap(data);
			BoundStatement bs=new BoundStatement(insertStatement);
			getSession().execute(bs.bind(key,buf,length));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new SystemException(e);
		}


	}
	
}


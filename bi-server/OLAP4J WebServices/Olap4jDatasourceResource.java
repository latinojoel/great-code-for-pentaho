package org.pentaho.platform.dataaccess.datasource.wizard.service.impl;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;
import static javax.ws.rs.core.MediaType.WILDCARD;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.platform.api.engine.IAuthorizationPolicy;
import org.pentaho.platform.api.engine.PentahoAccessControlException;
import org.pentaho.platform.api.repository2.unified.IRepositoryFileData;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.plugin.services.importer.PlatformImportException;
import org.pentaho.platform.plugin.services.importexport.RepositoryFileBundle;
import org.pentaho.platform.plugin.services.importexport.StreamConverter;
import org.pentaho.platform.plugin.services.importexport.legacy.MondrianCatalogRepositoryHelper;
import org.pentaho.platform.plugin.services.importexport.legacy.MondrianCatalogRepositoryHelper.Olap4jServerInfo;
import org.pentaho.platform.repository2.ClientRepositoryPaths;
import org.pentaho.platform.repository2.unified.fileio.RepositoryFileInputStream;
import org.pentaho.platform.security.policy.rolebased.actions.AdministerSecurityAction;
import org.pentaho.platform.security.policy.rolebased.actions.RepositoryCreateAction;
import org.pentaho.platform.security.policy.rolebased.actions.RepositoryReadAction;
import org.pentaho.platform.web.http.api.resources.JaxbList;

import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.FormDataParam;

@Path("/data-access/api/olap4j")
public class Olap4jDatasourceResource {
  
  private static final int SUCCESS = 3;
  
  private static final Log logger = LogFactory.getLog(Olap4jDatasourceResource.class);
  public static final String APPLICATION_ZIP = "application/zip"; //$NON-NLS-1$

  @XmlRootElement( name = "Olap4jInfo" )
  public static class Olap4jInfo implements Serializable {
    private String className;
    private String name;
    private String URL;
    private Properties properties; // I don't think this is serializable
    private String user;

    public Olap4jInfo() {}
    
    Olap4jInfo(Olap4jServerInfo info) {
      this.className = info.className;
      this.name = info.name;
      this.properties = info.properties;
      this.URL = info.URL;
      this.user = info.user;
    }

    @XmlElement
    public String getClassName() {
      return className;
    }
    
    @XmlElement
    public String getName() {
      return name;
    }
    
    @XmlElement
    public String getURL() {
      return URL;
    }
    
    @XmlElement
    public String getUser() {
      return user;
    }
  }

  /**
   * Get list of IDs of olap4j datasources
   *
   * @return JaxbList<String> of olap4j IDs
   */
  @GET
  @Path("/ids")
  @Produces( { APPLICATION_XML, APPLICATION_JSON })
  public JaxbList<String> getOlap4JDatasourceIds() {
    List<String> olap4jIds = new ArrayList<String>();
    final MondrianCatalogRepositoryHelper helper = new MondrianCatalogRepositoryHelper( PentahoSystem.get(IUnifiedRepository.class) );
    olap4jIds.addAll(helper.getOlap4jServers());
    return new JaxbList<String>(olap4jIds);
  }

  /**
   * Remove the olap4j datasource for a given olap4j ID
   *
   * @param olap4jId String ID of the olap4j datasource to remove
   *
   * @return Response ok if successful
   */
  @POST
  @Path("/{olap4jId : .+}/remove")
  @Produces(WILDCARD)
  public Response doRemoveOlap4jDatasource(@PathParam("olap4jId") String olap4jId) {
    if(!canAdminister()) {
      return Response.status(UNAUTHORIZED).build();
    }
    final MondrianCatalogRepositoryHelper helper = new MondrianCatalogRepositoryHelper( PentahoSystem.get(IUnifiedRepository.class) );
    helper.deleteOlap4jServer( olap4jId );
    return Response.ok().build();
  }
  
  /**
   * Get the data source wizard info (parameters) for a specific data source wizard id
   *
   * @param dswId String id for a data source wizard
   *
   * @return Response containing the parameter list
   */
  @GET
  @Path("/{olap4jId : .+}/getOlap4jServerInfo")
  @Produces( { APPLICATION_XML, APPLICATION_JSON })
  public Olap4jInfo getOlap4jServerInfo(@PathParam("olap4jId") String olap4jId) {
    final MondrianCatalogRepositoryHelper helper = new MondrianCatalogRepositoryHelper( PentahoSystem.get(IUnifiedRepository.class) );
    Olap4jServerInfo info = helper.getOlap4jServerInfo(olap4jId);
    if (info == null) {
      throw new IllegalArgumentException("Error: cannot find datasource '" + olap4jId + "'");
    }
    Olap4jInfo infopojo = new Olap4jInfo(info);
    return infopojo;
  }
  
  private boolean canAdminister() {
    IAuthorizationPolicy policy = PentahoSystem
        .get(IAuthorizationPolicy.class);
    return policy
        .isAllowed(RepositoryReadAction.NAME) && policy.isAllowed(RepositoryCreateAction.NAME)
        && (policy.isAllowed(AdministerSecurityAction.NAME));
  }
  
  /**
   * This is used by PUC to use a Jersey put to import a Mondrian Schema XML into PUR
   * @author: tband
   * date: 7/10/12
   * @param dataInputStream
   * @param schemaFileInfo
   * @param catalogName
   * @param datasourceName
   * @param overwrite
   * @param xmlaEnabledFlag
   * @param parameters
   * @return this method returns a response of "3" for success, 8 if exists, etc.
   * @throws PentahoAccessControlException
   */
  @POST
  @Path("/postSchema")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces("text/plain")
  public Response postOlap4jSchema(
      @FormDataParam("upload") InputStream dataInputStream,
      @FormDataParam("upload") FormDataContentDisposition schemaFileInfo,
      @FormDataParam("name") String datasourceName,
      @FormDataParam("className") String className,
      @FormDataParam("url") String url,
      @FormDataParam("user") String user,
      @FormDataParam("password") String password,
      @FormDataParam("overwrite") String overwrite,
      @FormDataParam("parameters") String parameters) throws PentahoAccessControlException {
    Response response = null;
    int statusCode = PlatformImportException.PUBLISH_GENERAL_ERROR;
    try {
      if(!canAdminister()) {
        return Response.status(UNAUTHORIZED).build();
      }
      
      if (datasourceName == null) {
        logger.error("Error postOlap4jSchema, missing 'name' parameter");
        return Response.status(2).build();
      }
      
      if (url == null) {
        logger.error("Error postOlap4jSchema, missing 'url' parameter");
        return Response.status(2).build();
      }
      
      if (className == null) {
        logger.error("Error postOlap4jSchema, missing 'className' parameter");
        return Response.status(2).build();
      }
      
      if (dataInputStream == null) {
        logger.error("Error postOlap4jSchema, missing 'upload' parameter");
        return Response.status(2).build();
      }

      // TODO: implement overwrite flag
      
      // Store metadata
 
      // for now, it's the responsibility of the client to specify the "Catalog=solution://etc/olap-servers/<datasourceName>/schema.xml
      
      // TODO: Parse Parameters - not used yet
      Properties params = new Properties();
      
      final MondrianCatalogRepositoryHelper helper = new MondrianCatalogRepositoryHelper( PentahoSystem.get(IUnifiedRepository.class) );
      helper.addOlap4jServer( datasourceName, className, url, user, password, params );
      addHostedSchema( dataInputStream, datasourceName );
      // processMondrianImport(dataInputStream, catalogName, origCatalogName, overwrite, xmlaEnabledFlag, parameters, fileName);
      statusCode = SUCCESS;
      /*
    } catch (PentahoAccessControlException pac) {
      logger.error(pac.getMessage());
      statusCode = PlatformImportException.PUBLISH_USERNAME_PASSWORD_FAIL;
    } catch (PlatformImportException pe) {
      statusCode = pe.getErrorStatus();
      logger.error("Error putMondrianSchema " + pe.getMessage() + " status = " + statusCode);
      */
    } catch (Exception e) {
      logger.error("Error putMondrianSchema " + e.getMessage());
      statusCode = PlatformImportException.PUBLISH_GENERAL_ERROR;
    }

    response = Response.ok().entity(Integer.toString( statusCode)).type(MediaType.TEXT_PLAIN).build();
    logger.debug("putMondrianSchema Response " + response);
    return response;
  }


  /**
   * Download the analysis files for a given analysis id
   *
   * @param analysisId String Id of the analysis data to retrieve
   *
   * @return Response containing the file data
   */
  @GET
  @Path("/{olap4jId : .+}/download")
  @Produces(WILDCARD)
  public Response doGetOlap4jFilesAsDownload(@PathParam("olap4jId") String olap4jId) {
    if(!canAdminister()) {
      return Response.status(UNAUTHORIZED).build();
    }
    Map<String, InputStream> fileData = getOlap4jSchemaFiles(olap4jId);
    parseMondrianSchemaName(olap4jId, fileData);
    return createAttachment(fileData, olap4jId);
  }
  
  private void parseMondrianSchemaName( String olap4jId, Map<String, InputStream> fileData ) {
    final String keySchema = "schema.xml";//$NON-NLS-1$
    if ( fileData.containsKey( keySchema ) ) {
      fileData.put( olap4jId + ".mondrian.xml", fileData.get( keySchema ) );//$NON-NLS-1$
      fileData.remove( keySchema );
    }
  }
  
  private Response createAttachment(Map<String, InputStream> fileData, String domainId) {
    String quotedFileName = null;
    final InputStream is;
    if (fileData.size() > 1) { // we've got more than one file so we want to zip them up and send them
      File zipFile = null;
      try {
        zipFile = File.createTempFile("datasourceExport", ".zip"); //$NON-NLS-1$ //$NON-NLS-2$
        zipFile.deleteOnExit();
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile));
        for (String fileName : fileData.keySet()) {
          InputStream zipEntryIs = null;
          try {
            ZipEntry entry = new ZipEntry(fileName);
            zos.putNextEntry(entry);
            zipEntryIs = fileData.get(fileName);
            IOUtils.copy(zipEntryIs, zos);
          } catch (Exception e) {
            continue;
          } finally {
            zos.closeEntry();
            if (zipEntryIs != null) {
              zipEntryIs.close();
            }
          }
        }
        zos.close();
        is = new FileInputStream(zipFile);
      } catch (IOException ioe) {
        return Response.serverError().entity(ioe.toString()).build();
      }
      StreamingOutput streamingOutput = new StreamingOutput() {
        public void write(OutputStream output) throws IOException {
          IOUtils.copy(is, output);
        }
      };
      final int xmiIndex = domainId.lastIndexOf( ".xmi" );//$NON-NLS-1$
      quotedFileName = "\"" + ( xmiIndex > 0 ? domainId.substring( 0, xmiIndex ) : domainId ) + ".zip\""; //$NON-NLS-1$//$NON-NLS-2$
      return Response.ok(streamingOutput, APPLICATION_ZIP).header("Content-Disposition", "attachment; filename=" + quotedFileName).build(); //$NON-NLS-1$ //$NON-NLS-2$
    } else if (fileData.size() == 1) {  // we've got a single metadata file so we just return that.
      String fileName = (String) fileData.keySet().toArray()[0];
      quotedFileName = "\"" + fileName + "\""; //$NON-NLS-1$ //$NON-NLS-2$
      is = fileData.get(fileName);
      String mimeType = MediaType.TEXT_PLAIN;
      if (is instanceof RepositoryFileInputStream) {
        mimeType = ((RepositoryFileInputStream)is).getMimeType();
      }
      StreamingOutput streamingOutput = new StreamingOutput() {
        public void write(OutputStream output) throws IOException {
          IOUtils.copy(is, output);
        }
      };
      return Response.ok(streamingOutput, mimeType).header("Content-Disposition", "attachment; filename=" + quotedFileName).build(); //$NON-NLS-1$ //$NON-NLS-2$
    }
    return Response.serverError().build();
  }

  private Map<String, InputStream> getOlap4jSchemaFiles( String catalogName ) {
    Map<String, InputStream> values = new HashMap<String, InputStream>();
    IUnifiedRepository repository = PentahoSystem.get(IUnifiedRepository.class);
    RepositoryFile catalogFolder =
      repository.getFile( MondrianCatalogRepositoryHelper.ETC_OLAP_SERVERS_JCR_FOLDER + RepositoryFile.SEPARATOR + catalogName );

    for ( RepositoryFile repoFile : repository.getChildren( catalogFolder.getId() ) ) {
      RepositoryFileInputStream is;
      try {
        if ( repoFile.getName().equals( "metadata" ) ) {
          continue;
        }
        is = new RepositoryFileInputStream( repoFile );
      } catch ( Exception e ) {
        return null; // This pretty much ensures an exception will be thrown later and passed to the client
      }
      values.put( repoFile.getName(), is );
    }
    return values;
  }

  private void addHostedSchema( InputStream schemaFile, String datasourceName ) throws Exception {
    
    IUnifiedRepository repository = PentahoSystem.get(IUnifiedRepository.class);
    // this assumes the schema folder is already there.

    final RepositoryFile etcOlapServers =
        repository.getFile( MondrianCatalogRepositoryHelper.ETC_OLAP_SERVERS_JCR_FOLDER );

    RepositoryFile entry =
        repository.getFile(
          MondrianCatalogRepositoryHelper.ETC_OLAP_SERVERS_JCR_FOLDER
          + RepositoryFile.SEPARATOR
          + datasourceName );

      if ( entry == null ) {
        entry =
          repository.createFolder(
            etcOlapServers.getId(),
            new RepositoryFile.Builder( datasourceName )
              .folder( true )
              .build(),
            "Creating entry for olap server: "
              + datasourceName
              + " into folder "
              + MondrianCatalogRepositoryHelper.ETC_OLAP_SERVERS_JCR_FOLDER );
      }
      
    File tempFile = File.createTempFile( "tempFile", null );
    tempFile.deleteOnExit();
    FileOutputStream outputStream = new FileOutputStream( tempFile );
    IOUtils.copy( schemaFile, outputStream );

    RepositoryFile repoFile = new RepositoryFile.Builder( "schema.xml" ).build();
    RepositoryFileBundle repoFileBundle =
        new RepositoryFileBundle( repoFile, null,
            MondrianCatalogRepositoryHelper.ETC_OLAP_SERVERS_JCR_FOLDER + RepositoryFile.SEPARATOR + datasourceName + RepositoryFile.SEPARATOR, tempFile,
            "UTF-8", "text/xml" );

    RepositoryFile schema =
        repository.getFile( MondrianCatalogRepositoryHelper.ETC_OLAP_SERVERS_JCR_FOLDER + RepositoryFile.SEPARATOR + datasourceName + RepositoryFile.SEPARATOR
            + "schema.xml" );
    IRepositoryFileData data =
        new StreamConverter().convert(
          repoFileBundle.getInputStream(), repoFileBundle.getCharset(), repoFileBundle.getMimeType() );
    if ( schema == null ) {
      repository.createFile( entry.getId(), repoFileBundle.getFile(), data, null );
    } else {
      repository.updateFile( schema, data, null );
    }
  }

}

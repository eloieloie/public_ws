import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

public class chk_jdbc {
    
    // BigQuery connection parameters - modify these according to your setup
    private static final String DB_URL = "jdbc:bigquery://https://private.googleapis.com/bigquery/v2:443;ProjectId=tnn-sb-to970548-1;DefaultDataset=your_dataset";
    private static final String CREDENTIAL_FILE_PATH = "/path/to/wif-credentials.json";
    private static final String SERVICE_ACCOUNT_TOKEN_FILE = "/var/run/service-account/token";
    private static final String BIGQUERY_DRIVER_PATH = "/opt/denodo/lib/extensions/jdbc-drivers-external/bigquery";
    
    public static void main(String[] args) {
        testBigQueryJdbcConnection();
    }
    
    /**
     * Load BigQuery JDBC drivers from the specified directory
     */
    private static void loadBigQueryDrivers() {
        try {
            File driverDir = new File(BIGQUERY_DRIVER_PATH);
            if (!driverDir.exists() || !driverDir.isDirectory()) {
                System.out.println("Warning: BigQuery driver directory not found: " + BIGQUERY_DRIVER_PATH);
                return;
            }
            
            File[] jarFiles = driverDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".jar"));
            if (jarFiles == null || jarFiles.length == 0) {
                System.out.println("Warning: No JAR files found in BigQuery driver directory");
                return;
            }
            
            URL[] urls = new URL[jarFiles.length];
            for (int i = 0; i < jarFiles.length; i++) {
                urls[i] = jarFiles[i].toURI().toURL();
                System.out.println("Loading driver: " + jarFiles[i].getName());
            }
            
            URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
            Thread.currentThread().setContextClassLoader(classLoader);
            
            System.out.println("✓ BigQuery drivers loaded from: " + BIGQUERY_DRIVER_PATH);
            
        } catch (Exception e) {
            System.out.println("✗ Error loading BigQuery drivers: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Tests JDBC connection to BigQuery using WIF authentication
     */
    public static void testBigQueryJdbcConnection() {
        Connection connection = null;
        
        try {
            System.out.println("Testing BigQuery JDBC connection with WIF authentication...");
            System.out.println("Database URL: " + DB_URL);
            System.out.println("Service Account Token File: " + SERVICE_ACCOUNT_TOKEN_FILE);
            System.out.println("-".repeat(50));
            
            // Load BigQuery drivers first
            loadBigQueryDrivers();
            
            // Set up connection properties for WIF authentication
            Properties props = new Properties();
            props.setProperty("AuthenticationType", "4"); // External Account
            props.setProperty("CredentialsPath", CREDENTIAL_FILE_PATH);
            
            // Load Simba BigQuery driver
            Class.forName("com.simba.googlebigquery.jdbc.Driver");
            
            // Attempt to establish connection
            connection = DriverManager.getConnection(DB_URL, props);
            
            if (connection != null) {
                System.out.println("✓ BigQuery connection successful!");
                
                // Get database metadata
                DatabaseMetaData metaData = connection.getMetaData();
                System.out.println("Database Product: " + metaData.getDatabaseProductName());
                System.out.println("Database Version: " + metaData.getDatabaseProductVersion());
                System.out.println("Driver Name: " + metaData.getDriverName());
                System.out.println("Driver Version: " + metaData.getDriverVersion());
                System.out.println("JDBC Version: " + metaData.getJDBCMajorVersion() + "." + metaData.getJDBCMinorVersion());
                
                // Test if connection is still valid
                if (connection.isValid(10)) {
                    System.out.println("✓ Connection is valid and responsive");
                } else {
                    System.out.println("✗ Connection is not responding");
                }
                
            } else {
                System.out.println("✗ Failed to establish BigQuery connection");
            }
            
        } catch (ClassNotFoundException e) {
            System.out.println("✗ BigQuery JDBC Driver not found!");
            System.out.println("Error: " + e.getMessage());
            System.out.println("Ensure Simba BigQuery JDBC driver JAR is in classpath");
            
        } catch (SQLException e) {
            System.out.println("✗ BigQuery JDBC Connection failed!");
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("SQL State: " + e.getSQLState());
            System.out.println("Error Message: " + e.getMessage());
            
            // BigQuery specific error suggestions
            suggestBigQuerySolutions(e);
            
        } catch (Exception e) {
            System.out.println("✗ Unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
            
        } finally {
            // Close connection
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println("✓ Connection closed successfully");
                } catch (SQLException e) {
                    System.out.println("✗ Error closing connection: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Masks password for secure display
     */
    private static String maskPassword(String password) {
        if (password == null || password.isEmpty()) {
            return "****";
        }
        return "*".repeat(password.length());
    }
    
    /**
     * Provides suggestions based on common JDBC errors
     */
    private static void suggestSolutions(SQLException e) {
        System.out.println("\n--- Troubleshooting Suggestions ---");
        
        String errorMessage = e.getMessage().toLowerCase();
        
        if (errorMessage.contains("no suitable driver")) {
            System.out.println("• Ensure JDBC driver JAR is in classpath");
            System.out.println("• For MySQL: mysql-connector-java-x.x.x.jar");
            System.out.println("• For PostgreSQL: postgresql-x.x.x.jar");
        }
        
        if (errorMessage.contains("connection refused") || errorMessage.contains("connection timed out")) {
            System.out.println("• Check if database server is running");
            System.out.println("• Verify host and port in connection URL");
            System.out.println("• Check firewall settings");
        }
        
        if (errorMessage.contains("access denied") || errorMessage.contains("authentication failed")) {
            System.out.println("• Verify username and password");
            System.out.println("• Check user privileges on the database");
        }
        
        if (errorMessage.contains("unknown database") || errorMessage.contains("database does not exist")) {
            System.out.println("• Verify database name in connection URL");
            System.out.println("• Ensure database exists on the server");
        }
        
        System.out.println("• Check network connectivity to database server");
        System.out.println("• Verify SSL/TLS configuration if required");
    }
    
    /**
     * Provides BigQuery-specific troubleshooting suggestions
     */
    private static void suggestBigQuerySolutions(SQLException e) {
        System.out.println("\n--- BigQuery Troubleshooting Suggestions ---");
        
        String errorMessage = e.getMessage().toLowerCase();
        
        if (errorMessage.contains("error requesting access token") || errorMessage.contains("httptransport io error")) {
            System.out.println("• Check WIF credential file exists at: " + CREDENTIAL_FILE_PATH);
            System.out.println("• Verify service account token file exists at: " + SERVICE_ACCOUNT_TOKEN_FILE);
            System.out.println("• Ensure the workload identity pool and provider are correctly configured");
            System.out.println("• Verify the service account has BigQuery permissions");
            System.out.println("• Check if the Kubernetes service account is properly annotated");
            System.out.println("• Validate the audience field in WIF credentials matches the workload identity provider");
        }
        
        if (errorMessage.contains("no suitable driver")) {
            System.out.println("• Ensure Simba BigQuery JDBC driver JAR is in classpath");
            System.out.println("• Download from: https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers");
        }
        
        if (errorMessage.contains("project") || errorMessage.contains("projectid")) {
            System.out.println("• Verify ProjectId in connection URL: tnn-sb-to970548-1");
            System.out.println("• Ensure the service account has access to this project");
        }
        
        if (errorMessage.contains("dataset")) {
            System.out.println("• Check if DefaultDataset exists in BigQuery");
            System.out.println("• Verify dataset permissions for the service account");
        }
        
        System.out.println("• Check Google Cloud IAM permissions for the service account");
        System.out.println("• Verify network connectivity to BigQuery API endpoints");
        System.out.println("• Ensure the cluster has proper Google Cloud API access");
        
        // Additional WIF-specific suggestions
        System.out.println("\n--- WIF Authentication Specific ---");
        System.out.println("• Verify the external_account credential file format is correct");
        System.out.println("• Check the subject_token_type is: urn:ietf:params:oauth:token-type:jwt");
        System.out.println("• Ensure token_url points to: https://sts.googleapis.com/v1/token");
        System.out.println("• Validate service_account_impersonation_url is correctly formatted");
        System.out.println("• Check the workload identity binding between KSA and GSA");
    }
    
    /**
     * Alternative method to test with custom parameters
     */
    public static boolean testConnection(String url, String username, String password) {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            return connection != null && connection.isValid(5);
        } catch (SQLException e) {
            System.out.println("Connection test failed: " + e.getMessage());
            return false;
        }
    }
}
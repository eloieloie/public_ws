import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * CRITICAL: Simba BigQuery JDBC Driver ALWAYS requires BOTH:
 * - AuthenticationType (0=ADC, 1=Service Account, 2=UserAccount, 4=External Account)
 * - OAuthType (1=Bearer Token, 2=WIF/Workload Identity Federation)
 * 
 * For WIF authentication, use:
 * - AuthenticationType=2 (UserAccount) + OAuthType=2 (WIF)
 * - OR provide STS-exchanged access tokens directly
 */
public class chk_jdbc_fixed {
    
    // BigQuery connection parameters - modify these according to your setup
    private static final String DB_URL = "jdbc:bigquery://https://private.googleapis.com/bigquery/v2:443;ProjectId=tnn-sb-to970548-1;DefaultDataset=bq_test_ds1";
    private static final String CREDENTIAL_FILE_PATH = "/opt/denodo/work/eloi_work/wif-credentials.json";
    private static final String SERVICE_ACCOUNT_TOKEN_FILE = "/var/run/service-account/token";
    private static final String BIGQUERY_DRIVER_PATH = "/opt/denodo/lib/extensions/jdbc-drivers-external/bigquery";
    
    public static void main(String[] args) {
        try {
            // Set the environment variable at OS level using ProcessBuilder
            ProcessBuilder pb = new ProcessBuilder();
            pb.environment().put("GOOGLE_APPLICATION_CREDENTIALS", "/opt/denodo/work/eloi_work/wif-credentials.json");
            
            // Also set as system property
            System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "/opt/denodo/work/eloi_work/wif-credentials.json");
            
            System.out.println("Environment variables set:");
            System.out.println("  GOOGLE_APPLICATION_CREDENTIALS (system property): " + System.getProperty("GOOGLE_APPLICATION_CREDENTIALS"));
            System.out.println("  GOOGLE_APPLICATION_CREDENTIALS (env var): " + pb.environment().get("GOOGLE_APPLICATION_CREDENTIALS"));
            
        } catch (Exception e) {
            System.out.println("Warning: Could not set environment variable: " + e.getMessage());
        }
        
        testBigQueryJdbcConnection();
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
            
            // Load BigQuery drivers first and get the class loader
            URLClassLoader driverClassLoader = loadBigQueryDrivers();
            if (driverClassLoader == null) {
                throw new Exception("Failed to load BigQuery drivers");
            }
            
            // Validate credential files before attempting connection
            validateCredentialFiles();
            
            // Test Google credentials loading (if available)
            testGoogleCredentials();
            
            // Try reading the service account token directly
            String serviceAccountToken = readServiceAccountToken();
            
            // Set GOOGLE_APPLICATION_CREDENTIALS environment variable for ADC
            System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", CREDENTIAL_FILE_PATH);
            
            // Try multiple authentication approaches
            // Approach 1: Direct token usage (if we have a token)
            if (serviceAccountToken != null && !serviceAccountToken.trim().isEmpty()) {
                System.out.println("\n--- Approach 1: Using Kubernetes Service Account Token Directly ---");
                Connection tokenConnection = tryDirectTokenAuth(serviceAccountToken, driverClassLoader);
                if (tokenConnection != null) {
                    System.out.println("✓ Connection successful with direct token!");
                    testConnectionSuccess(tokenConnection);
                    return;
                }
            }
            
            // Approach 1b: URL token approach
            if (serviceAccountToken != null && !serviceAccountToken.trim().isEmpty()) {
                System.out.println("\n--- Approach 1b: Using Access Token in JDBC URL ---");
                Connection urlTokenConnection = tryUrlTokenAuth(serviceAccountToken, driverClassLoader);
                if (urlTokenConnection != null) {
                    System.out.println("✓ Connection successful with URL token!");
                    testConnectionSuccess(urlTokenConnection);
                    return;
                }
            }
            
            // Approach 2: UserAccount + OAuthType=2 + STS Token
            System.out.println("\n--- Approach 2: UserAccount + OAuthType=2 + STS Token ---");
            if (serviceAccountToken != null && !serviceAccountToken.trim().isEmpty()) {
                // Get Google access token via STS
                String googleAccessToken = exchangeTokenWithSTS(serviceAccountToken);
                if (googleAccessToken != null) {
                    Properties props = new Properties();
                    
                    // Use UserAccount authentication with WIF OAuth type and STS token
                    props.setProperty("AuthenticationType", "2"); // UserAccount (REQUIRED for WIF!)
                    props.setProperty("OAuthType", "2"); // WIF/Workload Identity Federation (REQUIRED)
                    props.setProperty("OAuthAccessToken", googleAccessToken); // STS exchanged token
                    props.setProperty("OAuthRefreshToken", googleAccessToken); // Same token as refresh (may help)
                    props.setProperty("LogLevel", "6"); // Enable detailed logging
                    props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log"); // Log file location
                    
                    System.out.println("Connection properties (UserAccount=2 + OAuthType=2 + STS token):");
                    System.out.println("  AuthenticationType: " + props.getProperty("AuthenticationType") + " (UserAccount - REQUIRED for WIF!)");
                    System.out.println("  OAuthType: " + props.getProperty("OAuthType") + " (WIF/Workload Identity Federation - REQUIRED)");
                    System.out.println("  OAuthAccessToken: [STS_EXCHANGED_TOKEN_PROVIDED]");
                    System.out.println("  OAuthRefreshToken: [STS_EXCHANGED_TOKEN_PROVIDED]");
                    System.out.println("  Note: Using STS-exchanged token with UserAccount + WIF combination");
                    
                    // Try to load BigQuery driver
                    try {
                        System.out.println("Loading BigQuery JDBC driver...");
                        Class<?> driverClassObj = Class.forName("com.simba.googlebigquery.jdbc.Driver", true, driverClassLoader);
                        
                        // Instantiate the driver to register it with DriverManager
                        java.sql.Driver driver = (java.sql.Driver) driverClassObj.getDeclaredConstructor().newInstance();
                        DriverManager.registerDriver(new DriverShim(driver));
                        
                        System.out.println("✓ Successfully loaded and registered BigQuery driver");
                        
                        // Create the connection using our custom class loader
                        Thread.currentThread().setContextClassLoader(driverClassLoader);
                        connection = DriverManager.getConnection(DB_URL, props);
                        
                        if (connection != null) {
                            System.out.println("✓ BigQuery connection successful with UserAccount + WIF + STS token!");
                            testConnectionSuccess(connection);
                            return;
                        }
                        
                    } catch (Exception e) {
                        System.out.println("✗ UserAccount + WIF approach failed: " + e.getMessage());
                    }
                } else {
                    System.out.println("✗ Could not get STS token for UserAccount approach");
                }
            }
            
            // If we get here, all approaches failed
            System.out.println("\n✗ All authentication approaches failed!");
            
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
        
        System.out.println("\n--- BigQuery Troubleshooting Suggestions ---");
        System.out.println("• Check Google Cloud IAM permissions for the service account");
        System.out.println("• Verify network connectivity to BigQuery API endpoints");
        System.out.println("• Ensure the cluster has proper Google Cloud API access");
        
        System.out.println("\n--- WIF Authentication Specific ---");
        System.out.println("• Verify the external_account credential file format is correct");
        System.out.println("• Check the subject_token_type is: urn:ietf:params:oauth:token-type:jwt");
        System.out.println("• Ensure token_url points to: https://sts.googleapis.com/v1/token");
        System.out.println("• Validate service_account_impersonation_url is correctly formatted");
        System.out.println("• Check the workload identity binding between KSA and GSA");
    }
    
    // Add all the helper methods from the original file here...
    // (I'll continue with the key methods needed)
    
    /**
     * Load BigQuery JDBC drivers from the specified path
     */
    private static URLClassLoader loadBigQueryDrivers() {
        try {
            File driverDir = new File(BIGQUERY_DRIVER_PATH);
            if (!driverDir.exists() || !driverDir.isDirectory()) {
                System.out.println("✗ BigQuery driver directory not found: " + BIGQUERY_DRIVER_PATH);
                return null;
            }
            
            File[] jarFiles = driverDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".jar"));
            if (jarFiles == null || jarFiles.length == 0) {
                System.out.println("✗ No JAR files found in BigQuery driver directory");
                return null;
            }
            
            URL[] jarUrls = new URL[jarFiles.length];
            for (int i = 0; i < jarFiles.length; i++) {
                jarUrls[i] = jarFiles[i].toURI().toURL();
            }
            
            URLClassLoader classLoader = new URLClassLoader(jarUrls, ClassLoader.getSystemClassLoader());
            System.out.println("✓ BigQuery drivers loaded from: " + BIGQUERY_DRIVER_PATH);
            return classLoader;
            
        } catch (Exception e) {
            System.out.println("✗ Failed to load BigQuery drivers: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Test Google credentials loading (if available)
     */
    private static void testGoogleCredentials() {
        try {
            System.out.println("Testing Google Credentials directly...");
            
            // Try to use reflection to load Google credentials without requiring the dependency at compile time
            Class<?> googleCredentialsClass = Class.forName("com.google.auth.oauth2.GoogleCredentials");
            java.lang.reflect.Method fromStreamMethod = googleCredentialsClass.getMethod("fromStream", java.io.InputStream.class);
            java.lang.reflect.Method createScopedMethod = googleCredentialsClass.getMethod("createScoped", java.util.Collection.class);
            java.lang.reflect.Method refreshMethod = googleCredentialsClass.getMethod("refreshAccessToken");
            
            Object credentials = fromStreamMethod.invoke(null, new FileInputStream(CREDENTIAL_FILE_PATH));
            credentials = createScopedMethod.invoke(credentials, java.util.Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
            
            System.out.println("✓ Google credentials loaded successfully");
            System.out.println("  Credential type: " + credentials.getClass().getSimpleName());
            
            // Try to get an access token
            Object accessToken = refreshMethod.invoke(credentials);
            System.out.println("✓ Access token obtained successfully");
            
        } catch (ClassNotFoundException e) {
            System.out.println("× Google Auth library not found in classpath");
            System.out.println("  This is expected - continuing with JDBC driver's internal auth");
        } catch (Exception e) {
            System.out.println("× Failed to load Google credentials: " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println("  Root cause: " + e.getCause().getMessage());
            }
        }
    }
    
    /**
     * Validate that required credential files exist and are readable
     */
    private static void validateCredentialFiles() throws Exception {
        // Check WIF credential file
        File credFile = new File(CREDENTIAL_FILE_PATH);
        if (!credFile.exists()) {
            throw new Exception("WIF credential file not found: " + CREDENTIAL_FILE_PATH);
        }
        if (!credFile.canRead()) {
            throw new Exception("WIF credential file not readable: " + CREDENTIAL_FILE_PATH);
        }
        System.out.println("✓ WIF credential file found: " + CREDENTIAL_FILE_PATH);
        
        // Check service account token file
        File tokenFile = new File(SERVICE_ACCOUNT_TOKEN_FILE);
        if (!tokenFile.exists()) {
            System.out.println("⚠ Warning: Service account token file not found: " + SERVICE_ACCOUNT_TOKEN_FILE);
            System.out.println("  This may be normal if running outside Kubernetes");
        } else {
            System.out.println("✓ Service account token file found: " + SERVICE_ACCOUNT_TOKEN_FILE);
        }
    }
    
    /**
     * Test a successful connection and display information
     */
    private static void testConnectionSuccess(Connection connection) {
        try {
            if (connection != null && connection.isValid(5)) {
                System.out.println("✓ BigQuery connection successful!");
                DatabaseMetaData metaData = connection.getMetaData();
                System.out.println("Driver Name: " + metaData.getDriverName());
                System.out.println("Driver Version: " + metaData.getDriverVersion());
                System.out.println("Database Product: " + metaData.getDatabaseProductName());
            }
        } catch (SQLException e) {
            System.out.println("✗ Error testing connection: " + e.getMessage());
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                    System.out.println("✓ Connection closed successfully");
                }
            } catch (SQLException e) {
                System.out.println("✗ Error closing connection: " + e.getMessage());
            }
        }
    }
    
    /**
     * Read the Kubernetes service account token directly
     */
    private static String readServiceAccountToken() {
        try {
            File tokenFile = new File(SERVICE_ACCOUNT_TOKEN_FILE);
            if (!tokenFile.exists()) {
                System.out.println("⚠ Service account token file not found: " + SERVICE_ACCOUNT_TOKEN_FILE);
                return null;
            }
            
            String token = new String(java.nio.file.Files.readAllBytes(tokenFile.toPath())).trim();
            System.out.println("✓ Read service account token (" + token.length() + " characters)");
            System.out.println("  Token preview: " + token.substring(0, Math.min(50, token.length())) + "...");
            return token;
            
        } catch (Exception e) {
            System.out.println("✗ Failed to read service account token: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Try direct token authentication using manually exchanged STS token
     */
    private static Connection tryDirectTokenAuth(String kubernetesToken, URLClassLoader driverClassLoader) {
        try {
            System.out.println("Trying STS token exchange for WIF...");
            
            // Step 1: Exchange Kubernetes token for Google Cloud access token via STS
            String googleAccessToken = exchangeTokenWithSTS(kubernetesToken);
            if (googleAccessToken == null) {
                System.out.println("✗ STS token exchange failed");
                return null;
            }
            
            // Step 2: Use the Google access token with JDBC driver
            System.out.println("Using exchanged Google access token with JDBC driver...");
            Properties props = new Properties();
            props.setProperty("AuthenticationType", "1"); // Service Account (REQUIRED)
            props.setProperty("OAuthType", "1"); // Bearer Token (REQUIRED)
            props.setProperty("OAuthAccessToken", googleAccessToken); // Google access token
            props.setProperty("LogLevel", "6");
            props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log");
            
            System.out.println("  AuthenticationType: 1 (Service Account - REQUIRED)");
            System.out.println("  OAuthType: 1 (Bearer Token - REQUIRED)");
            System.out.println("  OAuthAccessToken: [GOOGLE_ACCESS_TOKEN_PROVIDED]");
            
            // Register the driver with DriverManager using the custom class loader
            System.out.println("Re-registering BigQuery driver for token auth...");
            Thread.currentThread().setContextClassLoader(driverClassLoader);
            Class<?> driverClass = driverClassLoader.loadClass("com.simba.googlebigquery.jdbc.Driver");
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(new DriverShim(driver));
            System.out.println("✓ BigQuery driver registered for token auth");
            
            // Create the connection
            return DriverManager.getConnection(DB_URL, props);
            
        } catch (SQLException e) {
            System.out.println("✗ Direct token auth failed: " + e.getMessage());
            return null;
        } catch (Exception e) {
            System.out.println("✗ Unexpected error in direct token auth: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Try using access token embedded in JDBC URL
     */
    private static Connection tryUrlTokenAuth(String kubernetesToken, URLClassLoader driverClassLoader) {
        try {
            System.out.println("Trying STS token exchange for URL embedding...");
            
            // Step 1: Exchange Kubernetes token for Google Cloud access token via STS
            String googleAccessToken = exchangeTokenWithSTS(kubernetesToken);
            if (googleAccessToken == null) {
                System.out.println("✗ STS token exchange failed");
                return null;
            }
            
            // Step 2: Embed token in JDBC URL
            String urlWithToken = DB_URL + ";AuthenticationType=1;OAuthType=1;OAuthAccessToken=" + googleAccessToken;
            
            System.out.println("Using JDBC URL with embedded access token...");
            System.out.println("  URL: " + DB_URL + ";AuthenticationType=1;OAuthType=1;OAuthAccessToken=[TOKEN]");
            
            Properties props = new Properties();
            props.setProperty("LogLevel", "6");
            props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log");
            
            // Register driver
            Thread.currentThread().setContextClassLoader(driverClassLoader);
            Class<?> driverClass = driverClassLoader.loadClass("com.simba.googlebigquery.jdbc.Driver");
            Driver driver = (Driver) driverClass.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(new DriverShim(driver));
            
            // Create connection
            return DriverManager.getConnection(urlWithToken, props);
            
        } catch (SQLException e) {
            System.out.println("✗ URL token auth failed: " + e.getMessage());
            return null;
        } catch (Exception e) {
            System.out.println("✗ Unexpected error in URL token auth: " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Exchange Kubernetes service account token for Google Cloud access token via STS
     */
    private static String exchangeTokenWithSTS(String kubernetesToken) {
        try {
            System.out.println("Performing STS token exchange...");
            
            // STS endpoint and parameters (from your WIF credentials)
            String stsUrl = "https://sts.googleapis.com/v1/token";
            String audience = "//iam.googleapis.com/projects/618647108376/locations/global/workloadIdentityPools/automation/providers/aks-aks-denodo-updater-sa";
            String scope = "https://www.googleapis.com/auth/cloud-platform";
            
            // Build the POST request body
            String requestBody = "audience=" + java.net.URLEncoder.encode(audience, "UTF-8") +
                "&grant_type=" + java.net.URLEncoder.encode("urn:ietf:params:oauth:grant-type:token-exchange", "UTF-8") +
                "&requested_token_type=" + java.net.URLEncoder.encode("urn:ietf:params:oauth:token-type:access_token", "UTF-8") +
                "&scope=" + java.net.URLEncoder.encode(scope, "UTF-8") +
                "&subject_token_type=" + java.net.URLEncoder.encode("urn:ietf:params:oauth:token-type:jwt", "UTF-8") +
                "&subject_token=" + java.net.URLEncoder.encode(kubernetesToken, "UTF-8");
            
            // Make the HTTP request to STS
            java.net.URL url = new java.net.URL(stsUrl);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            conn.setDoOutput(true);
            
            // Send request
            try (java.io.OutputStream os = conn.getOutputStream()) {
                os.write(requestBody.getBytes("UTF-8"));
            }
            
            // Read response
            int responseCode = conn.getResponseCode();
            System.out.println("  STS response code: " + responseCode);
            
            if (responseCode == 200) {
                try (java.io.BufferedReader br = new java.io.BufferedReader(
                    new java.io.InputStreamReader(conn.getInputStream(), "UTF-8"))) {
                    StringBuilder response = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        response.append(line);
                    }
                    
                    // Parse JSON response to extract access_token
                    String responseStr = response.toString();
                    System.out.println("  STS response: " + responseStr.substring(0, Math.min(200, responseStr.length())) + "...");
                    
                    // Better JSON parsing for access_token - handle whitespace and formatting
                    if (responseStr.contains("access_token")) {
                        // Find the access_token field, handling various JSON formatting
                        int startIdx = responseStr.indexOf("\"access_token\"");
                        if (startIdx > -1) {
                            // Find the start of the token value (after the quote)
                            int valueStart = responseStr.indexOf("\"", responseStr.indexOf(":", startIdx)) + 1;
                            // Find the end of the token value (next quote)
                            int valueEnd = responseStr.indexOf("\"", valueStart);
                            
                            if (valueStart > 0 && valueEnd > valueStart) {
                                String accessToken = responseStr.substring(valueStart, valueEnd);
                                System.out.println("✓ Successfully exchanged token via STS");
                                System.out.println("  Access token length: " + accessToken.length());
                                System.out.println("  Access token preview: " + accessToken.substring(0, Math.min(50, accessToken.length())) + "...");
                                return accessToken;
                            } else {
                                System.out.println("✗ Failed to find token boundaries in STS response");
                                System.out.println("  valueStart: " + valueStart + ", valueEnd: " + valueEnd);
                                return null;
                            }
                        } else {
                            System.out.println("✗ access_token field not found in STS response");
                            return null;
                        }
                    } else {
                        System.out.println("✗ No access_token in STS response");
                        return null;
                    }
                }
            } else {
                // Read error response
                try (java.io.BufferedReader br = new java.io.BufferedReader(
                    new java.io.InputStreamReader(conn.getErrorStream(), "UTF-8"))) {
                    StringBuilder errorResponse = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        errorResponse.append(line);
                    }
                    System.out.println("✗ STS token exchange failed: " + errorResponse.toString());
                }
                return null;
            }
            
        } catch (Exception e) {
            System.out.println("✗ STS token exchange error: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * Driver wrapper to handle class loader issues with DriverManager
     */
    static class DriverShim implements java.sql.Driver {
        private java.sql.Driver driver;
        
        DriverShim(java.sql.Driver driver) {
            this.driver = driver;
        }
        
        public boolean acceptsURL(String url) throws SQLException {
            return driver.acceptsURL(url);
        }
        
        public Connection connect(String url, Properties info) throws SQLException {
            return driver.connect(url, info);
        }
        
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return driver.getPropertyInfo(url, info);
        }
        
        public int getMajorVersion() {
            return driver.getMajorVersion();
        }
        
        public int getMinorVersion() {
            return driver.getMinorVersion();
        }
        
        public boolean jdbcCompliant() {
            return driver.jdbcCompliant();
        }
        
        public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
            return driver.getParentLogger();
        }
    }
}

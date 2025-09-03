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

public class chk_jdbc {
    
    // BigQuery connection parameters - modify these according to your setup
    private static final String DB_URL = "jdbc:bigquery://https://private.googleapis.com/bigquery/v2:443;ProjectId=tnn-sb-to970548-1;DefaultDataset=your_dataset";
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
     * Load BigQuery JDBC drivers from the specified directory
     */
    private static URLClassLoader loadBigQueryDrivers() {
        try {
            File driverDir = new File(BIGQUERY_DRIVER_PATH);
            if (!driverDir.exists() || !driverDir.isDirectory()) {
                System.out.println("Warning: BigQuery driver directory not found: " + BIGQUERY_DRIVER_PATH);
                return null;
            }
            
            File[] jarFiles = driverDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".jar"));
            if (jarFiles == null || jarFiles.length == 0) {
                System.out.println("Warning: No JAR files found in BigQuery driver directory");
                return null;
            }
            
            URL[] urls = new URL[jarFiles.length];
            for (int i = 0; i < jarFiles.length; i++) {
                urls[i] = jarFiles[i].toURI().toURL();
            }
            
            URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
            Thread.currentThread().setContextClassLoader(classLoader);
            
            System.out.println("✓ BigQuery drivers loaded from: " + BIGQUERY_DRIVER_PATH);
            
            return classLoader;
            
        } catch (Exception e) {
            System.out.println("✗ Error loading BigQuery drivers: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * Test if we can load Google credentials directly
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
     * Try multiple authentication methods
     */
    private static void tryMultipleAuthMethods(URLClassLoader driverClassLoader) throws Exception {
        Connection connection = null;
        
        // Method 1: ADC with External Account
        try {
            System.out.println("\n=== Attempting Method 1: ADC with External Account ===");
            connection = tryADCAuth(driverClassLoader);
            if (connection != null) {
                System.out.println("✓ Method 1 succeeded!");
                testConnection(connection);
                return;
            }
        } catch (Exception e) {
            System.out.println("✗ Method 1 failed: " + e.getMessage());
        }
        
        // Method 2: Service Account Key approach
        try {
            System.out.println("\n=== Attempting Method 2: Service Account Key ===");
            connection = tryServiceAccountAuth(driverClassLoader);
            if (connection != null) {
                System.out.println("✓ Method 2 succeeded!");
                testConnection(connection);
                return;
            }
        } catch (Exception e) {
            System.out.println("✗ Method 2 failed: " + e.getMessage());
        }
        
        // Method 3: Direct OAuth Token (if we can get one)
        try {
            System.out.println("\n=== Attempting Method 3: Manual Token ===");
            connection = tryManualTokenAuth(driverClassLoader);
            if (connection != null) {
                System.out.println("✓ Method 3 succeeded!");
                testConnection(connection);
                return;
            }
        } catch (Exception e) {
            System.out.println("✗ Method 3 failed: " + e.getMessage());
        }
        
        throw new Exception("All authentication methods failed");
    }
    
    private static Connection tryADCAuth(URLClassLoader driverClassLoader) throws Exception {
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", CREDENTIAL_FILE_PATH);
        
        Properties props = new Properties();
        props.setProperty("AuthenticationType", "0"); // ADC
        props.setProperty("OAuthType", "3"); // External Account
        props.setProperty("LogLevel", "6");
        props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log");
        
        loadDriver(driverClassLoader);
        return DriverManager.getConnection(DB_URL, props);
    }
    
    private static Connection tryServiceAccountAuth(URLClassLoader driverClassLoader) throws Exception {
        Properties props = new Properties();
        props.setProperty("AuthenticationType", "1"); // Service Account
        props.setProperty("KeyFile", CREDENTIAL_FILE_PATH);
        props.setProperty("LogLevel", "6");
        props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log");
        
        loadDriver(driverClassLoader);
        return DriverManager.getConnection(DB_URL, props);
    }
    
    private static Connection tryManualTokenAuth(URLClassLoader driverClassLoader) throws Exception {
        // This would require manual token generation, skip for now
        throw new Exception("Manual token generation not implemented yet");
    }
    
    private static void loadDriver(URLClassLoader driverClassLoader) throws Exception {
        try {
            Class<?> driverClassObj = Class.forName("com.simba.googlebigquery.jdbc.Driver", true, driverClassLoader);
            java.sql.Driver driver = (java.sql.Driver) driverClassObj.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(new DriverShim(driver));
        } catch (Exception e) {
            throw new Exception("Failed to load BigQuery driver: " + e.getMessage(), e);
        }
    }
    
    private static void testConnection(Connection connection) throws SQLException {
        if (connection != null) {
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println("Database Product: " + metaData.getDatabaseProductName());
            System.out.println("Database Version: " + metaData.getDatabaseProductVersion());
            System.out.println("Driver Name: " + metaData.getDriverName());
            System.out.println("Driver Version: " + metaData.getDriverVersion());
            
            if (connection.isValid(10)) {
                System.out.println("✓ Connection is valid and responsive");
            } else {
                System.out.println("✗ Connection is not responding");
            }
            
            connection.close();
            System.out.println("✓ Connection closed successfully");
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
                    // Test the connection and close it
                    testConnectionSuccess(tokenConnection);
                    return;
                }
            }
            
            // Approach 2: ADC + OAuthType=2 (original approach)
            System.out.println("\n--- Approach 2: ADC + OAuthType=2 ---");
            Properties props = new Properties();
            
            // Explicit ADC + required OAuthType combination
            props.setProperty("AuthenticationType", "0"); // Application Default Credentials
            props.setProperty("OAuthType", "2"); // WIF/Workload Identity Federation (required!)
            props.setProperty("LogLevel", "6"); // Enable detailed logging
            props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log"); // Log file location
            
            System.out.println("Connection properties (ADC=0 + OAuthType=2 explicit combination):");
            System.out.println("  AuthenticationType: " + props.getProperty("AuthenticationType") + " (Application Default Credentials)");
            System.out.println("  OAuthType: " + props.getProperty("OAuthType") + " (WIF/Workload Identity Federation - REQUIRED)");
            System.out.println("  GOOGLE_APPLICATION_CREDENTIALS: " + System.getProperty("GOOGLE_APPLICATION_CREDENTIALS"));
            System.out.println("  Note: Explicit ADC + OAuthType combination for WIF");
            
            // Try to load BigQuery driver - we know it's the Simba driver
            try {
                System.out.println("Loading BigQuery JDBC driver...");
                Class<?> driverClassObj = Class.forName("com.simba.googlebigquery.jdbc.Driver", true, driverClassLoader);
                
                // Instantiate the driver to register it with DriverManager
                java.sql.Driver driver = (java.sql.Driver) driverClassObj.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(new DriverShim(driver));
                
                System.out.println("✓ Successfully loaded and registered BigQuery driver");
            } catch (Exception e) {
                throw new Exception("Failed to load BigQuery driver: " + e.getMessage(), e);
            }
            
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
        
        if (errorMessage.contains("unable to obtain application default credentials")) {
            System.out.println("• The driver is trying to use Application Default Credentials (ADC)");
            System.out.println("• Set GOOGLE_APPLICATION_CREDENTIALS environment variable to WIF credential file");
            System.out.println("• Verify the WIF credential file is in the correct JSON format");
            System.out.println("• Check if the service account token file is accessible");
            System.out.println("• Try setting LogLevel=6 to see detailed authentication logs");
        }
        
        if (errorMessage.contains("error requesting access token") || errorMessage.contains("httptransport io error")) {
            System.out.println("• Check WIF credential file exists at: " + CREDENTIAL_FILE_PATH);
            System.out.println("• Verify service account token file exists at: " + SERVICE_ACCOUNT_TOKEN_FILE);
            System.out.println("• Ensure the workload identity pool and provider are correctly configured");
            System.out.println("• Verify the service account has BigQuery permissions");
            System.out.println("• Check if the Kubernetes service account is properly annotated");
            System.out.println("• Validate the audience field in WIF credentials matches the workload identity provider");
        }
        
        if (errorMessage.contains("required setting oauthtype") || errorMessage.contains("oauthtype is not present")) {
            System.out.println("• Add OAuthType=3 for External Account authentication");
            System.out.println("• Ensure AuthenticationType=4 for External Account");
            System.out.println("• Verify CredentialsPath points to valid WIF credential file");
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
            props.setProperty("AuthenticationType", "1"); // Service Account authentication
            props.setProperty("OAuthType", "1"); // Bearer token OAuth type
            props.setProperty("OAuthAccessToken", googleAccessToken); // Google access token
            props.setProperty("LogLevel", "6");
            props.setProperty("LogPath", "/opt/denodo/work/eloi_work/bigquery_jdbc.log");
            
            System.out.println("  AuthenticationType: 1 (Service Account)");
            System.out.println("  OAuthType: 1 (Bearer Token)");
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
                        String pattern = "\"access_token\"\\s*:\\s*\"";
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
        
        public int getMajorVersion() {
            return driver.getMajorVersion();
        }
        
        public int getMinorVersion() {
            return driver.getMinorVersion();
        }
        
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
            return driver.getPropertyInfo(url, info);
        }
        
        public boolean jdbcCompliant() {
            return driver.jdbcCompliant();
        }
        
        public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
            return driver.getParentLogger();
        }
    }
}
package com.maersk.kafkautility.utils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;

@Slf4j
@Component
public class AzureUtil {

    private AzureUtil()
    {
        //private constructor
    }

    @Autowired
    private static ApplicationContext context;

    private static final String PAYLOAD_FILE_NAME = "${events-payload.file-name}";
    private static final String AZURE_STORAGE_ACCOUNT_NAME = "${azure.storage.account-name}";
    private static final String AZURE_STORAGE_ACCOUNT_KEY = "${azure.storage.account-key}";
    private static final String AZURE_STORAGE_CONTAINER_NAME = "${azure.storage.container-name}";
    private static final String AZURE_STORAGE_ENDPOINT_SUFFIX = "${azure.storage.endpoint-suffix}";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s";

    public static CloudBlobContainer getCloudBlobContainer() throws StorageException, URISyntaxException, InvalidKeyException {
        String containerName = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_CONTAINER_NAME);
        log.info("containerName: {}", containerName);
        CloudStorageAccount storageAccountDest = CloudStorageAccount.parse(getConnectionString());
        CloudBlobClient blobClientDest = storageAccountDest.createCloudBlobClient();
        CloudBlobContainer containerDest = blobClientDest.getContainerReference(containerName);
        log.info("Container: {}", containerDest.getName());
        return containerDest;
    }

    private static String getConnectionString()
    {
        String storageAccountName = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ACCOUNT_NAME);
        String storageAccountKey = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ACCOUNT_KEY);
        String endpointSuffix = context.getEnvironment().resolvePlaceholders(AZURE_STORAGE_ENDPOINT_SUFFIX);
        String connectionString = String.format(CONNECTION_STRING,storageAccountName,storageAccountKey,endpointSuffix);
        log.info("Azure storage connection string: {}", connectionString);
        return connectionString;
    }

    public static String getPayloadFilename()
    {
        String fileName = context.getEnvironment().resolvePlaceholders(PAYLOAD_FILE_NAME);
        fileName = fileName.concat("_").concat(UUID.randomUUID().toString()).concat(".dat");
        log.info("Payload file name: {}", fileName);
        return fileName;
    }
}

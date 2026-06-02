package com.karix.dlrreceiver.util;

import com.karix.commonutil.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.karix.commonutil.Validation.ValidationException;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

import java.nio.file.Files;
import java.nio.file.Path;

@Component
public class ApiValidations {

    private static final Logger log = LogManager.getLogger(ApiValidations.class);

    private final String downloadDirectoryPath;
    private final Integer fileMaxByteSize;
    private final String allowedExtension;

    public ApiValidations(
            @Value("${download.directory}") String downloadDirectoryPath,
            @Value("${file.max.bytes}") Integer fileMaxByteSize,
            @Value("${allowed.file.extension}") String allowedExtension ){

        this.downloadDirectoryPath = downloadDirectoryPath;
        this.fileMaxByteSize = fileMaxByteSize;
        this.allowedExtension=allowedExtension;
    }

    public void smsSubProcess(SMSSubmissionRequest request) {
        if (request == null) {
            throw new ValidationException(
                    StatusCode.INVALID_REQUEST.name(),
                    StatusCode.INVALID_REQUEST.getDescription()
            );
        }
    }

    public void smsDnProcess(SMSDNRequest request) {
        if (request == null) {
            throw new ValidationException(
                    StatusCode.INVALID_REQUEST.name(),
                    StatusCode.INVALID_REQUEST.getDescription()
            );
        }
    }

    public void emailSubProcess(EmailSubRequest request) {

        if (request == null) {
            throw new ValidationException(
                    StatusCode.INVALID_REQUEST.name(),
                    StatusCode.INVALID_REQUEST.getDescription()
            );
        }

        String ackId = request.getAckid();
        log.debug("filename:{}",request.getFilename());

        if (ackId == null || ackId.isBlank()) {
            throw new ValidationException(
                    StatusCode.INVALID_REQUEST.name(),
                    StatusCode.INVALID_REQUEST.getDescription()
            );
        }
    }

    public void emailDnProcess(EmailDnRequest request) {
        if (request == null) {
            throw new ValidationException(
                    StatusCode.INVALID_REQUEST.name(),
                    StatusCode.INVALID_REQUEST.getDescription()
            );
        }
    }

    public Path writeToNfs(EmailSubRequest request) {

        // Skip processing if filename or file_content is null/blank
        if (request.getFilename() == null || request.getFilename().isBlank() ||
                request.getFile_content() == null || request.getFile_content().isBlank()) {

            log.info("Skipping NFS write as filename or file_content is empty for ackId={}",
                    request.getAckid());

            return null;
        }

        final byte[] decoded;

        try {
            decoded = Base64.getDecoder().decode(request.getFile_content());
        } catch (IllegalArgumentException e) {
            log.error("Invalid Base64 content for ackId: {}", request.getAckid(), e);
            return null;
        }

        // File size validation
        if (decoded.length > fileMaxByteSize) {
            log.warn("File size exceeded for ackId: {}. Size: {} bytes, Max allowed: {} bytes",
                    request.getAckid(), decoded.length, fileMaxByteSize);
            return null;
        }

        String lowerFilename = request.getFilename().toLowerCase();

        //Extension validation
        if (!lowerFilename.endsWith(allowedExtension)) {
            log.warn("Invalid file type for ackId: {}. Filename: {}. Only {} files allowed",
                    request.getAckid(), request.getFilename(), allowedExtension);
            return null;
        }

        Path dirPath = Paths.get(downloadDirectoryPath, request.getAckid());

        try {
            Files.createDirectories(dirPath);

            String safeFilename = lowerFilename
                    .replaceAll("[^a-zA-Z0-9._-]", "_");

            Path filePath = dirPath.resolve(safeFilename);

            Files.write(
                    filePath,
                    decoded,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );

            log.info("Stored file for ackId {} at {}", request.getAckid(), filePath);

            return filePath;

        } catch (IOException e) {
            log.error("NFS write failed for ackId {} at {}", request.getAckid(), dirPath, e);
            throw new RuntimeException("File storage failed", e);
        }
    }
}

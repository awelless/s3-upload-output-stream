package com.github.awelless.s3upload;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class S3UploadOutputStream extends OutputStream {

    private static final int DEFAULT_MAX_BUFFER_SIZE = 10 * 1024 * 1024;

    private final S3AsyncClient s3Client;
    private final String bucket;
    private final String key;

    private final int bufferSize;
    private byte[] buffer; // mutable, so it can be dropped when steam is closed

    private int bufferWriteIndex = 0;

    private boolean closed = false;

    private final CompletableFuture<String> uploadIdFuture = new CompletableFuture<>();
    private final CompletableFuture<CompleteMultipartUploadResponse> completionFuture = new CompletableFuture<>();
    private final List<CompletableFuture<CompletedPart>> completedPartFutures = new ArrayList<>();

    public S3UploadOutputStream(S3AsyncClient s3Client, String bucket, String key) {
        this(s3Client, bucket, key, DEFAULT_MAX_BUFFER_SIZE);
    }

    public S3UploadOutputStream(S3AsyncClient s3Client, String bucket, String key, int bufferSize) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];
    }

    @Override
    public void write(int b) throws IOException {
        assertOpen();

        buffer[bufferWriteIndex++] = (byte) b;

        if (bufferWriteIndex == bufferSize) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        assertOpen();

        if (completedPartFutures.isEmpty()) {
            createMultipartUpload()
                    .thenAccept(response -> uploadIdFuture.complete(response.uploadId()));
        }

        // all mutable values should never be calculated in future lambda body
        byte[] bufferCopy = Arrays.copyOf(buffer, bufferWriteIndex);
        int partNumber = completedPartFutures.size() + 1;
        CompletableFuture<CompletedPart> completedPartFuture = uploadIdFuture.thenCompose(uploadId -> uploadPart(uploadId, bufferCopy, partNumber));

        completedPartFutures.add(completedPartFuture);

        bufferWriteIndex = 0;
    }

    private CompletableFuture<CreateMultipartUploadResponse> createMultipartUpload() {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        return s3Client.createMultipartUpload(request);
    }

    private CompletableFuture<CompletedPart> uploadPart(String uploadId, byte[] buffer, int partNumber) {
        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        return s3Client.uploadPart(request, AsyncRequestBody.fromBytes(buffer))
                .thenApply(uploadPartResponse -> CompletedPart.builder()
                        .partNumber(partNumber)
                        .eTag(uploadPartResponse.eTag())
                        .build());
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        // if buffer has some data, flush it
        if (bufferWriteIndex > 0) {
            flush();
        }

        closed = true;

        CompletableFuture.allOf(completedPartFutures.toArray(new CompletableFuture[0])) // wait till all part uploads are done
                .thenCompose(unused2 -> completeMultipartUpload())
                .whenComplete((response, error) -> {
                    if (error != null) {
                        completionFuture.completeExceptionally(error);
                    } else {
                        completionFuture.complete(response);
                    }
                });

        buffer = null; // no writes happen when stream is closed, therefore buffer can be dropped
    }

    private CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload() {
        // create completed parts
        List<CompletedPart> completedParts = completedPartFutures.stream()
                .map(CompletableFuture::join) // all upload features are completed
                .collect(toList());

        completedPartFutures.clear(); // after retrieving all CompletedParts, futures can be dropped

        // and complete upload request
        return uploadIdFuture.thenCompose(uploadId -> {
            CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder()
                            .parts(completedParts)
                            .build())
                    .build();

            return s3Client.completeMultipartUpload(completeRequest);
        });
    }

    private void assertOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    /**
     * Returns {@link CompletableFuture} that completes only when stream is closed and upload is finished.
     * <br>
     * <b>Note: DO NOT call {@link CompletableFuture#get()} on returned future
     * while {@link S3UploadOutputStream} is not closed</b>
     */
    public CompletableFuture<CompleteMultipartUploadResponse> getCompletion() {
        return completionFuture;
    }
}

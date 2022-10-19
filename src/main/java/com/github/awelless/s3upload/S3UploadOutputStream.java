package com.github.awelless.s3upload;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class S3UploadOutputStream extends OutputStream {

    private static final int DEFAULT_MAX_BUFFER_SIZE = 10 * 1024 * 1024;

    private final S3AsyncClient s3Client;
    private final String bucket;
    private final String key;

    private final int bufferSize;
    private final byte[] buffer;

    private int bufferWriteIndex = 0;

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final CompletableFuture<String> uploadIdFuture = new CompletableFuture<>();
    private final List<CompletableFuture<OrderedUploadPartResponse>> uploadPartFutures = new ArrayList<>();

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
    public void write(int i) throws IOException {
        assertOpen();

        buffer[bufferWriteIndex++] = (byte) i;

        if (bufferWriteIndex == bufferSize) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        assertOpen();

        if (uploadPartFutures.isEmpty()) {
            startUpload();
        }

        CompletableFuture<OrderedUploadPartResponse> uploadPartFuture = uploadIdFuture.thenCompose(this::createUploadPartFuture);
        uploadPartFutures.add(uploadPartFuture);

        bufferWriteIndex = 0;
    }

    private void startUpload() {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        s3Client.createMultipartUpload(request).thenAccept(response -> uploadIdFuture.complete(response.uploadId()));
    }

    private CompletableFuture<OrderedUploadPartResponse> createUploadPartFuture(String uploadId) {
        int partNumber = uploadPartFutures.size() + 1;

        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        return s3Client.uploadPart(request, AsyncRequestBody.fromBytes(buffer))
                .thenApply(uploadPartResponse -> new OrderedUploadPartResponse(partNumber, uploadPartResponse));
    }

    @Override
    public void close() throws IOException {
        assertOpen();

        // if buffer has some data, flush it
        if (bufferWriteIndex > 0) {
            flush();
        }

        closeFuture.complete(null);
    }

    private void assertOpen() throws IOException {
        if (closeFuture.isDone()) {
            throw new IOException("Stream is closed");
        }
    }

    public CompletableFuture<CompleteMultipartUploadResponse> getCompletionFuture() {
        return closeFuture.thenCompose(u1 -> // wait till stream is closed
                // wait till all part uploads are done
                CompletableFuture.allOf(uploadPartFutures.toArray(new CompletableFuture[0])).thenCompose(u2 -> {
                    // create completed parts
                    List<CompletedPart> completedParts = uploadPartFutures.stream()
                            .map(CompletableFuture::join)
                            .map(OrderedUploadPartResponse::toCompletedPart)
                            .collect(toList());

                    // and complete upload request
                    CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .multipartUpload(CompletedMultipartUpload.builder()
                                    .parts(completedParts)
                                    .build())
                            .build();

                    return s3Client.completeMultipartUpload(completeRequest);
                })
        );
    }

    private static class OrderedUploadPartResponse {

        private final int uploadPartNumber;
        private final UploadPartResponse uploadPartResponse;

        private OrderedUploadPartResponse(int uploadPartNumber, UploadPartResponse uploadPartResponse) {
            this.uploadPartNumber = uploadPartNumber;
            this.uploadPartResponse = uploadPartResponse;
        }

        CompletedPart toCompletedPart() {
            return CompletedPart.builder()
                    .partNumber(uploadPartNumber)
                    .eTag(uploadPartResponse.eTag())
                    .checksumCRC32(uploadPartResponse.checksumCRC32())
                    .checksumCRC32C(uploadPartResponse.checksumCRC32C())
                    .checksumSHA1(uploadPartResponse.checksumSHA1())
                    .checksumSHA256(uploadPartResponse.checksumSHA256())
                    .build();
        }
    }
}

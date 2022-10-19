# S3 Upload Output Stream

Allows uploading of huge amounts of data in multipart chunks to s3.

### Usage example
```java
S3AsyncClient s3Client = S3AsyncClient.create();
String bucket = "some.bucket";
String key = "some.key";
int bufferSize = 10 * 1024 * 1024; // default buffer size is 10 MB

// stream shouldn't be opened in try-with-resources, because part upload can run after .close()  
S3UploadOutputStream outputStream = new S3UploadOutputStream(s3Client, bucket, key, bufferSize);

try {
    // write data to stream
    outputStream.write(1);
    outputStream.write(2);
    outputStream.write(3);
} catch (IOException e) {
} finally {
    try {
        outputStream.close();
    } catch (IOException e) {
    }
}

// when stream is closed some part uploads can be still running
CompletableFuture<CompleteMultipartUploadResponse> completionFuture = outputStream.getCompletionFuture();

// further processing...
```

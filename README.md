# S3 Upload Output Stream

Allows uploading of huge amounts of data in multipart chunks to s3.

### Usage example
```java
S3AsyncClient s3Client = S3AsyncClient.create();
String bucket = "some.bucket";
String key = "some.key";
int bufferSize = 10 * 1024 * 1024; // default buffer size is 10 MB

// use this CompletableFuture to have an access to CompleteMultipartUploadResponse when multipart upload is completed 
CompletableFuture<CompleteMultipartUploadResponse> completion;

try (S3UploadOutputStream outputStream = new S3UploadOutputStream(s3Client, bucket, key, bufferSize)) {
    completion = outputStream.getCompletion();
    
    // write data to stream
    outputStream.write(1);
    outputStream.write(2);
    outputStream.write(3);

} catch (IOException e) {
    // error handling
    completion = new CompletableFuture<>();
    completion.completeExceptionally(e);
}

// when stream is closed some part uploads can still be running

// we can continue working asynchronously or simply call completion.get()
completion.thenAccept(response -> System.out.println("Multipart upload is finished. Uri: " + response.location()));
```

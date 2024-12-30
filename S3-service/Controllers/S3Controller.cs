using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using S3_service.Model;

namespace S3_service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class S3Controller : ControllerBase
    {
        private readonly IAmazonS3 _s3Client;
        public S3Controller(IAmazonS3 s3Client)
        {
            _s3Client = s3Client;
        }

        [HttpPost("create-bucket")]
        public async Task<IActionResult> CreateBucketAsync(string bucketName)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);
            if (bucketExists) return BadRequest($"Bucket {bucketName} already exists.");
            await _s3Client.PutBucketAsync(bucketName);
            return Ok($"Bucket {bucketName} created.");
        }

        [HttpGet("get-all-bucket")]
        public async Task<IActionResult> GetAllBucketAsync()
        {
            var data = await _s3Client.ListBucketsAsync();
            var buckets = data.Buckets.Select(b => { return b.BucketName; });
            return Ok(buckets);
        }

        [HttpPost("bulk-upload-by-item")]
        public async Task<IActionResult> UploadBulkAsync(List<IFormFile> files, string bucketName, string? prefix)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);

            var uploadTasks = files.Select(async file =>
            {
                var request = new PutObjectRequest()
                {
                    BucketName = bucketName,
                    Key = string.IsNullOrEmpty(prefix)
                        ? file.FileName
                        : $"{prefix.TrimEnd('/')}/{file.FileName}",
                    InputStream = file.OpenReadStream()
                };

                request.Metadata.Add("Content-Type", file.ContentType);

                try
                {
                    await _s3Client.PutObjectAsync(request);
                }
                catch (Exception ex)
                {
                    return $"Error uploading file {file.FileName}: {ex.Message}";
                }

                return null;
            }).ToList();

            var uploadResults = await Task.WhenAll(uploadTasks);

            var errors = uploadResults.Where(result => result != null).ToList();
            if (errors.Any())
            {
                return StatusCode(500, string.Join(Environment.NewLine, errors));
            }

            return Ok($"{files.Count} file(s) uploaded to S3 successfully!");
        }

        [HttpPost("bulk-upload-by-folder")]
        public async Task<IActionResult> UploadBulkAsync(string folderPath, string bucketName, string? prefix)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);

            var files = Directory.GetFiles(folderPath);

            if (!files.Any())
                return BadRequest("No files found in the folder to upload.");

            var uploadTasks = files.Select(async filePath =>
            {
                var fileName = Path.GetFileName(filePath);
                var request = new PutObjectRequest()
                {
                    BucketName = bucketName,
                    Key = string.IsNullOrEmpty(prefix)
                        ? fileName
                        : $"{prefix.TrimEnd('/')}/{fileName}",
                    FilePath = filePath
                };

                try
                {
                    await _s3Client.PutObjectAsync(request);
                }
                catch (Exception ex)
                {
                    return $"Error uploading file {fileName}: {ex.Message}";
                }

                return null;
            }).ToList();

            var uploadResults = await Task.WhenAll(uploadTasks);

            var errors = uploadResults.Where(result => result != null).ToList();
            if (errors.Any())
            {
                return StatusCode(500, string.Join(Environment.NewLine, errors));
            }

            return Ok($"{files.Length} file(s) uploaded to S3 successfully!");
        }

        [HttpDelete("delete-bucket")]
        public async Task<IActionResult> DeleteBucketAsync(string bucketName)
        {
            await _s3Client.DeleteBucketAsync(bucketName);
            return NoContent();
        }

        [HttpPost("upload-file")]
        public async Task<IActionResult> UploadFileAsync(IFormFile file, string bucketName, string? prefix)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);
            if (!bucketExists) return NotFound($"Bucket {bucketName} does not exist.");
            var request = new PutObjectRequest()
            {
                BucketName = bucketName,
                Key = string.IsNullOrEmpty(prefix) ? file.FileName : $"{prefix?.TrimEnd('/')}/{file.FileName}",
                InputStream = file.OpenReadStream()
            };
            request.Metadata.Add("Content-Type", file.ContentType);
            await _s3Client.PutObjectAsync(request);
            return Ok($"File {prefix}/{file.FileName} uploaded to S3 successfully!");
        }

        [HttpGet("get-all-file")]
        public async Task<IActionResult> GetAllFilesAsync(string bucketName, string? prefix)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);
            if (!bucketExists) return NotFound($"Bucket {bucketName} does not exist.");
            var request = new ListObjectsV2Request()
            {
                BucketName = bucketName,
                Prefix = prefix
            };
            var result = await _s3Client.ListObjectsV2Async(request);
            var s3Objects = result.S3Objects.Select(s =>
            {
                var urlRequest = new GetPreSignedUrlRequest()
                {
                    BucketName = bucketName,
                    Key = s.Key,
                    Expires = DateTime.UtcNow.AddMinutes(1)
                };
                return new S3ObjectDto()
                {
                    Name = s.Key.ToString(),
                    PresignedUrl = _s3Client.GetPreSignedURL(urlRequest),
                };
            });
            return Ok(s3Objects);
        }

        [HttpGet("get-by-key")]
        public async Task<IActionResult> GetFileByKeyAsync(string bucketName, string key)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);
            if (!bucketExists) return NotFound($"Bucket {bucketName} does not exist.");
            var s3Object = await _s3Client.GetObjectAsync(bucketName, key);
            return File(s3Object.ResponseStream, s3Object.Headers.ContentType);
        }

        [HttpDelete("delete-file")]
        public async Task<IActionResult> DeleteFileAsync(string bucketName, string key)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);
            if (!bucketExists) return NotFound($"Bucket {bucketName} does not exist");
            await _s3Client.DeleteObjectAsync(bucketName, key);
            return NoContent();
        }

        [HttpPost("bulk-delete-by-type")]
        public async Task<IActionResult> DeleteBulkAsync(string bucketName, string? extension = null, bool deleteAll = false)
        {
            var bucketExists = await _s3Client.DoesS3BucketExistAsync(bucketName);
            if (!bucketExists)
                return NotFound($"Bucket {bucketName} does not exist.");

            var listObjectsRequest = new ListObjectsV2Request
            {
                BucketName = bucketName
            };

            var objectList = new List<S3Object>();

            do
            {
                var listResponse = await _s3Client.ListObjectsV2Async(listObjectsRequest);
                objectList.AddRange(listResponse.S3Objects);
                listObjectsRequest.ContinuationToken = listResponse.NextContinuationToken;

            } while (!string.IsNullOrEmpty(listObjectsRequest.ContinuationToken));

            if (!deleteAll && !string.IsNullOrEmpty(extension))
            {
                objectList = objectList.Where(obj => obj.Key.EndsWith(extension, StringComparison.OrdinalIgnoreCase)).ToList();
            }

            if (!objectList.Any())
                return BadRequest("No files found to delete.");

            var deleteObjectsRequest = new DeleteObjectsRequest
            {
                BucketName = bucketName,
                Objects = objectList.Select(obj => new KeyVersion { Key = obj.Key }).ToList()
            };

            try
            {
                var deleteResponse = await _s3Client.DeleteObjectsAsync(deleteObjectsRequest);
                if (deleteResponse.HttpStatusCode == HttpStatusCode.OK)
                {
                    return Ok($"file(s) deleted successfully.");
                }
                else
                {
                    return StatusCode(500, "An error occurred while deleting the files.");
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error deleting files: {ex.Message}");
            }
        }
    }
}

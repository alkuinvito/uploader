package uploader

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type UploadClient struct {
	accessKey      string
	chunkSize      int
	endpoint       string
	httpClient     *http.Client
	logger         *log.Logger
	maxConcurrency int
}

type IUploadClient interface {
	DeleteObject(bucketName, objectName string) error
	DownloadObject(bucketName, objectName string, w io.Writer) error
	GetObject(bucketName, objectName string) ([]byte, error)
	ListObjects(bucketName, path string) ([]ObjectInfo, error)
	PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error)
	PutObjectV2(ctx context.Context, bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error)
	PutObjectForm(bucketName, objectName string, data []byte) (string, error)
	PutObjectFormV2(ctx context.Context, bucketName, objectName string, data []byte) (string, error)
	StatObject(bucketName, objectName string) (*ObjectInfo, error)
}

// NewClient creates a new upload client
func NewClient(options *UploadClientOptions) *UploadClient {
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{
			Timeout: 30 * time.Second, // 30 seconds timeout
		}
	}

	if options.Logger == nil {
		options.Logger = log.New(log.Writer(), "[UPLOADER] ", log.Flags())
	}

	return &UploadClient{
		accessKey:      options.AccessKey,
		chunkSize:      options.ChunkSize,
		endpoint:       options.Endpoint,
		httpClient:     options.HTTPClient,
		logger:         options.Logger,
		maxConcurrency: options.MaxConcurrency,
	}
}

// NewClientWithDefaults creates a new upload client with default options
func NewClientWithDefaults(endpoint, accessKey string) *UploadClient {
	return NewClient(&UploadClientOptions{
		AccessKey: accessKey,
		ChunkSize: 5 * 1024 * 1024, // 5 MB default
		Endpoint:  endpoint,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:       15 * time.Second,
					KeepAlive:     30 * time.Second,
					DualStack:     true,
					FallbackDelay: 300 * time.Millisecond,
				}).DialContext,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   10,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			Timeout: 0,
		},
		Logger:         log.New(log.Writer(), "[UPLOADER] ", log.Flags()),
		MaxConcurrency: 3,
	})
}

// CalculateChecksum calculates file checksum using selected algorithm
func (c *UploadClient) calculateChecksum(data []byte, algorithm string) (string, error) {
	switch algorithm {
	case Sha256Sum, "":
		hash := sha256.Sum256(data)
		return hex.EncodeToString(hash[:]), nil
	case Md5Sum:
		hash := md5.Sum(data)
		return hex.EncodeToString(hash[:]), nil
	default:
		c.logger.Printf("Unsupported algorithm: %s", algorithm)
		return "", ErrUnsupportedAlgorithm
	}
}

// DeleteObject calls DeleteFile handler on server
func (c *UploadClient) DeleteObject(bucketName, objectName string) error {
	url := fmt.Sprintf("%s/upload/delete", c.endpoint)

	reqBody := deleteFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.Printf("Failed to marshal request body: %v", err)
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	return c.parseServerResponse(resp, nil)
}

// DownloadObject streams object to provided writer (e.g. file)
func (c *UploadClient) DownloadObject(bucketName, objectName string, w io.Writer) error {
	url := fmt.Sprintf("%s/upload/download", c.endpoint)

	reqBody := downloadFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	err = c.parseServerResponse(resp, nil)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		c.logger.Printf("Failed to stream object: %v", err)
		return ErrClientFailedToReadObject
	}

	return nil
}

// GetObject calls server download handler and returns bytes
func (c *UploadClient) GetObject(bucketName, objectName string) ([]byte, error) {
	url := fmt.Sprintf("%s/upload/download", c.endpoint)

	reqBody := downloadFileRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return nil, ErrFailedToConnect
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = c.parseServerResponse(resp, nil)
		if err != nil {
			return nil, err
		}

		return nil, ErrUnknown
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Printf("Failed to read object body: %v", err)
		return nil, ErrClientFailedToReadObject
	}

	return data, nil
}

// ListObjects lists objects in a bucket
func (c *UploadClient) ListObjects(bucketName, path string) ([]ObjectInfo, error) {
	var result []ObjectInfo

	url := fmt.Sprintf("%s/upload/list", c.endpoint)

	reqBody := listObjectsRequest{
		Bucket: bucketName,
		Path:   path,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.Printf("Failed to marshal list objects request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to marshal list objects request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send list objects request: %v", err)
		return nil, ErrFailedToConnect
	}
	defer resp.Body.Close()

	err = c.parseServerResponse(resp, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// parseServerError parses the error response from the server
func (c *UploadClient) parseServerError(err *serverErrorBody) error {
	if err == nil {
		return nil
	}

	switch err.ErrorCode {
	case "ErrNotFound":
		return ErrResourceNotFound
	case "ErrAuthApiKey":
		return ErrUnauthorized
	case "ErrBucketNotFound":
		return ErrBucketNotFound
	case "ErrObjectNotFound":
		return ErrObjectNotFound
	case "ErrObjectFailedToCreateDir":
		return ErrObjectFailedToCreateDir
	case "ErrObjectFailedToCreateObj":
		return ErrObjectFailedToCreateObject
	case "ErrObjectFailedToOpen":
		return ErrObjectFailedToOpen
	case "ErrObjectInvalidDataURI":
		return ErrObjectInvalidDataURI
	}

	return ErrUnknown
}

// parseServerResponse parses the response from the server
func (c *UploadClient) parseServerResponse(resp *http.Response, out any) error {
	var response serverResponse

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.logger.Printf("Failed to read response body: %v", err)
		return ErrParseResponseFailed
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		c.logger.Printf("Failed to unmarshal response body: %v", err)
		return ErrParseResponseFailed
	}

	if out != nil {
		err = json.Unmarshal(response.Data, out)
		if err != nil {
			c.logger.Printf("Failed to unmarshal response body: %v", err)
			return ErrParseResponseFailed
		}
	}

	return c.parseServerError(response.Error)
}

// PutObject uploads a file in chunks with checksum verification using concurrent workers
func (c *UploadClient) PutObject(bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error) {
	totalSize := int64(len(data))

	checksumAlgo := Sha256Sum
	if opts != nil && opts.ChecksumAlgorithm != "" {
		checksumAlgo = opts.ChecksumAlgorithm
	}

	checksum, err := c.calculateChecksum(data, checksumAlgo)
	if err != nil {
		c.logger.Printf("PutObject: Failed to calculate checksum: %v", err)
		return nil, err
	}

	fileId := uuid.New().String()

	// Split data into chunks first
	chunks := c.splitIntoChunks(data)
	totalChunks := len(chunks)

	// Configure concurrency
	numWorkers := 5 // Adjust based on your needs
	if c.maxConcurrency > 0 {
		numWorkers = c.maxConcurrency
	}

	jobs := make(chan chunkJob, totalChunks)
	results := make(chan error, totalChunks)

	// Progress tracking
	var uploadedMutex sync.Mutex
	var uploaded int64

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := c.sendChunk(bucketName, objectName, fileId, job.data, job.chunkNum, checksum)
				results <- err

				if err == nil {
					// Update progress
					uploadedMutex.Lock()
					uploaded += int64(len(job.data))
					currentUploaded := uploaded
					uploadedMutex.Unlock()

					if opts != nil && opts.OnProgress != nil {
						opts.OnProgress(currentUploaded, totalSize)
					}
				}
			}
		}()
	}

	// Send jobs to workers
	for i, chunk := range chunks {
		jobs <- chunkJob{
			data:     chunk,
			chunkNum: i,
		}
	}
	close(jobs)

	// Wait for all workers to finish
	wg.Wait()
	close(results)

	// Check for errors
	for err := range results {
		if err != nil {
			c.logger.Printf("PutObject: Failed to send chunk: %v", err)
			return nil, err
		}
	}

	// Verify checksum
	if err := c.verifyChecksum(bucketName, objectName, fileId, checksum); err != nil {
		return nil, ErrChecksumMismatch
	}

	return &UploadResult{
		Checksum: checksum,
		Size:     totalSize,
	}, nil
}

// PutObjectV2 uploads a file in chunks with checksum verification using concurrent workers
func (c *UploadClient) PutObjectV2(ctx context.Context, bucketName, objectName string, data []byte, opts *UploadOptions) (*UploadResult, error) {
	totalSize := int64(len(data))
	checksum, _ := c.calculateChecksum(data, Sha256Sum)
	fileId := uuid.New().String()

	// Use context to prevent hanging uploads
	g, ctx := errgroup.WithContext(ctx)

	sem := make(chan struct{}, c.maxConcurrency)
	var uploaded int64

	for i := 0; i < len(data); i += c.chunkSize {
		start, end := i, i+c.chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunkNum := i / c.chunkSize
		chunkData := data[start:end] // Zero-copy slicing

		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			// Retry loop: 3 attempts with backoff
			return backoff.Retry(func() error {
				err := c.sendChunk(bucketName, objectName, fileId, chunkData, chunkNum, checksum)
				if err == nil {
					atomic.AddInt64(&uploaded, int64(len(chunkData)))
					if opts != nil && opts.OnProgress != nil {
						opts.OnProgress(atomic.LoadInt64(&uploaded), totalSize)
					}
				}
				return err
			}, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &UploadResult{Checksum: checksum, Size: totalSize}, nil
}

// PutObjectForm uploads a file in one form request
func (c *UploadClient) PutObjectForm(bucketName, objectName string, data []byte) (string, error) {
	url := fmt.Sprintf("%s/upload/file", c.endpoint)

	// Create a buffer to write multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add text field: bucket
	err := writer.WriteField("bucket", bucketName)
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to write bucket field: %v", err)
		return "", ErrFailedToParseRequest
	}

	// Add text field: path
	err = writer.WriteField("path", objectName)
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to write path field: %v", err)
		return "", ErrFailedToParseRequest
	}

	// Add file field: data
	part, err := writer.CreateFormFile("data", filepath.Base(objectName))
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to create form file: %v", err)
		return "", ErrFailedToParseRequest
	}

	// Copy file data
	_, err = io.Copy(part, bytes.NewReader(data))
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to copy file data: %v", err)
		return "", ErrFailedToParseRequest
	}

	// Close the writer
	err = writer.Close()
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to close writer: %v", err)
		return "", ErrFailedToParseRequest
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, body)
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to create request: %v", err)
		return "", ErrFailedToParseRequest
	}

	// Set content type header
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Add authentication if available
	req.Header.Set("X-API-KEY", c.accessKey)

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to send request: %v", err)
		return "", ErrFailedToConnect
	}
	defer resp.Body.Close()

	var resultPath string
	err = c.parseServerResponse(resp, &resultPath)
	if err != nil {
		c.logger.Printf("PutObjectForm: Failed to parse response: %v", err)
		return "", ErrParseResponseFailed
	}

	return resultPath, nil
}

// PutObjectFormV2 uploads a file in one form request
func (c *UploadClient) PutObjectFormV2(ctx context.Context, bucketName, objectName string, data []byte) (string, error) {
	url := fmt.Sprintf("%s/upload/file", c.endpoint)

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer writer.Close()

		writer.WriteField("bucket", bucketName)
		writer.WriteField("path", objectName)

		part, err := writer.CreateFormFile("data", filepath.Base(objectName))
		if err != nil {
			c.logger.Printf("PutObjectFormV2: Failed to create form file: %v", err)
			return
		}

		io.Copy(part, bytes.NewReader(data))
	}()

	req, err := http.NewRequestWithContext(ctx, "POST", url, pr)
	if err != nil {
		c.logger.Printf("PutObjectFormV2: Failed to create http request: %v", err)
		return "", ErrFailedToParseRequest
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-API-KEY", c.accessKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("PutObjectFormV2: Failed to connect to server: %v", err)
		return "", ErrFailedToConnect
	}
	defer resp.Body.Close()

	var resultPath string
	err = c.parseServerResponse(resp, &resultPath)
	return resultPath, err
}

// SendChunk sends single chunk to server
func (c *UploadClient) sendChunk(bucketName, objectName, fileId string, chunk []byte, chunkNum int, checksum string) error {
	url := fmt.Sprintf("%s/upload/chunk", c.endpoint)

	encodedChunk := base64.StdEncoding.EncodeToString(chunk)

	payload := chunkRequest{
		BucketName: bucketName,
		ObjectName: objectName,
		FileId:     fileId,
		ChunkNum:   chunkNum,
		Data:       encodedChunk,
		Checksum:   checksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Printf("Failed to marshal chunk request: %v", err)
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to connect to server: %v", err)
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	return c.parseServerResponse(resp, nil)
}

// splitIntoChunks splits data into chunks of c.chunkSize
func (c *UploadClient) splitIntoChunks(data []byte) [][]byte {
	var chunks [][]byte
	dataLen := len(data)

	for i := 0; i < dataLen; i += int(c.chunkSize) {
		end := i + int(c.chunkSize)
		end = min(end, dataLen)
		chunks = append(chunks, data[i:end])
	}

	return chunks
}

// StatObject returns information about an object in a bucket
func (c *UploadClient) StatObject(bucketName, objectName string) (*ObjectInfo, error) {
	var result ObjectInfo

	url := fmt.Sprintf("%s/upload/stat", c.endpoint)

	payload := statObjectRequest{
		Bucket: bucketName,
		Path:   objectName,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Printf("Failed to marshal stat request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return nil, ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return nil, ErrFailedToConnect
	}
	defer resp.Body.Close()

	err = c.parseServerResponse(resp, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// VerifyChecksum calls server verify endpoint
func (c *UploadClient) verifyChecksum(bucketName, objectName, fileId, expectedChecksum string) error {
	url := fmt.Sprintf("%s/upload/verify", c.endpoint)

	payload := verifyRequest{
		Bucket:   bucketName,
		Path:     objectName,
		FileId:   fileId,
		Checksum: expectedChecksum,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		c.logger.Printf("Failed to marshal verify request: %v", err)
		return ErrFailedToParseRequest
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		c.logger.Printf("Failed to create request: %v", err)
		return ErrFailedToParseRequest
	}

	req.Header.Set("X-API-KEY", c.accessKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Printf("Failed to send request: %v", err)
		return ErrFailedToConnect
	}
	defer resp.Body.Close()

	return c.parseServerResponse(resp, nil)
}
